"""Shared client for the Databricks SQL Statement Execution API.

The ETL scripts all POST a MERGE to /api/2.0/sql/statements. Two failure modes
this handles that a bare requests.post does not:

1. The API returns HTTP 200 even when the statement itself fails -- the real
   outcome lives in status.state. Checking only the HTTP code lets a failed
   MERGE look like a successful run.
2. The routing layer in front of the warehouse occasionally rejects a request
   with 400 BAD_REQUEST "The request could not be processed by the warehouse."
   before the statement ever reaches the warehouse (it never shows up in Query
   History). That is transient, so it is worth retrying.
"""
import os
import time

import requests

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
WAREHOUSE_REJECT = "could not be processed by the warehouse"


class DatabricksSQLError(RuntimeError):
    """A statement was rejected by the API or failed on the warehouse."""


def _config():
    # Read lazily so importing this module does not require the env vars.
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    return host, {"Authorization": f"Bearer {token}"}, warehouse_id


def _retryable(response):
    """True for transient failures worth another attempt."""
    if response.status_code in RETRYABLE_STATUS:
        return True
    # The pre-warehouse rejection: transient, and distinct from a bad statement,
    # which comes back as 200 with status.state == FAILED.
    return response.status_code == 400 and WAREHOUSE_REJECT in response.text


def execute_statement(sql, wait_timeout="30s", max_attempts=5, poll_seconds=5,
                      poll_timeout=600):
    """Run a statement to completion. Returns the final response payload.

    Raises DatabricksSQLError if the request is rejected, the statement fails,
    or it is still running after poll_timeout seconds.
    """
    host, headers, warehouse_id = _config()
    url = f"{host}/api/2.0/sql/statements"
    payload = {
        "statement": sql,
        "warehouse_id": warehouse_id,
        "wait_timeout": wait_timeout,
    }

    response = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=120)
        except requests.exceptions.RequestException as exc:
            if attempt == max_attempts:
                raise DatabricksSQLError(f"Request failed after {attempt} attempts: {exc}")
            backoff = 2 ** attempt
            print(f"Request error ({exc}); retrying in {backoff}s "
                  f"[attempt {attempt}/{max_attempts}]")
            time.sleep(backoff)
            continue

        if response.status_code == 200:
            break
        if not _retryable(response) or attempt == max_attempts:
            raise DatabricksSQLError(
                f"HTTP {response.status_code} from statements API: {response.text}")

        backoff = 2 ** attempt
        print(f"HTTP {response.status_code} from statements API; retrying in "
              f"{backoff}s [attempt {attempt}/{max_attempts}]: {response.text}")
        time.sleep(backoff)

    return _await_result(response.json(), host, headers, poll_seconds, poll_timeout)


def _await_result(body, host, headers, poll_seconds, poll_timeout):
    """Poll until the statement reaches a terminal state, then check it."""
    deadline = time.monotonic() + poll_timeout

    while _state(body) in ("PENDING", "RUNNING"):
        if time.monotonic() > deadline:
            raise DatabricksSQLError(
                f"Statement {body.get('statement_id')} still {_state(body)} after "
                f"{poll_timeout}s")
        time.sleep(poll_seconds)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{body['statement_id']}",
            headers=headers, timeout=120)
        if poll.status_code != 200:
            raise DatabricksSQLError(
                f"HTTP {poll.status_code} polling statement: {poll.text}")
        body = poll.json()

    state = _state(body)
    if state != "SUCCEEDED":
        error = body.get("status", {}).get("error", {})
        raise DatabricksSQLError(
            f"Statement {state}: {error.get('error_code', '')} "
            f"{error.get('message', body)}".strip())
    return body


def _state(body):
    return body.get("status", {}).get("state")


def execute_merge(rows, build_sql, batch_size=200, label="rows"):
    """Run build_sql(batch) over rows in batches of batch_size.

    Batching keeps the statement text well clear of the size at which the API
    starts rejecting requests, and keeps a single bad batch from being ambiguous.
    """
    if not rows:
        print(f"No {label} to load.")
        return 0

    loaded = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        execute_statement(build_sql(batch))
        loaded += len(batch)
        if len(rows) > batch_size:
            print(f"Loaded {loaded}/{len(rows)} {label}")
    return loaded


def fetch_rows(body):
    """Return every row of an inline result, following chunk links.

    A large result is split across chunks; reading only result.data_array
    silently truncates it once the table outgrows a single chunk.
    """
    host, headers, _ = _config()
    result = body.get("result", {})
    rows = list(result.get("data_array") or [])
    link = result.get("next_chunk_internal_link")

    while link:
        response = requests.get(f"{host}{link}", headers=headers, timeout=120)
        if response.status_code != 200:
            raise DatabricksSQLError(
                f"HTTP {response.status_code} fetching result chunk: {response.text}")
        chunk = response.json()
        rows.extend(chunk.get("data_array") or [])
        link = chunk.get("next_chunk_internal_link")

    return rows
