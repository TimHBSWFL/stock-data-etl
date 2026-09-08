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

The statements API also auto-starts a stopped warehouse, but when that start
fails it reports the same opaque 400 -- naming neither the phase nor the reason.
So the start is done explicitly here first, which turns a cold-start failure
into the warehouse's own state and health message.
"""
import os
import time

import requests

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
WAREHOUSE_REJECT = "could not be processed by the warehouse"
# A cold serverless start is usually under a minute; classic warehouses take
# longer. Kept inside the workflow's timeout-minutes so the job fails with this
# module's message rather than being killed mid-start.
START_TIMEOUT = 240


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


def _health_text(warehouse):
    """Whatever the warehouse says about why it is unhealthy."""
    health = warehouse.get("health") or {}
    parts = [health.get("status"), health.get("summary"), health.get("message")]
    code = (health.get("failure_reason") or {}).get("code")
    if code:
        parts.append(f"failure_reason={code}")
    return " | ".join(p for p in parts if p) or "no health detail reported"


def _get_warehouse(host, headers, warehouse_id):
    """Warehouse description, or None if it cannot be inspected with this token."""
    try:
        response = requests.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}",
                                headers=headers, timeout=60)
    except requests.exceptions.RequestException as exc:
        print(f"Could not inspect warehouse {warehouse_id} ({exc}); "
              f"submitting anyway and letting the API auto-start it.")
        return None

    if response.status_code == 200:
        return response.json()

    # Reading the warehouse needs CAN_USE. Preflight is a diagnostic, so a token
    # that cannot do it should not turn into a new way for the job to fail.
    print(f"Could not inspect warehouse {warehouse_id} "
          f"(HTTP {response.status_code}: {response.text}); submitting anyway "
          f"and letting the API auto-start it.")
    return None


def _start_warehouse(host, headers, warehouse_id):
    """Request a start. False means this token may not start it; fall back."""
    response = requests.post(f"{host}/api/2.0/sql/warehouses/{warehouse_id}/start",
                             headers=headers, timeout=60)
    if response.status_code == 200:
        return True
    if response.status_code in (401, 403):
        print(f"Not permitted to start warehouse {warehouse_id} "
              f"(HTTP {response.status_code}); submitting anyway.")
        return False
    raise DatabricksSQLError(
        f"Could not start warehouse {warehouse_id} "
        f"(HTTP {response.status_code}): {response.text}")


def ensure_warehouse_running(start_timeout=START_TIMEOUT, poll_seconds=5):
    """Start the warehouse and wait for it, so a cold-start failure is legible."""
    host, headers, warehouse_id = _config()

    warehouse = _get_warehouse(host, headers, warehouse_id)
    if warehouse is None:
        return

    state = warehouse.get("state")
    if state == "RUNNING":
        return
    if state == "DELETED":
        raise DatabricksSQLError(
            f"Warehouse {warehouse_id} is DELETED. Check the warehouse ID in "
            f"DATABRICKS_WAREHOUSE_ID against the workspace.")

    print(f"Warehouse {warehouse_id} is {state}; starting it before submitting.")
    started_at = time.monotonic()
    deadline = started_at + start_timeout
    start_requested = False

    while True:
        if state == "RUNNING":
            print(f"Warehouse {warehouse_id} is RUNNING after "
                  f"{time.monotonic() - started_at:.0f}s.")
            return
        if state == "DELETED":
            raise DatabricksSQLError(
                f"Warehouse {warehouse_id} is DELETED. Check the warehouse ID in "
                f"DATABRICKS_WAREHOUSE_ID against the workspace.")
        if state == "STOPPED":
            if start_requested:
                raise DatabricksSQLError(
                    f"Warehouse {warehouse_id} returned to STOPPED after a start "
                    f"request -- it could not start: {_health_text(warehouse)}")
            if not _start_warehouse(host, headers, warehouse_id):
                return
            start_requested = True

        if time.monotonic() > deadline:
            raise DatabricksSQLError(
                f"Warehouse {warehouse_id} still {state} after {start_timeout}s: "
                f"{_health_text(warehouse)}")

        time.sleep(poll_seconds)
        warehouse = _get_warehouse(host, headers, warehouse_id)
        if warehouse is None:
            return
        state = warehouse.get("state")


def execute_statement(sql, wait_timeout="30s", max_attempts=5, poll_seconds=5,
                      poll_timeout=600):
    """Run a statement to completion. Returns the final response payload.

    Raises DatabricksSQLError if the request is rejected, the statement fails,
    or it is still running after poll_timeout seconds.
    """
    ensure_warehouse_running()

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
