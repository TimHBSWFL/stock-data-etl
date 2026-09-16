"""Touch the Databricks workspace so it is not deactivated for inactivity.

Free Edition marks a workspace INACTIVE after roughly two days with no activity,
after which the resource-gatekeeper denies all compute and only a human opening
the workspace UI can clear it. The weekday-only ETL and dbt schedules leave a
~72 hour gap over every weekend, which is why failures always started on a
Monday. This runs every day to keep that gap short.
"""
import os
import sys

sys.path.append(os.path.join(os.getcwd(), "scripts"))
from databricks_sql import execute_statement, stop_warehouse, warehouse_state

# Only stop what we started -- another job may be mid-run on a warehouse that
# was already up, and a scheduled run can drift into that window.
was_running = warehouse_state() == "RUNNING"

execute_statement("SELECT 1")
print("Workspace touched.")

if was_running:
    print("Warehouse was already running; leaving it up.")
else:
    # Don't leave a warehouse idling against the Free Edition usage quota.
    stop_warehouse()
