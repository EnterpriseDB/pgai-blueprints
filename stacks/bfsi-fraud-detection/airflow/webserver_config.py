"""
Airflow webserver auth config — DEMO ONLY.

For demonstration purposes only.

Grants anonymous users the Admin role so the BFSI demo doesn't have to stop
at a login wall. Safe because :8888 is bound to localhost on the laptop.
Never deploy this config to a publicly-reachable Airflow.
"""
from airflow.www.fab_security.manager import AUTH_DB

AUTH_TYPE = AUTH_DB
AUTH_ROLE_PUBLIC = "Admin"
WTF_CSRF_ENABLED = False
