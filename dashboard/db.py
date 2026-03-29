"""Shared Snowflake connection helper for dashboards.

Supports both environment variables (local) and Streamlit secrets (cloud).
"""
from __future__ import annotations

import os
from typing import Optional

import snowflake.connector


def _get_config(key: str, default: str = "") -> str:
    """Get config from Streamlit secrets first, then env vars."""
    try:
        import streamlit as st
        if hasattr(st, "secrets") and key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.environ.get(key, default)


def get_connection():
    """Return a Snowflake connection."""
    return snowflake.connector.connect(
        account=_get_config("SNOWFLAKE_ACCOUNT"),
        user=_get_config("SNOWFLAKE_USER"),
        password=_get_config("SNOWFLAKE_PASSWORD"),
        warehouse=_get_config("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=_get_config("SNOWFLAKE_DATABASE", "RISK_MANAGEMENT"),
        schema=_get_config("SNOWFLAKE_SCHEMA", "CORE"),
    )


def run_query(query: str, params: Optional[tuple] = None):
    """Execute *query* and return all rows as a list of dicts."""
    conn = get_connection()
    try:
        cur = conn.cursor(snowflake.connector.DictCursor)
        cur.execute(query, params)
        return cur.fetchall()
    finally:
        conn.close()


def call_procedure(sql: str, params: Optional[tuple] = None) -> str:
    """Execute a CALL statement and return the scalar result."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else ""
    finally:
        conn.close()
