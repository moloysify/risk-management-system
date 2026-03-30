"""Risk Management System — Main entry point for Streamlit Cloud.

This file redirects to the home dashboard and sets up the multi-page structure.
Pages are in the pages/ directory at the repo root.
"""
from __future__ import annotations
import sys, os

# Add dashboard dir to path for db.py imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "dashboard"))
# Add project root for simulator imports
sys.path.insert(0, os.path.dirname(__file__))

# Run the home dashboard content
exec(open(os.path.join(os.path.dirname(__file__), "dashboard", "home_dashboard.py")).read())
