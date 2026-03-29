# This file makes the ingestion dashboard available as a page
# in the multi-page Streamlit app
import importlib, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# Re-export the ingestion dashboard content
exec(open(os.path.join(os.path.dirname(__file__), "..", "ingestion_dashboard.py")).read())
