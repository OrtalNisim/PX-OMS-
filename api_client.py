"""
API client for fetching hourly metrics and updating margin.
Uses mock data by default; replace fetch_hourly_metrics and update_margin
when the real API is available.
"""
import os
from typing import Any, Dict

# Placeholder: set when you have the real API
METRICS_API_URL = os.environ.get("METRICS_API_URL", "")  # e.g. https://api.example.com/metrics
UPDATE_MARGIN_API_URL = os.environ.get("UPDATE_MARGIN_API_URL", "")  # e.g. https://api.example.com/margin
API_KEY = os.environ.get("API_KEY", "")


def fetch_hourly_metrics() -> Dict[str, Any]:
    """
    Fetch hourly metrics for the current margin arm.
    Replace this with a real API call when available.

    Real API should return something like:
    {
        "impressions": 50000,
        "revenue": 23.5,
        "cost": 15.2,
        "bid_rate": 1.4,
        "responses": 25000,
        "margin": 35
    }
    """
    # Mock: returns fake data for testing
    return {
        "impressions": 55_000,
        "revenue": 25.0,
        "cost": 16.0,
        "bid_rate": 1.5,
        "responses": 28_000,
        "margin": 35,
    }


def update_margin(margin: float) -> bool:
    """
    Call the platform API to update the margin.
    margin is in percent (0-100).
    Replace with real HTTP call when you have the endpoint.

    Returns True if successful.
    """
    if not UPDATE_MARGIN_API_URL:
        print(f"[MOCK] Would update margin to {margin}% (no UPDATE_MARGIN_API_URL set)")
        return True

    # Placeholder for real implementation:
    # import requests
    # resp = requests.post(
    #     UPDATE_MARGIN_API_URL,
    #     json={"margin": margin},
    #     headers={"Authorization": f"Bearer {API_KEY}"},
    # )
    # return resp.status_code == 200
    print(f"[MOCK] Would POST to {UPDATE_MARGIN_API_URL} with margin={margin}")
    return True
