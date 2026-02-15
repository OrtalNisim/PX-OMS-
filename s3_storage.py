"""
S3 storage for optimizer state and run logs.
Set S3_BUCKET (and optionally S3_PREFIX) to enable. Uses boto3.
"""
import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "margin-optimizer/")


def _client():
    """Lazy import to avoid boto3 dependency when S3 is not used."""
    import boto3
    return boto3.client("s3")


def _enabled() -> bool:
    return bool(S3_BUCKET.strip())


def save_state(state: Dict[str, Any]) -> bool:
    """
    Save optimizer state to S3 (in addition to local file).
    State is already saved locally by MarginOptimizer.
    """
    if not _enabled():
        return True

    try:
        client = _client()
        key = f"{S3_PREFIX.rstrip('/')}/optimizer_state.json"
        body = json.dumps(state, indent=2).encode("utf-8")
        client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        return True
    except Exception as e:
        print(f"Warning: failed to save state to S3: {e}", flush=True)
        return False


def load_state() -> Optional[Dict[str, Any]]:
    """Load optimizer state from S3. Returns None if not found or S3 disabled."""
    if not _enabled():
        return None

    try:
        client = _client()
        key = f"{S3_PREFIX.rstrip('/')}/optimizer_state.json"
        resp = client.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Warning: failed to load state from S3: {e}", flush=True)
        return None


def save_run_log(
    current_margin: float,
    next_margin: float,
    metrics: Dict[str, Any],
    success: bool,
) -> bool:
    """
    Append a run log to S3. Uses timestamped key for audit trail.
    """
    if not _enabled():
        return True

    try:
        client = _client()
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
        key = f"{S3_PREFIX.rstrip('/')}/runs/{ts}.json"
        log = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "current_margin": current_margin,
            "next_margin": next_margin,
            "metrics": metrics,
            "success": success,
        }
        body = json.dumps(log, indent=2).encode("utf-8")
        client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        return True
    except Exception as e:
        print(f"Warning: failed to save run log to S3: {e}", flush=True)
        return False
