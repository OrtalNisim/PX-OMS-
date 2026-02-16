#!/usr/bin/env python3
"""Full S3 upload/download test: text file + CSV file."""
import os
from pathlib import Path
from dotenv import load_dotenv
import boto3

load_dotenv(Path(__file__).parent / ".env")

client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
bucket = os.environ.get("S3_BUCKET")
prefix = os.environ.get("S3_PREFIX", "margin-optimizer/")

# --- TEST 1: Upload a small text file ---
print("=== TEST 1: Upload text file ===")
test_key = prefix + "test_upload.txt"
client.put_object(Bucket=bucket, Key=test_key, Body=b"Hello from test upload!", ContentType="text/plain")
print(f"  Uploaded to s3://{bucket}/{test_key}")

# --- TEST 2: Read it back ---
print("\n=== TEST 2: Download and verify ===")
resp = client.get_object(Bucket=bucket, Key=test_key)
content = resp["Body"].read().decode("utf-8")
print(f"  Downloaded content: {content}")
assert content == "Hello from test upload!", "Content mismatch!"
print("  Content matches - PASSED")

# --- TEST 3: Upload the actual CSV file from this project ---
print("\n=== TEST 3: Upload a real CSV file ===")
csv_path = Path(__file__).parent / "margin_recommendations_next_round.csv"
if csv_path.exists():
    csv_data = csv_path.read_bytes()
    csv_key = prefix + "test_csv_upload.csv"
    client.put_object(Bucket=bucket, Key=csv_key, Body=csv_data, ContentType="text/csv")
    print(f"  Uploaded {csv_path.name} ({len(csv_data)} bytes) to s3://{bucket}/{csv_key}")

    resp = client.get_object(Bucket=bucket, Key=csv_key)
    downloaded = resp["Body"].read()
    assert downloaded == csv_data, "CSV content mismatch!"
    print("  CSV round-trip - PASSED")
else:
    csv_key = None
    print(f"  {csv_path.name} not found, skipping CSV test")

# --- TEST 4: List objects to confirm they exist ---
print("\n=== TEST 4: List uploaded objects in bucket ===")
resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
for obj in resp.get("Contents", []):
    print(f"  {obj['Key']}  ({obj['Size']} bytes, {obj['LastModified']})")

# --- Cleanup test files ---
print("\n=== Cleanup: deleting test files ===")
client.delete_object(Bucket=bucket, Key=test_key)
print(f"  Deleted {test_key}")
if csv_key:
    client.delete_object(Bucket=bucket, Key=csv_key)
    print(f"  Deleted {csv_key}")

print("\n ALL TESTS PASSED")
