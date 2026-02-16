#!/usr/bin/env python3
"""
Simple connectivity test for S3 using s3_storage.py.
Run from the project folder: python test_s3.py
"""
from s3_storage import S3_BUCKET, _client

def main():
    if not S3_BUCKET:
        print("S3_BUCKET is not set. Check your .env or environment variables.")
        return

    try:
        client = _client()
        client.head_bucket(Bucket=S3_BUCKET)
        print("Connected to bucket:", S3_BUCKET)
    except Exception as e:
        print("Failed to connect to bucket:", S3_BUCKET)
        print("Error:", e)

if __name__ == "__main__":
    main()

