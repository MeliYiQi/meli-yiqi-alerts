import os
import requests

BASE_URL = os.environ["BASE_URL"].rstrip("/")
KEY = os.environ["DIGEST_SECRET"]

r = requests.post(f"{BASE_URL}/digest/stock", params={"key": KEY}, timeout=60)
print(r.status_code, r.text)
r.raise_for_status()
