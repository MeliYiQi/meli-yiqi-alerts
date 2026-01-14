from flask import Flask, request, jsonify
import os
import json
from datetime import datetime

app = Flask(__name__)

@app.get("/")
def home():
    return "OK", 200

@app.get("/notify/test")
def notify_test():
    return jsonify({
        "ok": True,
        "msg": "notify/test endpoint OK"
    }), 200

@app.post("/ingest/stock-yiqi")
def ingest_stock_yiqi():
    received_at = datetime.utcnow().isoformat() + "Z"

    if request.is_json:
        payload = request.get_json(silent=True) or {}
        return jsonify({
            "ok": True,
            "type": "json",
            "received_at": received_at,
            "keys": list(payload.keys())
        }), 200

    if "file" in request.files:
        f = request.files["file"]
        return jsonify({
            "ok": True,
            "type": "file",
            "filename": f.filename,
            "content_type": f.content_type,
            "received_at": received_at
        }), 200

    return jsonify({
        "ok": False,
        "error": "Send JSON or multipart file with key 'file'"
    }), 400
