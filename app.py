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
@app.post("/ingest/stock-yiqi")
def ingest_stock_yiqi():
    from datetime import datetime
    import pandas as pd

    received_at = datetime.utcnow().isoformat() + "Z"

    # 1) JSON
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        return jsonify({
            "ok": True,
            "type": "json",
            "received_at": received_at,
            "keys": list(payload.keys())
        }), 200

    # 2) Archivo
    if "file" not in request.files:
        return jsonify({"ok": False, "error": "Send JSON or multipart file with key 'file'"}), 400

    f = request.files["file"]
    filename = (f.filename or "").lower()

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(f)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(f)
        else:
            return jsonify({"ok": False, "error": "Only .csv or .xlsx files supported"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to read file: {str(e)}"}), 400

    cols = {c.lower().strip(): c for c in df.columns}

    sku_col = None
    stock_col = None

    for k in ["sku", "seller sku", "codigo", "código", "cod", "sku_seller", "seller_sku"]:
        if k in cols:
            sku_col = cols[k]
            break

    for k in ["stock", "qty", "quantity", "available", "disponible", "stock disponible", "stock_disponible"]:
        if k in cols:
            stock_col = cols[k]
            break

    if not sku_col or not stock_col:
        return jsonify({
            "ok": False,
            "error": "Could not detect SKU/Stock columns",
            "columns": list(df.columns)
        }), 400

    out = df[[sku_col, stock_col]].copy()
    out.columns = ["sku", "stock"]
    out["sku"] = out["sku"].astype(str).str.strip()
    out["stock"] = pd.to_numeric(out["stock"], errors="coerce").fillna(0).astype(int)

    total_rows = int(len(out))
    total_skus = int(out["sku"].nunique())
    low_stock = out[out["stock"] <= 2].sort_values("stock").head(20)

    return jsonify({
        "ok": True,
        "type": "file",
        "received_at": received_at,
        "filename": f.filename,
        "detected_columns": {"sku": sku_col, "stock": stock_col},
        "total_rows": total_rows,
        "unique_skus": total_skus,
        "low_stock_sample": low_stock.to_dict(orient="records")
    }), 200

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

