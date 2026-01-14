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
    # Acá luego enchufamos tu proveedor real (Twilio/CallMeBot/etc).
    # Por ahora devuelve OK para confirmar que existe.
    return jsonify({"ok": True, "msg": "notify/test endpoint OK"}), 200

@app.post("/ingest/stock-yiqi")
def ingest_stock_yiqi():
    """
    Acepta:
    - JSON (Content-Type: application/json)
    - o archivo (multipart/form-data) con key 'file'
    Devuelve un resumen de lo recibido.
    """
    received_at = datetime.utcnow().isoformat() + "Z"

    # Caso JSON
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        return jsonify({
            "ok": True,
            "type": "json",
            "received_at": received_at,
            "keys": list(payload.keys()),
            "sample": payload if len(json.dumps(payload)) < 1500 else "payload_too_large_for_sample"
        }), 200

    # Caso archivo (Excel/CSV)
    if "file" in request.files:
        f = request.files["file"]
        filename = f.filename or "uploaded_file"
        # No procesamos todavía; solo confirmamos recepción
        return jsonify({
            "ok": True,
            "type": "file",
            "received_at": received_at,
            "filename": filename,
            "content_type": f.content_type
        }), 200

    # Nada válido
    return jsonify({
        "ok": False,
        "error": "Send JSON or multipart file with key 'file'."
    }), 400


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))
