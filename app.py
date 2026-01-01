import os
from flask import Flask, jsonify
from twilio.rest import Client

app = Flask(__name__)

def must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

@app.get("/")
def home():
    return "OK", 200

@app.post("/notify/test")
def notify_test():
    account_sid = must_env("TWILIO_ACCOUNT_SID")
    auth_token = must_env("TWILIO_AUTH_TOKEN")
    from_ = must_env("TWILIO_WHATSAPP_FROM")   # whatsapp:+14155238886
    to = must_env("WHATSAPP_TO")               # whatsapp:+54...
    client = Client(account_sid, auth_token)

    msg = client.messages.create(
        from_=from_,
        to=to,
        body="✅ Render + Twilio OK. Mensaje de prueba."
    )
    return jsonify({"ok": True, "sid": msg.sid})
