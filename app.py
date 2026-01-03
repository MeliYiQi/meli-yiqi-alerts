import os
import io
from datetime import date
import pandas as pd
import psycopg2
from flask import Flask, jsonify, request
from twilio.rest import Client

app = Flask(__name__)

def env(name: str, default=None):
    return os.environ.get(name, default)

def must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def db_conn():
    # No romper el server si no está configurado
    database_url = env("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Missing env var: DATABASE_URL")
    return psycopg2.connect(database_url)

def init_db():
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        create table if not exists stock_latest (
          sku text primary key,
          stock numeric not null,
          updated_at timestamp not null default now()
        );
        """)
        cur.execute("""
        create table if not exists sales_latest (
          sku text primary key,
          sales_30d numeric not null,
          updated_at timestamp not null default now()
        );
        """)
        cur.execute("""
        create table if not exists inbound_plan (
          sku text primary key,
          next_inbound_date date,
          qty numeric,
          note text,
          updated_at timestamp not null default now()
        );
        """)

def send_whatsapp(body: str) -> str:
    client = Client(must_env("TWILIO_ACCOUNT_SID"), must_env("TWILIO_AUTH_TOKEN"))
    msg = client.messages.create(
        from_=must_env("TWILIO_WHATSAPP_FROM"),
        to=must_env("WHATSAPP_TO"),
        body=body
    )
    return msg.sid

@app.get("/")
def home():
    return "OK", 200

# ---------- TEST ----------
@app.post("/notify/test")
def notify_test():
    try:
        sid = send_whatsapp("✅ OK: Render + Twilio funcionando.")
        return jsonify({"ok": True, "sid": sid})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------- INGEST: STOCK (Stock_Disponible-YiQi.xlsx) ----------
@app.post("/ingest/stock-yiqi")
def ingest_stock_yiqi():
    try:
        init_db()
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB error: {e}"}), 500

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Falta archivo en field 'file'"}), 400

    raw = pd.read_excel(io.BytesIO(f.read()))
    sku_col = None
    for c in raw.columns:
        if str(c).strip().lower() in ["artículo - sku", "articulo - sku", "sku"]:
            sku_col = c
            break
    if sku_col is None:
        return jsonify({"ok": False, "error": "No encuentro columna SKU (Artículo - SKU)"}), 400

    # stock total = suma de todas columnas numéricas (depósitos)
    num_cols = [c for c in raw.columns if c != sku_col and pd.api.types.is_numeric_dtype(raw[c])]
    if not num_cols:
        return jsonify({"ok": False, "error": "No encuentro columnas numéricas de stock"}), 400

    df = raw[[sku_col] + num_cols].copy()
    df[sku_col] = df[sku_col].astype(str).str.strip()
    df["stock_total"] = df[num_cols].fillna(0).sum(axis=1)

    rows = df[[sku_col, "stock_total"]].values.tolist()

    with db_conn() as conn, conn.cursor() as cur:
        for sku, stock in rows:
            cur.execute("""
                insert into stock_latest(sku, stock, updated_at)
                values (%s, %s, now())
                on conflict (sku) do update set
                  stock=excluded.stock,
                  updated_at=now();
            """, (sku, float(stock)))

    return jsonify({"ok": True, "rows": len(rows), "num_cols_used": [str(c) for c in num_cols]})

# ---------- INGEST: VENTAS (hoja Recompra, ventas 30d en col F) ----------
@app.post("/ingest/sales-recompra")
def ingest_sales_recompra():
    try:
        init_db()
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB error: {e}"}), 500

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Falta archivo en field 'file'"}), 400

    xls = pd.ExcelFile(io.BytesIO(f.read()))
    sheet = next((s for s in xls.sheet_names if s.strip().lower() == "recompra"), None)
    if not sheet:
        return jsonify({"ok": False, "error": "No encuentro hoja 'Recompra'"}), 400

    df = pd.read_excel(xls, sheet_name=sheet)
    if df.shape[1] < 6:
        return jsonify({"ok": False, "error": "Recompra debe tener al menos 6 columnas (ventas 30d en F)"}), 400

    sku_col = df.columns[0]     # A
    sales30_col = df.columns[5] # F

    out = df[[sku_col, sales30_col]].copy()
    out.columns = ["sku", "sales_30d"]
    out["sku"] = out["sku"].astype(str).str.strip()
    out["sales_30d"] = pd.to_numeric(out["sales_30d"], errors="coerce").fillna(0)

    rows = out.values.tolist()

    with db_conn() as conn, conn.cursor() as cur:
        for sku, s30 in rows:
            cur.execute("""
                insert into sales_latest(sku, sales_30d, updated_at)
                values (%s, %s, now())
                on conflict (sku) do update set
                  sales_30d=excluded.sales_30d,
                  updated_at=now();
            """, (sku, float(s30)))

    return jsonify({"ok": True, "rows": len(rows)})

# ---------- INGEST: PROX INGRESOS ----------
@app.post("/ingest/prox-ingresos")
def ingest_prox_ingresos():
    try:
        init_db()
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB error: {e}"}), 500

    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Falta archivo en field 'file'"}), 400

    df = pd.read_excel(io.BytesIO(f.read()))
    cols = {str(c).strip(): c for c in df.columns}
    if "SKU" not in cols or "next_inbound_date" not in cols:
        return jsonify({"ok": False, "error": "Necesito columnas SKU y next_inbound_date"}), 400

    sku = df[cols["SKU"]].astype(str).str.strip()
    dts = pd.to_datetime(df[cols["next_inbound_date"]], errors="coerce").dt.date

    qty = pd.to_numeric(df[cols["qty"]], errors="coerce") if "qty" in cols else None
    note = df[cols["nota"]].astype(str) if "nota" in cols else None

    with db_conn() as conn, conn.cursor() as cur:
        for i in range(len(df)):
            sk = sku.iloc[i]
            if not sk:
                continue
            dt = dts.iloc[i]
            q = None if qty is None or pd.isna(qty.iloc[i]) else float(qty.iloc[i])
            n = None if note is None else (None if pd.isna(note.iloc[i]) else str(note.iloc[i]))
            cur.execute("""
                insert into inbound_plan(sku, next_inbound_date, qty, note, updated_at)
                values (%s, %s, %s, %s, now())
                on conflict (sku) do update set
                  next_inbound_date=excluded.next_inbound_date,
                  qty=excluded.qty,
                  note=excluded.note,
                  updated_at=now();
            """, (sk, dt, q, n))

    return jsonify({"ok": True, "rows": int(len(df))})

# ---------- DIGEST STOCK ----------
@app.post("/digest/stock")
def digest_stock():
    secret = env("DIGEST_SECRET")
    if not secret:
        return jsonify({"ok": False, "error": "Missing env var: DIGEST_SECRET"}), 500

    key = request.args.get("key", "")
    if key != secret:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    try:
        init_db()
    except Exception as e:
        return jsonify({"ok": False, "error": f"DB error: {e}"}), 500

    target_days = 30.0
    today = date.today()

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          select s.sku, s.stock,
                 coalesce(v.sales_30d,0) as sales_30d,
                 i.next_inbound_date
          from stock_latest s
          left join sales_latest v on v.sku = s.sku
          left join inbound_plan i on i.sku = s.sku
        """)
        rows = cur.fetchall()

    alerts = []
    for sku, stock, sales_30d, next_inbound in rows:
        sales_per_day = float(sales_30d) / 30.0
        if sales_per_day <= 0:
            continue
        coverage = float(stock) / sales_per_day

        if next_inbound:
            days_until_inbound = (next_inbound - today).days
            if days_until_inbound >= 0 and days_until_inbound <= coverage:
                continue

        if coverage < target_days:
            alerts.append((sku, coverage, float(stock), float(sales_30d), next_inbound))

    alerts.sort(key=lambda x: x[1])

    if not alerts:
        sid = send_whatsapp("✅ Stock OK: ningún SKU con cobertura < 30 días (ventas_30d/30).")
        return jsonify({"ok": True, "count": 0, "sid": sid})

    lines = ["📦 ALERTA STOCK (<30 días)"]
    for sku, cov, st, s30, inbound in alerts[:30]:
        inbound_txt = f" | ingresa {inbound.isoformat()}" if inbound else ""
        lines.append(f"- {sku}: {cov:.1f} días (stock {st:.0f}, v30 {s30:.0f}){inbound_txt}")

    if len(alerts) > 30:
        lines.append(f"... +{len(alerts)-30} más")

    sid = send_whatsapp("\n".join(lines))
    return jsonify({"ok": True, "count": len(alerts), "sid": sid})
