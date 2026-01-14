from flask import Flask, request, jsonify
import os
import pandas as pd
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

app = Flask(__name__)


# -----------------------
# DB helpers
# -----------------------
def get_db_conn():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url)


def ensure_tables():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_latest (
                sku TEXT PRIMARY KEY,
                stock_real INTEGER NOT NULL,
                stock_alerta INTEGER NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_snapshot (
                id BIGSERIAL PRIMARY KEY,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                source_filename TEXT,
                sku TEXT NOT NULL,
                stock_real INTEGER NOT NULL,
                stock_alerta INTEGER NOT NULL
            );
            """)
        conn.commit()


# -----------------------
# Routes
# -----------------------
@app.get("/")
def home():
    return "OK", 200


@app.get("/notify/test")
def notify_test():
    return jsonify({"ok": True, "msg": "notify/test endpoint OK"}), 200
@app.get("/stock/low")
def stock_low():
    max_stock = request.args.get("max", default="2")
    limit = request.args.get("limit", default="200")

    try:
        max_stock = int(max_stock)
        limit = int(limit)
    except:
        return jsonify({"ok": False, "error": "max and limit must be integers"}), 400

    try:
        ensure_tables()
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sku, stock_real, stock_alerta, updated_at
                    FROM stock_latest
                    WHERE stock_alerta <= %s
                    ORDER BY stock_alerta ASC, stock_real ASC, sku ASC
                    LIMIT %s
                """, (max_stock, limit))
                rows = cur.fetchall()

        return jsonify({
            "ok": True,
            "max": max_stock,
            "count": len(rows),
            "items": [
                {"sku": r[0], "stock": r[1], "stock_alerta": r[2], "updated_at": r[3].isoformat()}
                for r in rows
            ]
        }), 200

    except Exception as e:
        return jsonify({"ok": False, "error": f"DB query failed: {str(e)}"}), 500


@app.get("/stock/sku/<path:sku>")
def stock_by_sku(sku):
    try:
        ensure_tables()
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT sku, stock_real, stock_alerta, updated_at
                    FROM stock_latest
                    WHERE sku = %s
                """, (sku,))
                row = cur.fetchone()

        if not row:
            return jsonify({"ok": False, "error": "SKU not found"}), 404

        return jsonify({
            "ok": True,
            "sku": row[0],
            "stock": row[1],
            "stock_alerta": row[2],
            "updated_at": row[3].isoformat()
        }), 200

    except Exception as e:
        return jsonify({"ok": False, "error": f"DB query failed: {str(e)}"}), 500


@app.get("/stock/last-ingest")
def stock_last_ingest():
    try:
        ensure_tables()
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(ingested_at) FROM stock_snapshot;")
                row = cur.fetchone()

        last_ingest = row[0].isoformat() if row and row[0] else None
        return jsonify({"ok": True, "last_ingest_at": last_ingest}), 200

    except Exception as e:
        return jsonify({"ok": False, "error": f"DB query failed: {str(e)}"}), 500


@app.post("/ingest/stock-yiqi")
def ingest_stock_yiqi():
    received_at = datetime.utcnow().isoformat() + "Z"

    # 1) JSON (por si más adelante YiQi manda webhook)
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
        return jsonify({"ok": False, "error": "Send multipart file with key 'file'"}), 400

    f = request.files["file"]
    filename = (f.filename or "").lower()

    # Leer CSV / Excel
    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(f)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(f)
        else:
            return jsonify({"ok": False, "error": "Only .csv or .xlsx files supported"}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": f"Failed to read file: {str(e)}"}), 400

    # Normalizar nombres de columnas
    cols_norm = {c.lower().strip(): c for c in df.columns}

    # SKU: tu export trae "Artículo - SKU"
    sku_col = None
    for k in ["articulo - sku", "artículo - sku", "sku", "seller sku", "seller_sku", "codigo", "código", "cod"]:
        if k in cols_norm:
            sku_col = cols_norm[k]
            break

    if not sku_col:
        return jsonify({
            "ok": False,
            "error": "Could not detect SKU column",
            "columns": list(df.columns)
        }), 400

    # Depósitos a sumar (si existen)
    deposit_keys = [
        "deposito 1", "depósito 1",
        "deposito 2", "depósito 2",
        "deposito 3", "depósito 3",
    ]
    deposit_cols = [cols_norm[k] for k in deposit_keys if k in cols_norm]

    # Fallback si no hay depósitos: usar FULL u otras columnas genéricas
    stock_col = None
    for k in ["full", "stock disponible", "stock_disponible", "available", "qty", "quantity", "stock", "disponible"]:
        if k in cols_norm:
            stock_col = cols_norm[k]
            break

    # Construir salida base
    out = df[[sku_col]].copy()
    out.columns = ["sku"]
    out["sku"] = out["sku"].astype(str).str.strip()

    # Calcular stock
    if deposit_cols:
        tmp = df[deposit_cols].copy()
        for c in deposit_cols:
            tmp[c] = pd.to_numeric(tmp[c], errors="coerce").fillna(0)
        out["stock"] = tmp.sum(axis=1).astype(int)
        detected_stock = {"mode": "sum_deposits", "columns": deposit_cols}
    elif stock_col:
        out["stock"] = pd.to_numeric(df[stock_col], errors="coerce").fillna(0).astype(int)
        detected_stock = {"mode": "single_column", "column": stock_col}
    else:
        return jsonify({
            "ok": False,
            "error": "Could not detect Stock columns",
            "columns": list(df.columns)
        }), 400

    # Stock para alertas (no negativos)
    out["stock_alerta"] = out["stock"].clip(lower=0)

    total_rows = int(len(out))
    total_skus = int(out["sku"].nunique())

    # Persistir en base
    try:
        ensure_tables()

        rows_latest = [
            (r["sku"], int(r["stock"]), int(r["stock_alerta"]))
            for r in out[["sku", "stock", "stock_alerta"]].to_dict(orient="records")
        ]
        rows_snapshot = [(f.filename, sku, sr, sa) for (sku, sr, sa) in rows_latest]

        with get_db_conn() as conn:
            with conn.cursor() as cur:
                # histórico
                execute_values(
                    cur,
                    """
                    INSERT INTO stock_snapshot (source_filename, sku, stock_real, stock_alerta)
                    VALUES %s
                    """,
                    rows_snapshot,
                    page_size=1000
                )

                # latest upsert
                # latest upsert (updated_at lo pone la DB)
                execute_values(
                cur,
                """
                INSERT INTO stock_latest (sku, stock_real, stock_alerta)
                VALUES %s
                ON CONFLICT (sku)
                DO UPDATE SET
                    stock_real = EXCLUDED.stock_real,
                    stock_alerta = EXCLUDED.stock_alerta,
                    updated_at = NOW()
                """,
                rows_latest,
                page_size=1000
                )

            conn.commit()

        db_rows_inserted = total_rows
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"DB insert failed: {str(e)}"
        }), 500

    # Low stock usando stock_alerta
    low_stock = out[out["stock_alerta"] <= 2].sort_values("stock_alerta").head(20)

    return jsonify({
        "ok": True,
        "type": "file",
        "received_at": received_at,
        "filename": f.filename,
        "detected_columns": {"sku": sku_col, "stock": detected_stock},
        "total_rows": total_rows,
        "unique_skus": total_skus,
        "db_rows_inserted": db_rows_inserted,
        "low_stock_sample": low_stock[["sku", "stock", "stock_alerta"]].to_dict(orient="records")
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")))


