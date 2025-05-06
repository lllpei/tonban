"""
Tonban API (Export/Import HS Code Search)
========================================
Render.com‐ready Flask API server for searching Japanese export/import統番 tables.

Endpoints
---------
GET /tonban/export           : code=統番 で単一レコード取得
GET /tonban/export/search    : q=キーワード で品名・分類タイトル検索。limit=件数
GET /tonban/import           : code=統番 で単一レコード取得（関税率列を含む）
GET /tonban/import/search    : q=キーワード で品名・分類タイトル検索。limit=件数

共通レスポンス構造
------------------
{
  "resultCd": true/false,
  "message" : エラーメッセージ文字列 (失敗時のみ),
  "data"    : [...]
}

• SQLite ファイルは同ディレクトリの "統番.db" を利用。
• PORT, WORKERS 環境変数を Render が注入。ローカル実行時は PORT=10010 既定。
• Hypercorn を使った ASGI 起動 (render python buildpack 対応)。

Author: ChatGPT (2025‑05‑06)
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from flask import Flask, jsonify, request

# ──────────────────────────────────────────────
# ロギング設定
# ──────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "tonban_api.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Flask アプリ & DB
# ──────────────────────────────────────────────
app = Flask(__name__)
DB_FILE = Path("統番.db")

# ──────────────────────────────────────────────
# SQL テンプレート
# ──────────────────────────────────────────────
SELECT_COMMON = """
SELECT
    b.部番,
    r.類番,
    k.項番,
    g.号番,
    te.統番,
    b.部タイトル,
    r.類タイトル,
    k.項タイトル,
    g.号タイトル,
    b.部注,
    r.類注,
    te.品名,
    te.単位1,
    te.単位2,
    te.他法令
"""

SELECT_IMPORT_EXTRA = """
    ,te.関税率_基本
    ,te.関税率_暫定
    ,te.関税率_WTO
    ,te.関税率_特恵GSP
    ,te.関税率_特恵LDC
    ,te.関税率_EPA_SG
    ,te.関税率_EPA_MX
    ,te.関税率_EPA_MY
    ,te.関税率_EPA_CL
    ,te.関税率_EPA_TH
    ,te.関税率_EPA_ID
    ,te.関税率_EPA_BN
    ,te.関税率_EPA_ASEAN
    ,te.関税率_EPA_PH
    ,te.関税率_EPA_CH
    ,te.関税率_EPA_VN
    ,te.関税率_EPA_IN
    ,te.関税率_EPA_PE
    ,te.関税率_EPA_AU
    ,te.関税率_EPA_MN
    ,te.関税率_EPA_CPTPP
    ,te.関税率_EPA_EU
    ,te.関税率_EPA_UK
    ,te.関税率_EPA_RCEP1
    ,te.関税率_EPA_RCEP2
    ,te.関税率_EPA_RCEP3
    ,te.関税率_US
"""

SQL_EXPORT_CODE = (
    SELECT_COMMON
    + """
FROM   輸出統番 AS te
JOIN   号番   AS g ON g.号番 = substr(te.統番,1,7)
JOIN   項番   AS k ON k.項番 = g.項番
JOIN   類番   AS r ON r.類番 = k.類番
JOIN   部番   AS b ON b.部番 = r.部番
WHERE  te.統番 = :code;
"""
)

SQL_EXPORT_NAME = (
    SELECT_COMMON
    + """
FROM   輸出統番 AS te
JOIN   号番   AS g ON g.号番 = substr(te.統番,1,7)
JOIN   項番   AS k ON k.項番 = g.項番
JOIN   類番   AS r ON r.類番 = k.類番
JOIN   部番   AS b ON b.部番 = r.部番
WHERE  te.品名      LIKE :kw
   OR  b.部タイトル  LIKE :kw
   OR  r.類タイトル  LIKE :kw
   OR  k.項タイトル  LIKE :kw
   OR  g.号タイトル  LIKE :kw
ORDER BY te.統番
LIMIT :limit;
"""
)

SQL_IMPORT_CODE = (
    SELECT_COMMON + SELECT_IMPORT_EXTRA
    + """
FROM   輸入統番 AS te
JOIN   号番   AS g ON g.号番 = substr(te.統番,1,7)
JOIN   項番   AS k ON k.項番 = g.項番
JOIN   類番   AS r ON r.類番 = k.類番
JOIN   部番   AS b ON b.部番 = r.部番
WHERE  te.統番 = :code;
"""
)

SQL_IMPORT_NAME = (
    SELECT_COMMON + SELECT_IMPORT_EXTRA
    + """
FROM   輸入統番 AS te
JOIN   号番   AS g ON g.号番 = substr(te.統番,1,7)
JOIN   項番   AS k ON k.項番 = g.項番
JOIN   類番   AS r ON r.類番 = k.類番
JOIN   部番   AS b ON b.部番 = r.部番
WHERE  te.品名      LIKE :kw
   OR  b.部タイトル  LIKE :kw
   OR  r.類タイトル  LIKE :kw
   OR  k.項タイトル  LIKE :kw
   OR  g.号タイトル  LIKE :kw
ORDER BY te.統番
LIMIT :limit;
"""
)

# ──────────────────────────────────────────────
# DB ユーティリティ
# ──────────────────────────────────────────────

def _query(sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Execute query and return list of dict rows."""
    if not DB_FILE.exists():
        logger.error("DB ファイルが見つかりません: %s", DB_FILE)
        return []
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ──────────────────────────────────────────────
# 共通レスポンスラッパ
# ──────────────────────────────────────────────

def _success(data: Any):
    return jsonify({"resultCd": True, "data": data})


def _error(msg: str, status: int = 400):
    return jsonify({"resultCd": False, "message": msg}), status

# ──────────────────────────────────────────────
# ルート定義
# ──────────────────────────────────────────────

## Export --------------------------------------

@app.route("/tonban/export")
def export_by_code():
    code = request.args.get("code", "").strip()
    if not code:
        return _error("code パラメータを指定してください")
    data = _query(SQL_EXPORT_CODE, {"code": code})
    if not data:
        return _error(f"統番 {code} が見つかりません", 404)
    return _success(data)


@app.route("/tonban/export/search")
def export_by_name():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return _error("q パラメータを2文字以上で指定してください")
    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return _error("limit パラメータは整数で指定してください")
    limit = max(1, min(limit, 1000))
    data = _query(SQL_EXPORT_NAME, {"kw": f"%{q}%", "limit": limit})
    return _success(data)

## Import --------------------------------------

@app.route("/tonban/import")
def import_by_code():
    code = request.args.get("code", "").strip()
    if not code:
        return _error("code パラメータを指定してください")
    data = _query(SQL_IMPORT_CODE, {"code": code})
    if not data:
        return _error(f"統番 {code} が見つかりません", 404)
    return _success(data)


@app.route("/tonban/import/search")
def import_by_name():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return _error("q パラメータを2文字以上で指定してください")
    try:
        limit = int(request.args.get("limit", 100))
    except ValueError:
        return _error("limit パラメータは整数で指定してください")
    limit = max(1, min(limit, 1000))
    data = _query(SQL_IMPORT_NAME, {"kw": f"%{q}%", "limit": limit})
    return _success(data)

# ──────────────────────────────────────────────
# インデックス作成ユーティリティ (初回のみ呼び出し)
# ──────────────────────────────────────────────

def create_indexes() -> None:
    """有用なインデックスを自動作成 (存在しない場合のみ)。"""
    if not DB_FILE.exists():
        logger.warning("DB ファイル %s が見つかりません。インデックス作成をスキップ", DB_FILE)
        return
    conn = sqlite3.connect(DB_FILE)
    idx_sqls = [
        "CREATE INDEX IF NOT EXISTS idx_export_code   ON 輸出統番(統番);",
        "CREATE INDEX IF NOT EXISTS idx_export_name   ON 輸出統番(品名);",
        "CREATE INDEX IF NOT EXISTS idx_import_code   ON 輸入統番(統番);",
        "CREATE INDEX IF NOT EXISTS idx_import_name   ON 輸入統番(品名);",
    ]
    for s in idx_sqls:
        conn.execute(s)
    conn.commit()
    conn.close()
    logger.info("インデックス作成完了")

# ──────────────────────────────────────────────
# エントリポイント (Hypercorn)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    # インデックス作成
    create_indexes()
    logger.info("インデックス作成完了")

    # ポート設定
    port = int(os.environ.get("PORT", 10010))
    logger.info(f"Tonban API サーバー起動: http://0.0.0.0:{port}")

    # ASGIアプリケーションとして設定
    asgi_app = app
