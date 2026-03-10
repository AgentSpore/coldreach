import aiosqlite
import json
import re
from datetime import datetime

DB_PATH = "coldreach.db"
PRICE_PER_EMAIL = 0.002


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                from_name TEXT NOT NULL,
                from_email TEXT NOT NULL,
                subject_template TEXT NOT NULL,
                body_template TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                sent_count INTEGER NOT NULL DEFAULT 0,
                opened_count INTEGER NOT NULL DEFAULT 0,
                clicked_count INTEGER NOT NULL DEFAULT 0,
                replied_count INTEGER NOT NULL DEFAULT 0,
                bounced_count INTEGER NOT NULL DEFAULT 0,
                cost_usd REAL NOT NULL DEFAULT 0.0,
                created_at TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                company TEXT,
                custom TEXT,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER NOT NULL,
                recipient_email TEXT NOT NULL,
                event_type TEXT NOT NULL,
                recorded_at TEXT NOT NULL,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
            )
        """)
        await db.commit()


def _render(template: str, ctx: dict) -> str:
    for k, v in ctx.items():
        template = template.replace(f"{{{{{k}}}}}", v or "")
    return template


def _campaign_row(r, recipient_count: int = 0) -> dict:
    return {
        "id": r["id"], "name": r["name"], "from_name": r["from_name"],
        "from_email": r["from_email"], "subject_template": r["subject_template"],
        "status": r["status"], "recipient_count": recipient_count,
        "sent_count": r["sent_count"], "opened_count": r["opened_count"],
        "clicked_count": r["clicked_count"], "replied_count": r["replied_count"],
        "bounced_count": r["bounced_count"], "cost_usd": r["cost_usd"],
        "created_at": r["created_at"],
    }


async def _recipient_count(db, campaign_id: int) -> int:
    row = await (await db.execute("SELECT COUNT(*) FROM recipients WHERE campaign_id=?", (campaign_id,))).fetchone()
    return row[0] if row else 0


async def create_campaign(db, data: dict) -> dict:
    now = datetime.utcnow().isoformat()
    db.row_factory = aiosqlite.Row
    cur = await db.execute(
        """INSERT INTO campaigns (name, from_name, from_email, subject_template, body_template, created_at)
           VALUES (?,?,?,?,?,?)""",
        (data["name"], data["from_name"], data["from_email"],
         data["subject_template"], data["body_template"], now),
    )
    campaign_id = cur.lastrowid
    for rec in data.get("recipients", []):
        await db.execute(
            "INSERT INTO recipients (campaign_id, email, first_name, last_name, company, custom) VALUES (?,?,?,?,?,?)",
            (campaign_id, rec["email"], rec.get("first_name"), rec.get("last_name"),
             rec.get("company"), json.dumps(rec.get("custom")) if rec.get("custom") else None),
        )
    await db.commit()
    row = await (await db.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
    cnt = await _recipient_count(db, campaign_id)
    return _campaign_row(row, cnt)


async def list_campaigns(db) -> list:
    db.row_factory = aiosqlite.Row
    rows = await (await db.execute("SELECT * FROM campaigns ORDER BY id DESC")).fetchall()
    result = []
    for r in rows:
        cnt = await _recipient_count(db, r["id"])
        result.append(_campaign_row(r, cnt))
    return result


async def get_campaign(db, campaign_id: int):
    db.row_factory = aiosqlite.Row
    row = await (await db.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,))).fetchone()
    if not row:
        return None
    cnt = await _recipient_count(db, campaign_id)
    return _campaign_row(row, cnt)


async def send_campaign(db, campaign_id: int) -> dict | None:
    db.row_factory = aiosqlite.Row
    campaign = await get_campaign(db, campaign_id)
    if not campaign:
        return None
    recipients = await (await db.execute(
        "SELECT * FROM recipients WHERE campaign_id=?", (campaign_id,)
    )).fetchall()
    sent = len(recipients)
    cost = round(sent * PRICE_PER_EMAIL, 4)
    await db.execute(
        "UPDATE campaigns SET status='sent', sent_count=?, cost_usd=? WHERE id=?",
        (sent, cost, campaign_id),
    )
    await db.commit()
    return {"campaign_id": campaign_id, "sent": sent, "cost_usd": cost, "price_per_email": PRICE_PER_EMAIL}


async def record_event(db, campaign_id: int, recipient_email: str, event_type: str) -> None:
    now = datetime.utcnow().isoformat()
    await db.execute(
        "INSERT INTO events (campaign_id, recipient_email, event_type, recorded_at) VALUES (?,?,?,?)",
        (campaign_id, recipient_email, event_type, now),
    )
    col_map = {"opened": "opened_count", "clicked": "clicked_count",
               "replied": "replied_count", "bounced": "bounced_count"}
    if event_type in col_map:
        await db.execute(f"UPDATE campaigns SET {col_map[event_type]} = {col_map[event_type]} + 1 WHERE id=?", (campaign_id,))
    await db.commit()


async def get_stats(db, campaign_id: int) -> dict | None:
    c = await get_campaign(db, campaign_id)
    if not c:
        return None
    sent = c["sent_count"] or 0
    def pct(n): return round(n / sent * 100, 1) if sent else 0.0
    cost = c["cost_usd"]
    opens = c["opened_count"]
    replies = c["replied_count"]
    return {
        "campaign_id": campaign_id, "name": c["name"], "sent": sent,
        "open_rate_pct": pct(opens), "click_rate_pct": pct(c["clicked_count"]),
        "reply_rate_pct": pct(replies), "bounce_rate_pct": pct(c["bounced_count"]),
        "cost_usd": cost,
        "cost_per_open": round(cost / opens, 4) if opens else None,
        "cost_per_reply": round(cost / replies, 4) if replies else None,
    }


async def list_recipients(db, campaign_id: int) -> list:
    db.row_factory = aiosqlite.Row
    rows = await (await db.execute(
        "SELECT * FROM recipients WHERE campaign_id=? ORDER BY id ASC", (campaign_id,)
    )).fetchall()
    return [
        {
            "id": r["id"], "campaign_id": r["campaign_id"],
            "email": r["email"], "first_name": r["first_name"],
            "last_name": r["last_name"], "company": r["company"],
            "custom": json.loads(r["custom"]) if r["custom"] else None,
        }
        for r in rows
    ]


async def list_campaign_events(db, campaign_id: int, event_type: str | None = None) -> list:
    db.row_factory = aiosqlite.Row
    if event_type:
        rows = await (await db.execute(
            "SELECT * FROM events WHERE campaign_id=? AND event_type=? ORDER BY id DESC",
            (campaign_id, event_type),
        )).fetchall()
    else:
        rows = await (await db.execute(
            "SELECT * FROM events WHERE campaign_id=? ORDER BY id DESC", (campaign_id,)
        )).fetchall()
    return [
        {
            "id": r["id"], "campaign_id": r["campaign_id"],
            "recipient_email": r["recipient_email"],
            "event_type": r["event_type"], "recorded_at": r["recorded_at"],
        }
        for r in rows
    ]
