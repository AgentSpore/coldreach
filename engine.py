import aiosqlite
import json
import re
import random
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
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ab_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER NOT NULL UNIQUE,
                variants TEXT NOT NULL,
                sample_pct REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                winner_variant INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS ab_sends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                test_id INTEGER NOT NULL,
                variant_idx INTEGER NOT NULL,
                recipient_id INTEGER NOT NULL,
                recipient_email TEXT NOT NULL,
                FOREIGN KEY (test_id) REFERENCES ab_tests(id),
                FOREIGN KEY (recipient_id) REFERENCES recipients(id)
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


async def create_ab_test(db, campaign_id: int, variants: list[str], sample_pct: float) -> dict:
    db.row_factory = aiosqlite.Row

    existing = await (await db.execute(
        "SELECT id FROM ab_tests WHERE campaign_id=?", (campaign_id,)
    )).fetchone()
    if existing:
        raise ValueError("A/B test already exists for this campaign")

    all_recipients = await (await db.execute(
        "SELECT id, email FROM recipients WHERE campaign_id=?", (campaign_id,)
    )).fetchall()

    total = len(all_recipients)
    if total < len(variants):
        raise ValueError(f"Need at least {len(variants)} recipients, got {total}")

    sample_size = max(len(variants), int(total * sample_pct / 100))
    sample_size = min(sample_size, total)

    recipient_list = [(r["id"], r["email"]) for r in all_recipients]
    random.shuffle(recipient_list)
    sample = recipient_list[:sample_size]

    now = datetime.utcnow().isoformat()
    cur = await db.execute(
        "INSERT INTO ab_tests (campaign_id, variants, sample_pct, status, created_at) VALUES (?,?,?,?,?)",
        (campaign_id, json.dumps(variants), sample_pct, "running", now),
    )
    test_id = cur.lastrowid

    for i, (rid, email) in enumerate(sample):
        variant_idx = i % len(variants)
        await db.execute(
            "INSERT INTO ab_sends (test_id, variant_idx, recipient_id, recipient_email) VALUES (?,?,?,?)",
            (test_id, variant_idx, rid, email),
        )

    cost = round(sample_size * PRICE_PER_EMAIL, 4)
    await db.execute(
        "UPDATE campaigns SET sent_count = sent_count + ?, cost_usd = cost_usd + ?, status = 'ab_testing' WHERE id=?",
        (sample_size, cost, campaign_id),
    )
    await db.commit()

    return await get_ab_test(db, campaign_id)


async def get_ab_test(db, campaign_id: int) -> dict | None:
    db.row_factory = aiosqlite.Row

    test = await (await db.execute(
        "SELECT * FROM ab_tests WHERE campaign_id=?", (campaign_id,)
    )).fetchone()
    if not test:
        return None

    variants = json.loads(test["variants"])
    test_id = test["id"]

    total_recipients = await _recipient_count(db, campaign_id)
    sampled = await (await db.execute(
        "SELECT COUNT(*) FROM ab_sends WHERE test_id=?", (test_id,)
    )).fetchone()
    sampled_count = sampled[0] if sampled else 0

    variant_stats = []
    for idx, subject in enumerate(variants):
        row = await (await db.execute(
            "SELECT COUNT(*) FROM ab_sends WHERE test_id=? AND variant_idx=?", (test_id, idx)
        )).fetchone()
        sent = row[0] if row else 0

        emails = await (await db.execute(
            "SELECT recipient_email FROM ab_sends WHERE test_id=? AND variant_idx=?", (test_id, idx)
        )).fetchall()
        email_set = {e["recipient_email"] for e in emails}

        opened = 0
        clicked = 0
        if email_set:
            placeholders = ",".join("?" for _ in email_set)
            opened_row = await (await db.execute(
                f"SELECT COUNT(DISTINCT recipient_email) FROM events WHERE campaign_id=? AND event_type='opened' AND recipient_email IN ({placeholders})",
                (campaign_id, *email_set),
            )).fetchone()
            opened = opened_row[0] if opened_row else 0

            clicked_row = await (await db.execute(
                f"SELECT COUNT(DISTINCT recipient_email) FROM events WHERE campaign_id=? AND event_type='clicked' AND recipient_email IN ({placeholders})",
                (campaign_id, *email_set),
            )).fetchone()
            clicked = clicked_row[0] if clicked_row else 0

        variant_stats.append({
            "variant_idx": idx,
            "subject": subject,
            "sent": sent,
            "opened": opened,
            "clicked": clicked,
            "open_rate_pct": round(opened / sent * 100, 1) if sent else 0.0,
            "click_rate_pct": round(clicked / sent * 100, 1) if sent else 0.0,
        })

    winner_subject = None
    if test["winner_variant"] is not None:
        winner_subject = variants[test["winner_variant"]]

    return {
        "id": test["id"],
        "campaign_id": campaign_id,
        "status": test["status"],
        "sample_pct": test["sample_pct"],
        "total_sample": sampled_count,
        "variants": variant_stats,
        "winner_variant": test["winner_variant"],
        "winner_subject": winner_subject,
        "remaining_to_send": total_recipients - sampled_count,
        "created_at": test["created_at"],
    }


async def pick_ab_winner(db, campaign_id: int) -> dict:
    db.row_factory = aiosqlite.Row

    test_data = await get_ab_test(db, campaign_id)
    if not test_data:
        raise ValueError("No A/B test found for this campaign")
    if test_data["status"] == "completed":
        raise ValueError("A/B test already completed")

    best = max(test_data["variants"], key=lambda v: (v["open_rate_pct"], v["click_rate_pct"]))
    winner_idx = best["variant_idx"]
    winner_subject = best["subject"]

    test = await (await db.execute(
        "SELECT id FROM ab_tests WHERE campaign_id=?", (campaign_id,)
    )).fetchone()
    test_id = test["id"]

    sent_rids = await (await db.execute(
        "SELECT recipient_id FROM ab_sends WHERE test_id=?", (test_id,)
    )).fetchall()
    sent_rid_set = {r["recipient_id"] for r in sent_rids}

    all_recipients = await (await db.execute(
        "SELECT id, email FROM recipients WHERE campaign_id=?", (campaign_id,)
    )).fetchall()
    remaining = [(r["id"], r["email"]) for r in all_recipients if r["id"] not in sent_rid_set]
    remaining_count = len(remaining)

    for rid, email in remaining:
        await db.execute(
            "INSERT INTO ab_sends (test_id, variant_idx, recipient_id, recipient_email) VALUES (?,?,?,?)",
            (test_id, winner_idx, rid, email),
        )

    await db.execute(
        "UPDATE ab_tests SET status='completed', winner_variant=? WHERE id=?",
        (winner_idx, test_id),
    )

    cost_remaining = round(remaining_count * PRICE_PER_EMAIL, 4)
    await db.execute(
        "UPDATE campaigns SET sent_count = sent_count + ?, cost_usd = cost_usd + ?, status = 'sent', subject_template = ? WHERE id=?",
        (remaining_count, cost_remaining, winner_subject, campaign_id),
    )
    await db.commit()

    return {
        "campaign_id": campaign_id,
        "test_id": test_id,
        "winner_variant": winner_idx,
        "winner_subject": winner_subject,
        "remaining_sent": remaining_count,
        "cost_usd": cost_remaining,
    }
