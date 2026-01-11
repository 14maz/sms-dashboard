import os
import time
import csv
import sqlite3
import asyncio
from datetime import datetime
from typing import Optional, List

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Form, UploadFile, File, Depends, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import africastalking

load_dotenv()

APP_NAME = os.getenv("APP_NAME", "SMS Dashboard")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "change_me_now")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8000").rstrip("/")

AT_USERNAME = os.getenv("AT_USERNAME", "sandbox")
AT_API_KEY = os.getenv("AT_API_KEY", "")
AT_SENDER_ID = os.getenv("AT_SENDER_ID", "").strip() or None

SEND_PER_SECOND = max(1, min(30, int(os.getenv("SEND_PER_SECOND", "2"))))
MAX_DAILY_PER_CONTACT = max(1, min(20, int(os.getenv("MAX_DAILY_PER_CONTACT", "3"))))

DB_PATH = os.getenv("DB_PATH", "sms.db")

app = FastAPI(title=APP_NAME)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# -------------------------
# DB helpers
# -------------------------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS contacts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT DEFAULT '',
          phone TEXT UNIQUE NOT NULL,
          consented INTEGER NOT NULL DEFAULT 0,
          opted_out INTEGER NOT NULL DEFAULT 0,
          tags TEXT DEFAULT '',
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS campaigns (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          message_template TEXT NOT NULL,
          target_tag TEXT DEFAULT '', -- empty means all
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          started_at TEXT,
          completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          campaign_id INTEGER NOT NULL,
          contact_id INTEGER NOT NULL,
          to_phone TEXT NOT NULL,
          body TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'queued', -- queued|sending|sent|failed|skipped
          provider_id TEXT,
          error TEXT,
          created_at TEXT NOT NULL DEFAULT (datetime('now')),
          sent_at TEXT,
          FOREIGN KEY(campaign_id) REFERENCES campaigns(id),
          FOREIGN KEY(contact_id) REFERENCES contacts(id)
        );

        CREATE TABLE IF NOT EXISTS audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          action TEXT NOT NULL,
          meta TEXT DEFAULT '',
          created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )

    conn.commit()
    conn.close()


def audit(action: str, meta: str = ""):
    conn = db()
    conn.execute("INSERT INTO audit_log(action, meta) VALUES (?, ?)", (action, meta))
    conn.commit()
    conn.close()


def render_template(tpl: str, contact: sqlite3.Row) -> str:
    s = tpl
    s = s.replace("{{name}}", contact["name"] or "")
    s = s.replace("{{phone}}", contact["phone"] or "")
    s = s.replace("{{tags}}", contact["tags"] or "")
    return s


# -------------------------
# Auth (simple token)
# -------------------------
def require_admin(request: Request):
    token = request.query_params.get("token") or request.headers.get("x-admin-token")
    if not token or token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return token


# -------------------------
# Africa's Talking init
# -------------------------
_sms_client_ready = False

def init_sms():
    global _sms_client_ready
    if _sms_client_ready:
        return
    if not AT_API_KEY:
        # Allow running dashboard even if key missing (no send)
        return
    africastalking.initialize(AT_USERNAME, AT_API_KEY)
    _sms_client_ready = True


def send_sms_africastalking(to_phone: str, body: str) -> str:
    """
    Returns provider_id/messageId if available.
    Raises Exception if send fails.
    """
    init_sms()
    if not _sms_client_ready:
        raise Exception("Africa's Talking not configured. Set AT_API_KEY in .env")

    sms = africastalking.SMS
    data = {
        "message": body,
        "recipients": [to_phone]
    }
    if AT_SENDER_ID:
        data["sender_id"] = AT_SENDER_ID

    resp = sms.send(**data)

    # Parse SDK response
    rec = None
    try:
        rec = resp["SMSMessageData"]["Recipients"][0]
    except Exception:
        rec = None

    if rec:
        status = (rec.get("status") or "").lower()
        if status != "success":
            raise Exception(f"AT send failed: {rec.get('status')} ({rec.get('statusCode')})")
        return rec.get("messageId") or rec.get("statusCode") or f"at_{int(time.time())}"

    return f"at_{int(time.time())}"


# -------------------------
# Background sender
# -------------------------
async def worker_loop():
    """
    Every second, attempt up to SEND_PER_SECOND queued messages.
    """
    while True:
        try:
            await process_queue_tick()
        except Exception as e:
            print("Worker error:", e)
        await asyncio.sleep(1)


def count_sent_today(conn: sqlite3.Connection, contact_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM messages
        WHERE contact_id=? AND status='sent' AND date(sent_at)=date('now')
        """,
        (contact_id,)
    ).fetchone()
    return int(row["n"])


async def process_queue_tick():
    conn = db()

    # fetch up to SEND_PER_SECOND queued
    rows = conn.execute(
        """
        SELECT m.*, c.consented, c.opted_out
        FROM messages m
        JOIN contacts c ON c.id=m.contact_id
        WHERE m.status='queued'
        ORDER BY m.created_at ASC
        LIMIT ?
        """,
        (SEND_PER_SECOND,)
    ).fetchall()

    for m in rows:
        # Safety checks
        if int(m["consented"]) != 1 or int(m["opted_out"]) == 1:
            conn.execute(
                "UPDATE messages SET status='skipped', error='no consent or opted out' WHERE id=?",
                (m["id"],)
            )
            conn.commit()
            continue

        # Daily cap
        if count_sent_today(conn, int(m["contact_id"])) >= MAX_DAILY_PER_CONTACT:
            conn.execute(
                "UPDATE messages SET status='skipped', error='daily cap reached' WHERE id=?",
                (m["id"],)
            )
            conn.commit()
            continue

        # Mark sending
        conn.execute("UPDATE messages SET status='sending' WHERE id=?", (m["id"],))
        conn.commit()

        try:
            provider_id = send_sms_africastalking(m["to_phone"], m["body"])
            conn.execute(
                """
                UPDATE messages
                SET status='sent', provider_id=?, sent_at=datetime('now')
                WHERE id=?
                """,
                (provider_id, m["id"])
            )
            conn.commit()
        except Exception as e:
            conn.execute(
                "UPDATE messages SET status='failed', error=? WHERE id=?",
                (str(e), m["id"])
            )
            conn.commit()

    # Mark campaigns completed if nothing queued/sending for that campaign
    started = conn.execute(
        "SELECT id FROM campaigns WHERE started_at IS NOT NULL AND completed_at IS NULL"
    ).fetchall()
    for c in started:
        left = conn.execute(
            """
            SELECT COUNT(*) AS n FROM messages
            WHERE campaign_id=? AND status IN ('queued','sending')
            """,
            (c["id"],)
        ).fetchone()
        if int(left["n"]) == 0:
            conn.execute(
                "UPDATE campaigns SET completed_at=datetime('now') WHERE id=?",
                (c["id"],)
            )
            conn.commit()

    conn.close()


@app.on_event("startup")
async def startup():
    init_db()
    # start background worker
    asyncio.create_task(worker_loop())


# -------------------------
# Routes (UI)
# -------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # if token valid -> dashboard
    token = request.query_params.get("token")
    if token == ADMIN_TOKEN:
        return RedirectResponse(url=f"/dashboard?token={token}")
    return templates.TemplateResponse("login.html", {"request": request, "app_name": APP_NAME})


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, token: str = Depends(require_admin)):
    conn = db()
    stats = {}
    stats["contacts"] = conn.execute("SELECT COUNT(*) AS n FROM contacts").fetchone()["n"]
    stats["opted_out"] = conn.execute("SELECT COUNT(*) AS n FROM contacts WHERE opted_out=1").fetchone()["n"]
    stats["campaigns"] = conn.execute("SELECT COUNT(*) AS n FROM campaigns").fetchone()["n"]

    msg_counts = conn.execute(
        "SELECT status, COUNT(*) AS n FROM messages GROUP BY status"
    ).fetchall()
    stats["messages"] = {r["status"]: r["n"] for r in msg_counts}

    conn.close()
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "token": token, "app_name": APP_NAME, "stats": stats}
    )


@app.get("/contacts", response_class=HTMLResponse)
def contacts_page(request: Request, token: str = Depends(require_admin)):
    conn = db()
    contacts = conn.execute(
        "SELECT * FROM contacts ORDER BY created_at DESC LIMIT 500"
    ).fetchall()
    conn.close()
    return templates.TemplateResponse(
        "contacts.html",
        {"request": request, "token": token, "app_name": APP_NAME, "contacts": contacts}
    )


@app.post("/contacts/add")
def contacts_add(
    request: Request,
    name: str = Form(""),
    phone: str = Form(...),
    tags: str = Form(""),
    consented: Optional[str] = Form(None),
    token: str = Depends(require_admin),
):
    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO contacts(name, phone, tags, consented)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(phone) DO UPDATE SET
              name=excluded.name,
              tags=excluded.tags,
              consented=MAX(consented, excluded.consented)
            """,
            (name.strip(), phone.strip(), tags.strip(), 1 if consented else 0),
        )
        conn.commit()
        audit("contact_added", phone.strip())
    finally:
        conn.close()
    return RedirectResponse(url=f"/contacts?token={token}", status_code=303)


@app.post("/contacts/import_csv")
async def contacts_import_csv(
    request: Request,
    file: UploadFile = File(...),
    token: str = Depends(require_admin),
):
    """
    CSV columns (recommended):
      phone,name,consented,tags
    consented: 1/0 or true/false
    """
    content = (await file.read()).decode("utf-8", errors="replace")
    reader = csv.DictReader(content.splitlines())

    conn = db()
    count = 0
    try:
        for row in reader:
            phone = (row.get("phone") or "").strip()
            if not phone:
                continue
            name = (row.get("name") or "").strip()
            tags = (row.get("tags") or "").strip()

            c = (row.get("consented") or "").strip().lower()
            consented = 1 if c in ("1", "true", "yes", "y") else 0

            conn.execute(
                """
                INSERT INTO contacts(name, phone, tags, consented)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(phone) DO UPDATE SET
                  name=excluded.name,
                  tags=excluded.tags,
                  consented=MAX(consented, excluded.consented)
                """,
                (name, phone, tags, consented),
            )
            count += 1
        conn.commit()
        audit("contacts_import_csv", f"count={count}")
    finally:
        conn.close()

    return RedirectResponse(url=f"/contacts?token={token}", status_code=303)


@app.post("/contacts/{contact_id}/optout")
def contact_optout(contact_id: int, token: str = Depends(require_admin)):
    conn = db()
    conn.execute("UPDATE contacts SET opted_out=1 WHERE id=?", (contact_id,))
    conn.commit()
    conn.close()
    audit("contact_optout_admin", str(contact_id))
    return RedirectResponse(url=f"/contacts?token={token}", status_code=303)


@app.get("/campaigns", response_class=HTMLResponse)
def campaigns_page(request: Request, token: str = Depends(require_admin)):
    conn = db()
    campaigns = conn.execute(
        "SELECT * FROM campaigns ORDER BY created_at DESC LIMIT 200"
    ).fetchall()

    conn.close()
    return templates.TemplateResponse(
        "campaigns.html",
        {"request": request, "token": token, "app_name": APP_NAME, "campaigns": campaigns}
    )


@app.post("/campaigns/create")
def campaigns_create(
    request: Request,
    name: str = Form(...),
    message_template: str = Form(...),
    target_tag: str = Form(""),
    token: str = Depends(require_admin),
):
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO campaigns(name, message_template, target_tag) VALUES (?, ?, ?)",
        (name.strip(), message_template.strip(), target_tag.strip()),
    )
    conn.commit()
    cid = cur.lastrowid
    conn.close()
    audit("campaign_created", f"id={cid}")
    return RedirectResponse(url=f"/campaigns?token={token}", status_code=303)


@app.post("/campaigns/{campaign_id}/start")
def campaigns_start(campaign_id: int, token: str = Depends(require_admin)):
    conn = db()

    camp = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not camp:
        conn.close()
        raise HTTPException(404, "Campaign not found")

    # find recipients (consented & not opted-out)
    tag = (camp["target_tag"] or "").strip()
    if tag:
        recipients = conn.execute(
            """
            SELECT * FROM contacts
            WHERE consented=1 AND opted_out=0
              AND (',' || lower(tags) || ',') LIKE ?
            """,
            (f"%,{tag.lower()},%",),
        ).fetchall()
    else:
        recipients = conn.execute(
            "SELECT * FROM contacts WHERE consented=1 AND opted_out=0"
        ).fetchall()

    # queue messages
    unsub_base = f"{PUBLIC_BASE_URL}/u"
    q = conn.cursor()

    conn.execute("UPDATE campaigns SET started_at=datetime('now') WHERE id=?", (campaign_id,))

    for c in recipients:
        unsub = f"{unsub_base}/{c['phone']}"
        body = render_template(camp["message_template"], c)
        # Include opt-out link
        body = body.strip() + f"\nOpt out: {unsub}"

        q.execute(
            """
            INSERT INTO messages(campaign_id, contact_id, to_phone, body, status)
            VALUES (?, ?, ?, ?, 'queued')
            """,
            (campaign_id, c["id"], c["phone"], body),
        )

    conn.commit()
    conn.close()
    audit("campaign_started", f"id={campaign_id},queued={len(recipients)}")

    return RedirectResponse(url=f"/campaigns?token={token}", status_code=303)


@app.get("/campaigns/{campaign_id}/stats", response_class=HTMLResponse)
def campaigns_stats(request: Request, campaign_id: int, token: str = Depends(require_admin)):
    conn = db()
    camp = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    if not camp:
        conn.close()
        raise HTTPException(404, "Campaign not found")

    counts = conn.execute(
        "SELECT status, COUNT(*) AS n FROM messages WHERE campaign_id=? GROUP BY status",
        (campaign_id,),
    ).fetchall()

    last_msgs = conn.execute(
        """
        SELECT * FROM messages
        WHERE campaign_id=?
        ORDER BY created_at DESC
        LIMIT 50
        """,
        (campaign_id,),
    ).fetchall()

    conn.close()
    return templates.TemplateResponse(
        "campaigns.html",
        {
            "request": request,
            "token": token,
            "app_name": APP_NAME,
            "campaigns": [],  # hide list, show stats section
            "stats_view": True,
            "camp": camp,
            "counts": counts,
            "last_msgs": last_msgs,
        },
    )


# -------------------------
# Public Unsubscribe
# -------------------------
@app.get("/u/{phone}", response_class=HTMLResponse)
def unsubscribe(phone: str, request: Request):
    conn = db()
    conn.execute("UPDATE contacts SET opted_out=1 WHERE phone=?", (phone,))
    conn.commit()
    conn.close()
    audit("contact_optout_public", phone)

    return HTMLResponse(
        f"""
        <html><head><title>Unsubscribed</title></head>
        <body style="font-family:system-ui;padding:24px;">
          <h2>You have been unsubscribed ✅</h2>
          <p>We will not send further messages to <b>{phone}</b>.</p>
        </body></html>
        """
    )
