from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
import aiosqlite
from models import CampaignCreate, CampaignResponse, SendResponse, EventIn, StatsResponse
from engine import init_db, create_campaign, list_campaigns, get_campaign, send_campaign, record_event, get_stats

DB_PATH = "coldreach.db"


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with aiosqlite.connect(DB_PATH) as db:
        await init_db()
    yield


app = FastAPI(
    title="ColdReach",
    description="Pay-as-you-go cold email campaign manager. Create campaigns with templates, track recipients, record engagement events. PAYG pricing at $0.002/email — no $80/mo flat fee.",
    version="1.0.0",
    lifespan=lifespan,
)


async def get_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db


@app.post("/campaigns", response_model=CampaignResponse, status_code=201)
async def create(body: CampaignCreate, db=Depends(get_db)):
    """Create a campaign with template and recipient list."""
    return await create_campaign(db, body.model_dump())


@app.get("/campaigns", response_model=list[CampaignResponse])
async def index(db=Depends(get_db)):
    """List all campaigns with current send stats."""
    return await list_campaigns(db)


@app.get("/campaigns/{campaign_id}", response_model=CampaignResponse)
async def detail(campaign_id: int, db=Depends(get_db)):
    c = await get_campaign(db, campaign_id)
    if not c:
        raise HTTPException(404, "Campaign not found")
    return c


@app.post("/campaigns/{campaign_id}/send", response_model=SendResponse)
async def send(campaign_id: int, db=Depends(get_db)):
    """Send campaign to all recipients. Charges PAYG: $0.002 per email. Returns cost breakdown."""
    result = await send_campaign(db, campaign_id)
    if not result:
        raise HTTPException(404, "Campaign not found")
    return result


@app.post("/events", status_code=201)
async def log_event(body: EventIn, db=Depends(get_db)):
    """Record an engagement event: opened, clicked, replied, bounced, unsubscribed."""
    c = await get_campaign(db, body.campaign_id)
    if not c:
        raise HTTPException(404, "Campaign not found")
    await record_event(db, body.campaign_id, body.recipient_email, body.event_type)
    return {"status": "recorded"}


@app.get("/campaigns/{campaign_id}/stats", response_model=StatsResponse)
async def campaign_stats(campaign_id: int, db=Depends(get_db)):
    """Stats: open rate, click rate, reply rate, cost per open, cost per reply."""
    stats = await get_stats(db, campaign_id)
    if not stats:
        raise HTTPException(404, "Campaign not found")
    return stats
