from pydantic import BaseModel, EmailStr
from typing import Optional


PRICE_PER_EMAIL = 0.002  # $0.002 per email sent


class RecipientIn(BaseModel):
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company: Optional[str] = None
    custom: Optional[dict] = None


class CampaignCreate(BaseModel):
    name: str
    from_name: str
    from_email: str
    subject_template: str    # supports {{first_name}}, {{company}}
    body_template: str       # plain text, supports {{first_name}}, {{company}}
    recipients: list[RecipientIn]


class CampaignResponse(BaseModel):
    id: int
    name: str
    from_name: str
    from_email: str
    subject_template: str
    status: str              # draft | scheduled | sending | sent | paused
    recipient_count: int
    sent_count: int
    opened_count: int
    clicked_count: int
    replied_count: int
    bounced_count: int
    cost_usd: float
    created_at: str


class SendResponse(BaseModel):
    campaign_id: int
    sent: int
    cost_usd: float
    price_per_email: float


class EventIn(BaseModel):
    campaign_id: int
    recipient_email: str
    event_type: str   # opened | clicked | replied | bounced | unsubscribed


class StatsResponse(BaseModel):
    campaign_id: int
    name: str
    sent: int
    open_rate_pct: float
    click_rate_pct: float
    reply_rate_pct: float
    bounce_rate_pct: float
    cost_usd: float
    cost_per_open: Optional[float]
    cost_per_reply: Optional[float]
