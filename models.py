from pydantic import BaseModel
from typing import Optional


PRICE_PER_EMAIL = 0.002


class RecipientIn(BaseModel):
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    company: Optional[str] = None
    custom: Optional[dict] = None


class RecipientResponse(BaseModel):
    id: int
    campaign_id: int
    email: str
    first_name: Optional[str]
    last_name: Optional[str]
    company: Optional[str]
    custom: Optional[dict]


class CampaignCreate(BaseModel):
    name: str
    from_name: str
    from_email: str
    subject_template: str
    body_template: str
    recipients: list[RecipientIn]


class CampaignResponse(BaseModel):
    id: int
    name: str
    from_name: str
    from_email: str
    subject_template: str
    status: str
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
    event_type: str


class EventResponse(BaseModel):
    id: int
    campaign_id: int
    recipient_email: str
    event_type: str
    recorded_at: str


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
