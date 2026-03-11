from pydantic import BaseModel, Field
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


class ABTestCreate(BaseModel):
    variants: list[str] = Field(..., min_length=2, max_length=5, description="Subject line variants to test")
    sample_pct: float = Field(20.0, gt=0, le=50, description="Percent of recipients for the test sample (split evenly across variants)")


class ABVariantStats(BaseModel):
    variant_idx: int
    subject: str
    sent: int
    opened: int
    clicked: int
    open_rate_pct: float
    click_rate_pct: float


class ABTestResponse(BaseModel):
    id: int
    campaign_id: int
    status: str
    sample_pct: float
    total_sample: int
    variants: list[ABVariantStats]
    winner_variant: Optional[int]
    winner_subject: Optional[str]
    remaining_to_send: int
    created_at: str


class ABWinnerResponse(BaseModel):
    campaign_id: int
    test_id: int
    winner_variant: int
    winner_subject: str
    remaining_sent: int
    cost_usd: float
