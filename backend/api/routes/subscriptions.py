"""Stripe checkout and subscription status."""
from __future__ import annotations

import os

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from backend.api.deps import require_auth

router = APIRouter(tags=["subscriptions"], prefix="/subscriptions")


@router.post("/create-checkout")
def create_checkout(plan: str = Query("starter"), user: dict = Depends(require_auth)):
    secret = os.getenv("SCOUTPRO_STRIPE_SECRET_KEY", "")
    if not secret or secret.startswith("sk_test_placeholder"):
        return {
            "status": "dev_mode",
            "message": "Set SCOUTPRO_STRIPE_SECRET_KEY and price env vars for live Checkout",
            "plan": plan,
            "user_id": user.get("user_id"),
        }
    try:
        import stripe

        stripe.api_key = secret
        price = os.getenv(f"STRIPE_PRICE_{plan.upper()}", os.getenv("STRIPE_PRICE_STARTER", ""))
        if not price:
            raise HTTPException(status_code=400, detail=f"No Stripe price for plan {plan}")
        session = stripe.checkout.Session.create(
            mode="subscription",
            line_items=[{"price": price, "quantity": 1}],
            success_url=os.getenv("STRIPE_SUCCESS_URL", "http://localhost:5173/dashboard"),
            cancel_url=os.getenv("STRIPE_CANCEL_URL", "http://localhost:5173/pricing"),
            metadata={"user_id": user.get("user_id", "")},
        )
        return {"url": session.url}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/webhook")
async def stripe_webhook(request: Request):
    secret = os.getenv("SCOUTPRO_STRIPE_WEBHOOK_SECRET", "")
    payload = await request.body()
    if not secret or secret.startswith("whsec_placeholder"):
        return {"received": True, "note": "webhook secret not configured"}
    try:
        import stripe

        sig = request.headers.get("stripe-signature", "")
        event = stripe.Webhook.construct_event(payload, sig, secret)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if event["type"] == "checkout.session.completed":
        pass
    return {"ok": True}


@router.get("/status")
def subscription_status(user: dict = Depends(require_auth)):
    database_url = os.getenv("DATABASE_URL")
    uid = user.get("user_id")
    if not database_url or not uid:
        return {"tier": "starter", "reports_used_this_month": 0, "reports_limit": 10}
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tier, reports_used_this_month, reports_limit
                FROM subscriptions WHERE clerk_user_id = %s
                """,
                (uid,),
            )
            row = cur.fetchone()
    if not row:
        return {"tier": "starter", "reports_used_this_month": 0, "reports_limit": 10}
    return {"tier": row[0], "reports_used_this_month": row[1], "reports_limit": row[2]}
