"""PDF and export reports."""
from __future__ import annotations

import io

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from backend.api.deps import require_auth

router = APIRouter(tags=["reports"], prefix="/reports")


@router.get("/{mlbam_id}/pdf")
def report_pdf(mlbam_id: int, _user: dict = Depends(require_auth)):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen import canvas
    except ImportError as exc:
        raise HTTPException(status_code=501, detail="reportlab not installed") from exc

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    c.drawString(72, 720, f"ScoutPro — Player report (MLBAM {mlbam_id})")
    c.drawString(72, 700, "Arrival gauge, salary bars, and GDS chart placeholders.")
    c.showPage()
    c.save()
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="scoutpro_{mlbam_id}.pdf"'},
    )
