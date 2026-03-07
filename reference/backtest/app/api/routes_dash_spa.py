from pathlib import Path

import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse

router = APIRouter(tags=["ui"])

_DASH_STATIC_DIR = Path(__file__).resolve().parents[1] / "static" / "dash"
_DASH_INDEX = _DASH_STATIC_DIR / "index.html"


def _serve_dash_index() -> FileResponse:
    if not _DASH_INDEX.exists():
        raise HTTPException(status_code=503, detail="Dash frontend assets missing. Build frontend/dash and copy dist.")
    return FileResponse(_DASH_INDEX, media_type="text/html", headers={"Cache-Control": "no-store, max-age=0"})


@router.get("/dash")
@router.get("/dash/")
@router.get("/dash/{path:path}")
def dash_spa(request: Request, path: str = ""):
    if os.getenv("DASH_DEV_REDIRECT", "").strip() == "1":
        try:
            target_port = int(os.getenv("DASH_DEV_REDIRECT_PORT", "5173"))
        except ValueError:
            target_port = 5173
        if request.url.port != target_port:
            target = request.url.replace(port=target_port)
            return RedirectResponse(url=str(target), status_code=307)
    return _serve_dash_index()
