from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from app.config import settings
from app.services.db import db_service
from app.services.notifications import notification_service
from app.services.updater import updater_service

router = APIRouter(prefix="/api/system", tags=["System & Updates"])


class LanguageRequest(BaseModel):
    language: str


@router.get("/language")
async def get_system_language():
    """Restituisce la lingua attualmente configurata per la dashboard e le notifiche."""
    lang = await notification_service.get_language()
    return {"status": "success", "language": lang}


@router.post("/language")
async def set_system_language(payload: LanguageRequest):
    """Aggiorna e persiste la lingua preferita per la dashboard e le notifiche."""
    lang = payload.language.lower().strip()
    if lang not in ("it", "en"):
        raise HTTPException(status_code=400, detail="Lingua non supportata. Valori validi: 'en', 'it'")
    await notification_service.set_language(lang)
    return {"status": "success", "language": lang}


@router.get("/update/check")
async def check_for_updates(force: bool = Query(False, description="Forza il controllo remoto senza usare la cache")):
    """Controlla se è disponibile una nuova versione su Docker Hub e GitHub Releases."""
    try:
        info = await updater_service.check_for_updates(force=force)
        return info
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update/trigger")
async def trigger_update():
    """Avvia l'aggiornamento automatico del container Docker tramite Docker Socket o Watchtower."""
    try:
        res = await updater_service.trigger_update()
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
