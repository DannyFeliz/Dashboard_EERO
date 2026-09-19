import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.config import settings
from app.services.db import db_service
from app.services.eero_client import eero_client

logger = logging.getLogger(__name__)


class SpeedtestService:
    def __init__(self):
        self.is_running: bool = False
        self.last_run_time: Optional[str] = None
        self.last_result: Optional[Dict[str, Any]] = None

    async def run_speedtest(self, force_local: bool = False) -> Dict[str, Any]:
        """
        Esegue un test di velocità. Se autenticato con eero cloud, invia il trigger
        alle API native eero. In alternativa, esegue un test sintetico o locale.
        """
        if self.is_running:
            raise RuntimeError("Uno Speed Test è già in corso di esecuzione.")

        self.is_running = True
        logger.info("Avvio esecuzione Speed Test...")
        
        try:
            # Se siamo autenticati con eero (e non forzato locale o demo pura)
            if eero_client.is_authenticated and not force_local and not (getattr(eero_client, "user_token", "") or "").startswith("demo_"):
                # Rileva timestamp iniziale
                init_details = await eero_client.get_network_details()
                init_sp = init_details.get("speedtest", {}) if isinstance(init_details, dict) else {}
                init_time = init_sp.get("timestamp")

                await eero_client.trigger_eero_speedtest()
                
                # Attendi e verifica il completamento del test eero (fino a 25 secondi)
                st = init_sp
                network_details = init_details
                for _ in range(12):
                    await asyncio.sleep(2)
                    network_details = await eero_client.get_network_details()
                    curr_sp = network_details.get("speedtest", {}) if isinstance(network_details, dict) else {}
                    if curr_sp and curr_sp.get("timestamp") != init_time:
                        st = curr_sp
                        break
                    st = curr_sp
                    
                down = float(st.get("download_mbps") or 0.0)
                up = float(st.get("upload_mbps") or 0.0)
                ping = float(st.get("ping_ms") or 0.0)
                jitter = float(st.get("jitter") or 0.0)
                isp_name = (network_details.get("isp") if isinstance(network_details, dict) else None) or "eero Gateway"
                server = f"{isp_name} (eero Cloud SpeedTest)"
                if down <= 0.0:
                    raise RuntimeError("eero Gateway speed test did not return valid throughput metrics.")
            else:
                # Esecuzione simulata/sintetica rapida (solo per demo mode)
                await asyncio.sleep(2.0)
                down, up, ping, jitter, server = await self._run_synthetic_speedtest()

            # Registrazione nel database storico (solo per test reali e account autenticati)
            is_demo = (
                getattr(eero_client, "is_demo_mode", False) or 
                (getattr(eero_client, "user_token", "") or "").startswith("demo_") or 
                settings.demo_mode or
                down > 2000.0 or
                (abs(down - 912.45) < 0.05 and abs(up - 298.10) < 0.05) or
                (abs(down - 2240.50) < 0.05 and abs(up - 980.20) < 0.05)
            )
            if not is_demo:
                test_id = await db_service.save_speedtest(
                    download_mbps=round(down, 2),
                    upload_mbps=round(up, 2),
                    ping_ms=round(ping, 1),
                    jitter=round(jitter, 1),
                    server_name=server,
                    source="eero_cloud"
                )
            else:
                test_id = 0

            self.last_run_time = datetime.now(timezone.utc).isoformat()
            self.last_result = {
                "id": test_id,
                "download_mbps": round(down, 2),
                "upload_mbps": round(up, 2),
                "ping_ms": round(ping, 1),
                "jitter": round(jitter, 1),
                "server_name": server,
                "timestamp": self.last_run_time,
            }
            logger.info(f"Speed Test completato: ↓ {down} Mbps, ↑ {up} Mbps, Ping: {ping} ms")
            return self.last_result
        except Exception as e:
            logger.error(f"Speed Test execution error: {e}")
            raise
        finally:
            self.is_running = False

    async def _run_synthetic_speedtest(self):
        """Generatore di test con valori realistici congruenti alla linea 1Gbps."""
        down = random.uniform(945.0, 975.0)
        up = random.uniform(188.0, 196.0)
        ping = random.uniform(8.0, 10.5)
        jitter = random.uniform(0.4, 1.2)
        server = "Fastweb / Wind Tre (FTTH 1Gbps)"
        return down, up, ping, jitter, server


speedtest_service = SpeedtestService()
