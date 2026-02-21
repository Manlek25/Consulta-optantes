import time
import pandas as pd
from datetime import datetime
from typing import Callable, Optional

from app.services.consulta_site import consultar_optante, selenium_close


# API pública do CNPJá: 5 consultas/minuto por IP => ~12s por consulta.
PUBLIC_API_MIN_DELAY = 12.5


def consultar_optante_lote(
    df_validos: pd.DataFrame,
    sleep_seconds: float = PUBLIC_API_MIN_DELAY,
    progress_cb: Optional[Callable[[int, int], None]] = None,
    should_cancel: Optional[Callable[[], bool]] = None,
) -> pd.DataFrame:
    resultados = []

    cnpjs = []
    if df_validos is not None and "cnpj" in df_validos.columns:
        cnpjs = df_validos["cnpj"].dropna().astype(str).tolist()

    invalidos = df_validos.attrs.get("invalidos") if df_validos is not None else None
    total_invalidos = int(len(invalidos)) if invalidos is not None else 0

    total = len(cnpjs) + total_invalidos
    done = 0

    # garante rate limit (evita 429)
    try:
        sleep_seconds = float(sleep_seconds)
    except Exception:
        sleep_seconds = PUBLIC_API_MIN_DELAY
    sleep_seconds = max(PUBLIC_API_MIN_DELAY, sleep_seconds)

    def _tick():
        if progress_cb:
            try:
                progress_cb(done, total)
            except Exception:
                pass

    _tick()

    try:
        for cnpj in cnpjs:
            if should_cancel and should_cancel():
                break

            r = consultar_optante(cnpj)
            # Se veio do cache, não precisamos esperar (economiza MUITO em reprocessamentos)
            cached = bool(r.pop("_cached", False))
            resultados.append(r)

            done += 1
            _tick()
            if not cached:
                time.sleep(max(0.0, float(sleep_seconds)))

        # Se cancelou, encerra sem adicionar inválidos (para parar o lote de verdade)
        if should_cancel and should_cancel():
            pass
        elif invalidos is not None and len(invalidos) > 0:
            for _, row in invalidos.iterrows():
                resultados.append({
                    "cnpj": str(row.get("cnpj", "")) or str(row.get("cnpj_input", "")),
                    "razao_social": "",
                    "simples_nacional": "",
                    "simei": "",
                    "data_consulta": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "erro": "CNPJ inválido (precisa ter 14 dígitos)",
                })
                done += 1
                _tick()

    finally:
        # compat - hoje é no-op
        selenium_close()

    if not resultados:
        return pd.DataFrame(
            columns=["cnpj", "razao_social", "simples_nacional", "simei", "data_consulta", "erro"]
        )

    return pd.DataFrame(resultados)
