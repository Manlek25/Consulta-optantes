import re
import time
import requests
import os
import sqlite3
import threading
from datetime import datetime
from typing import Any, Dict, Optional

# =========================
# CNPJá - API pública (sem scraping)
# Docs: https://cnpja.com/api/open
# Endpoint: GET https://open.cnpja.com/office/:cnpj
# Limite (API pública): 5 consultas/minuto por IP.
# =========================

OPEN_CNPJA_BASE = "https://open.cnpja.com/office"

DEFAULT_TIMEOUT = 25

# =========================
# Cache local (SQLite)
# =========================
# Objetivo: acelerar reprocessamentos e permitir "resume" sem perder o q...
#
# Por padrão, o cache salva em: ./data/cnpja_cache.sqlite3
# TTL padrão: 24h

DEFAULT_CACHE_PATH = os.getenv("CNPJA_CACHE_PATH", os.path.join("data", "cnpja_cache.sqlite3"))
DEFAULT_CACHE_TTL_SECONDS = int(os.getenv("CNPJA_CACHE_TTL_SECONDS", str(24 * 60 * 60)))

_cache_lock = threading.Lock()


def _ensure_cache_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _db_connect(path: str) -> sqlite3.Connection:
    _ensure_cache_dir(path)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cnpja_cache (
            cnpj TEXT PRIMARY KEY,
            razao_social TEXT,
            simples_nacional TEXT,
            simei TEXT,
            data_consulta TEXT,
            fetched_at INTEGER
        )
        """
    )
    conn.commit()
    return conn


def _cache_get(cnpj: str, *, cache_path: str, ttl_seconds: int) -> Optional[Dict[str, Any]]:
    now_ts = int(time.time())
    with _cache_lock:
        conn = _db_connect(cache_path)
        try:
            row = conn.execute(
                "SELECT cnpj, razao_social, simples_nacional, simei, data_consulta, fetched_at FROM cnpja_cache WHERE cnpj = ?",
                (cnpj,),
            ).fetchone()
        finally:
            conn.close()

    if not row:
        return None

    fetched_at = int(row[5] or 0)
    if ttl_seconds > 0 and fetched_at > 0 and (now_ts - fetched_at) > ttl_seconds:
        return None

    return {
        "cnpj": row[0],
        "razao_social": row[1] or "",
        "simples_nacional": row[2] or "",
        "simei": row[3] or "",
        "data_consulta": row[4] or "",
        "erro": "",
        "_cached": True,
    }


def _cache_set(payload: Dict[str, Any], *, cache_path: str) -> None:
    cnpj = str(payload.get("cnpj") or "")
    if not re.fullmatch(r"\d{14}", cnpj):
        return

    with _cache_lock:
        conn = _db_connect(cache_path)
        try:
            conn.execute(
                """
                INSERT INTO cnpja_cache (cnpj, razao_social, simples_nacional, simei, data_consulta, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(cnpj) DO UPDATE SET
                    razao_social=excluded.razao_social,
                    simples_nacional=excluded.simples_nacional,
                    simei=excluded.simei,
                    data_consulta=excluded.data_consulta,
                    fetched_at=excluded.fetched_at
                """,
                (
                    cnpj,
                    str(payload.get("razao_social") or ""),
                    str(payload.get("simples_nacional") or ""),
                    str(payload.get("simei") or ""),
                    str(payload.get("data_consulta") or ""),
                    int(time.time()),
                ),
            )
            conn.commit()
        finally:
            conn.close()


def _clean_cnpj(cnpj: str) -> str:
    return re.sub(r"\D+", "", str(cnpj or ""))


def _as_sim_nao(value: Any) -> str:
    """Converte vários formatos possíveis para 'Sim'/'Não'/''."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "Sim" if value else "Não"
    if isinstance(value, (int, float)) and value in (0, 1):
        return "Sim" if int(value) == 1 else "Não"
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"sim", "s", "yes", "true", "1", "optante"}:
            return "Sim"
        if s in {"nao", "não", "n", "no", "false", "0", "nao optante", "não optante"}:
            return "Não"
        # alguns retornos podem trazer status textual
        if "optante" in s and "nao" not in s and "não" not in s:
            return "Sim"
        if "não" in s or "nao" in s:
            return "Não"
    return ""


def _extract_optant_flag(obj: Any) -> str:
    """Extrai indicador de opção em estruturas variadas da API."""
    if obj is None:
        return ""
    if isinstance(obj, (bool, int, float, str)):
        return _as_sim_nao(obj)
    if isinstance(obj, dict):
        # chaves mais comuns
        for k in [
            "optant",
            "opted",
            "is_optant",
            "isOptant",
            "is_opted",
            "option",
            "enabled",
            "mei",  # às vezes SIMEI vem assim
            "active",
            "status",
        ]:
            if k in obj:
                v = obj.get(k)
                # status pode ser "OPTANT"/"NON_OPTANT"
                if k == "status" and isinstance(v, str):
                    sv = v.strip().lower()
                    if "opt" in sv and "non" not in sv and "nao" not in sv and "não" not in sv:
                        return "Sim"
                    if "non" in sv or "nao" in sv or "não" in sv:
                        return "Não"
                out = _as_sim_nao(v)
                if out:
                    return out

        # fallback: procura qualquer boolean no dict
        for v in obj.values():
            out = _as_sim_nao(v)
            if out:
                return out
    return ""


def _pick_razao_social(data: Dict[str, Any]) -> str:
    company = (data or {}).get("company") or {}
    # tenta várias chaves possíveis
    for k in [
        "name",
        "legal_name",
        "corporate_name",
        "razao_social",
        "company_name",
        "social_reason",
    ]:
        v = company.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # fallback: alguns schemas podem ter 'alias' ou 'trade_name'
    for k in ["alias", "trade_name", "fantasy_name", "nome_fantasia"]:
        v = company.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    return ""


def consultar_optante(
    cnpj: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    use_cache: bool = True,
    cache_path: str = DEFAULT_CACHE_PATH,
    cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
    max_retries: int = 3,
) -> Dict[str, Any]:
    """Consulta CNPJ no CNPJá (API pública) e devolve o formato do app.

    Melhorias:
    - Cache SQLite (acelera reprocessamentos e permite "resume")
    - Retry inteligente (429/5xx/erros de rede) sem flood
    """
    cnpj_clean = _clean_cnpj(cnpj)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not re.fullmatch(r"\d{14}", cnpj_clean or ""):
        return {
            "cnpj": cnpj_clean,
            "razao_social": "",
            "simples_nacional": "",
            "simei": "",
            "data_consulta": now,
            "erro": "CNPJ inválido (precisa ter 14 dígitos)",
            "_cached": False,
        }

    if use_cache:
        cached = _cache_get(cnpj_clean, cache_path=cache_path, ttl_seconds=int(cache_ttl_seconds))
        if cached:
            return cached

    url = f"{OPEN_CNPJA_BASE}/{cnpj_clean}"

    headers = {
        "User-Agent": "consulta-optantes/1.0 (+https://cnpja.com/api/open)",
        "Accept": "application/json",
    }

    last_err = ""
    attempts = max(1, int(max_retries))
    for attempt in range(1, attempts + 1):
        try:
            r = requests.get(url, timeout=timeout, headers=headers)

            # Rate limit
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait_s = 60
                try:
                    if retry_after:
                        wait_s = int(float(retry_after))
                except Exception:
                    wait_s = 60

                last_err = (
                    "Rate limit excedido (API pública: 5 consultas/min). "
                    "Vou aguardar e tentar novamente."
                )

                if attempt < attempts:
                    time.sleep(max(1, min(90, wait_s)))
                    continue

                return {
                    "cnpj": cnpj_clean,
                    "razao_social": "",
                    "simples_nacional": "",
                    "simei": "",
                    "data_consulta": now,
                    "erro": last_err,
                    "_cached": False,
                }

            # Erros HTTP
            if r.status_code >= 500:
                last_err = f"Erro HTTP {r.status_code} (servidor) ao consultar CNPJá"
                if attempt < attempts:
                    time.sleep(min(20, 2**attempt))
                    continue
                return {
                    "cnpj": cnpj_clean,
                    "razao_social": "",
                    "simples_nacional": "",
                    "simei": "",
                    "data_consulta": now,
                    "erro": last_err,
                    "_cached": False,
                }

            if r.status_code >= 400:
                # 4xx normalmente não adianta retry
                return {
                    "cnpj": cnpj_clean,
                    "razao_social": "",
                    "simples_nacional": "",
                    "simei": "",
                    "data_consulta": now,
                    "erro": f"Erro HTTP {r.status_code} ao consultar CNPJá",
                    "_cached": False,
                }

            data = r.json() or {}
            company = data.get("company") or {}

            simples_obj = company.get("simples") or company.get("simples_nacional") or data.get("simples")
            simei_obj = company.get("simei") or company.get("mei") or data.get("simei") or data.get("mei")

            payload = {
                "cnpj": cnpj_clean,
                "razao_social": _pick_razao_social(data),
                "simples_nacional": _extract_optant_flag(simples_obj),
                "simei": _extract_optant_flag(simei_obj),
                "data_consulta": now,
                "erro": "",
                "_cached": False,
            }

            if use_cache:
                _cache_set(payload, cache_path=cache_path)

            return payload

        except requests.RequestException as e:
            last_err = f"Erro de rede: {type(e).__name__}: {e}"
            if attempt < attempts:
                time.sleep(min(15, 2**attempt))
                continue
            return {
                "cnpj": cnpj_clean,
                "razao_social": "",
                "simples_nacional": "",
                "simei": "",
                "data_consulta": now,
                "erro": last_err,
                "_cached": False,
            }

    # fallback (não deve chegar aqui)
    return {
        "cnpj": cnpj_clean,
        "razao_social": "",
        "simples_nacional": "",
        "simei": "",
        "data_consulta": now,
        "erro": last_err or "Falha desconhecida",
        "_cached": False,
    }


def selenium_close() -> None:
    """Compat: antes existia fallback Selenium. Agora não usamos scraping."""
    return
