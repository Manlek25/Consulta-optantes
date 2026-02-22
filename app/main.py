import sys
import asyncio

import threading
LOCK = threading.Lock()

# Necessário no Windows para libs que usam subprocess/asyncio em alguns cenários
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import uuid
import json
import traceback
from datetime import datetime
import pandas as pd

from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request
from sse_starlette.sse import EventSourceResponse
from starlette.templating import Jinja2Templates

from app.services.io_files import read_input_file_to_df, build_output_bytes
from app.services.consulta_optantes import consultar_optante_lote


app = FastAPI()

app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Jobs em memória
JOBS = {}  # job_id -> dict(status, progress, total, done, file_bytes, file_name, error, cancel_event)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/lotes")
async def criar_lote(
    file: UploadFile = File(...),
    output: str = Query(default="xlsx"),
    sleep_seconds: float = Query(default=12.5),
):
    output = (output or "").lower().strip()
    if output not in {"csv", "xlsx"}:
        raise HTTPException(400, "output deve ser csv ou xlsx")

    # API pública do CNPJá: 5 consultas/minuto por IP => ~12s por consulta
    try:
        sleep_seconds = float(sleep_seconds)
    except Exception:
        sleep_seconds = 12.5
    sleep_seconds = max(12.5, sleep_seconds)

    df = read_input_file_to_df(file)

    # ✅ garante coluna cnpj e evita KeyError
    if "cnpj" not in df.columns:
        raise HTTPException(
            status_code=500,
            detail=f"DataFrame sem coluna 'cnpj'. Colunas={list(df.columns)}",
        )

    # ✅ pode vir vazio (0 válidos) e ainda assim a gente gera arquivo com inválidos
    cnpjs = df["cnpj"].dropna().astype(str).tolist()

    invalidos = df.attrs.get("invalidos")
    total_invalidos = int(len(invalidos)) if invalidos is not None else 0

    # se não tem válidos NEM inválidos, aí sim é arquivo ruim
    if (not cnpjs) and (total_invalidos == 0):
        raise HTTPException(400, "Nenhum CNPJ encontrado no arquivo (válido ou inválido).")

    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "status": "queued",
        "progress": 0,
        "total": len(cnpjs) + total_invalidos,  # ✅ total real (válidos + inválidos)
        "done": False,
        "file_bytes": None,
        "file_name": f"resultado.{output}",
        "error": None,
        "cancel_event": threading.Event(),
    }

    asyncio.create_task(processar_job(job_id, df, output, sleep_seconds))
    return JSONResponse({"job_id": job_id})


async def processar_job(job_id: str, df_validos, output: str, sleep_seconds: float):
    try:
        with LOCK:
            JOBS[job_id]["status"] = "running"

        def progress_cb(done: int, total: int):
            with LOCK:
                # total pode ser reafirmado (válidos + inválidos)
                JOBS[job_id]["total"] = total
                JOBS[job_id]["progress"] = done

        def _run():
            return consultar_optante_lote(
                df_validos,
                sleep_seconds=sleep_seconds,
                progress_cb=progress_cb,
                should_cancel=lambda: bool(JOBS.get(job_id, {}).get("cancel_event") and JOBS[job_id]["cancel_event"].is_set()),
            )

        df_out = await asyncio.to_thread(_run)

        file_bytes = build_output_bytes(df_out, output)
        with LOCK:
            JOBS[job_id]["file_bytes"] = file_bytes
            JOBS[job_id]["done"] = True

            # Se cancelou, marcamos status "canceled", mas ainda liberamos download do parcial
            if JOBS[job_id]["cancel_event"].is_set():
                JOBS[job_id]["status"] = "canceled"
            else:
                JOBS[job_id]["status"] = "done"
                JOBS[job_id]["progress"] = JOBS[job_id]["total"]

    except Exception as e:
        traceback.print_exc()
        with LOCK:
            JOBS[job_id]["status"] = "error"
            JOBS[job_id]["error"] = f"{type(e).__name__}: {e}"
            JOBS[job_id]["done"] = True

@app.get("/status/{job_id}")
async def status(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job não encontrado")
    return JSONResponse(
        {
            "status": job["status"],
            "progress": job["progress"],
            "total": job["total"],
            "done": job["done"],
            "error": job["error"],
            "has_file": job["file_bytes"] is not None,
            "canceled": bool(job.get("cancel_event") and job["cancel_event"].is_set()),
        }
    )


@app.post("/cancel/{job_id}")
async def cancelar(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job não encontrado")

    # se já terminou, não tem o que cancelar
    if job.get("done"):
        return JSONResponse({"ok": True, "status": job.get("status")})

    ev = job.get("cancel_event")
    if ev:
        ev.set()
    with LOCK:
        # não marca done aqui: o worker vai finalizar e gerar o arquivo parcial
        if job.get("status") in {"queued", "running"}:
            job["status"] = "canceling"
    return JSONResponse({"ok": True, "status": "canceling"})


@app.get("/progresso/{job_id}")
async def progresso(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, "job não encontrado")

    async def event_generator():
        last = -1
        last_ping = asyncio.get_event_loop().time()

        # Sinaliza ao client para tentar reconectar rapidamente caso a conexão caia
        yield {"event": "open", "data": "ok", "retry": 5000}
        while True:
            job = JOBS.get(job_id)
            if not job:
                break

            if job["status"] == "error":
                yield {"event": "error", "data": job["error"] or "Erro desconhecido"}
                break

            if job["progress"] != last:
                last = job["progress"]
                payload = {
                    "status": job["status"],
                    "progress": job["progress"],
                    "total": job["total"],
                    "done": job["done"],
                }
                yield {"event": "progress", "data": json.dumps(payload, ensure_ascii=False)}

            # Keep-alive (alguns proxies derrubam SSE em streams "silenciosos")
            now = asyncio.get_event_loop().time()
            if now - last_ping >= 15:
                last_ping = now
                yield {"event": "ping", "data": "keepalive"}

            if job["done"]:
                yield {"event": "done", "data": "ok"}
                break

            await asyncio.sleep(0.3)

    return EventSourceResponse(event_generator())


@app.get("/download/{job_id}")
async def download(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "job não encontrado")
    if not job["done"] or not job["file_bytes"]:
        raise HTTPException(409, "Arquivo ainda não está pronto")

    filename = job["file_name"]
    media_type = (
        "text/csv"
        if filename.endswith(".csv")
        else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    return StreamingResponse(
        iter([job["file_bytes"]]),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )