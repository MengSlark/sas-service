import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from sas_service import execute_sas_job, get_artifact_path, safe_filename
from schemas import ExecuteRequest, ExecuteResponse

app = FastAPI(title="SAS Execution Service", version="2.1.0")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/execute", response_model=ExecuteResponse)
def execute(payload: ExecuteRequest) -> ExecuteResponse:
    request_id = uuid.uuid4().hex[:12]
    try:
        return execute_sas_job(request_id, payload)
    except Exception as ex:
        log_path = Path(__file__).resolve().parent / "runtime" / request_id / "execute.log"
        log_text = ""
        if log_path.exists():
            log_text = log_path.read_text(encoding="utf-8", errors="replace")
        raise HTTPException(
            status_code=400,
            detail={
                "message": str(ex),
                "request_id": request_id,
                "log": log_text,
            },
        )


@app.get("/artifacts/{request_id}/{filename:path}")
def download_artifact(request_id: str, filename: str):
    try:
        path = get_artifact_path(request_id, filename)
    except ValueError:
        raise HTTPException(status_code=404, detail="artifact not found")
    safe_name = safe_filename(path.name)
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="artifact not found")
    return FileResponse(path=str(path), filename=safe_name)
