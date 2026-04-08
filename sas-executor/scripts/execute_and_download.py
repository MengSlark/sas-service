import argparse
import json
import os
from pathlib import Path
from typing import Any

import requests


def _extract_error_info(resp: requests.Response) -> tuple[str, str, str]:
    request_id = ""
    log_text = ""
    try:
        body = resp.json()
        detail = body.get("detail", "")
        if isinstance(detail, dict):
            request_id = str(detail.get("request_id", "")).strip()
            log_text = str(detail.get("log", ""))
            message = str(detail.get("message", ""))
            fallback = json.dumps(detail, ensure_ascii=False)
            return message or fallback, request_id, log_text
        return str(detail), request_id, log_text
    except ValueError:
        return resp.text, request_id, log_text


def _ensure_ok(resp: requests.Response, action: str) -> None:
    if resp.status_code < 400:
        return
    detail, _, _ = _extract_error_info(resp)
    raise RuntimeError(f"{action} failed: HTTP {resp.status_code}. {detail}")


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    i = 1
    while True:
        candidate = path.with_name(f"{stem}_{i}{suffix}")
        if not candidate.exists():
            return candidate
        i += 1


def _save_log_file(cwd: Path, request_id: str, log_text: str) -> Path:
    rid = request_id or "unknown"
    target = _unique_path(cwd / f"sas_log_{rid}.log")
    target.write_text(log_text, encoding="utf-8", errors="replace")
    return target


def run(
    base_url: str,
    code_file: Path,
    output_dir: str,
    input_paths: list[str],
    timeout: int,
) -> dict[str, Any]:
    cwd = Path.cwd()
    health = requests.get(f"{base_url}/health", timeout=100)
    _ensure_ok(health, "health check")

    if not code_file.exists():
        raise FileNotFoundError(f"code file not found: {code_file}")
    code = code_file.read_text(encoding="utf-8", errors="replace")

    payload = {
        "code": code,
        "output_dir": output_dir,
        "input_paths": input_paths,
    }
    resp = requests.post(f"{base_url}/execute", json=payload, timeout=timeout)
    if resp.status_code >= 400:
        detail, request_id, log_text = _extract_error_info(resp)
        if not log_text:
            log_text = detail or resp.text or f"execute failed: HTTP {resp.status_code}"
        log_file = _save_log_file(cwd, request_id or "failed", log_text)
        raise RuntimeError(
            f"execute failed: HTTP {resp.status_code}. {detail} (log_file: {log_file})"
        )
    data = resp.json()
    request_id = str(data.get("request_id") or "")
    log_text = str(data.get("log", ""))
    artifacts = list(data.get("artifacts", []))

    saved_files: list[str] = []
    log_file_path = _save_log_file(cwd, request_id or "unknown", log_text)
    log_file = str(log_file_path)
    saved_files.append(log_file)
    local_log_name = log_file_path.name.lower()

    for item in artifacts:
        filename = Path(str(item.get("filename", ""))).name
        download_url = str(item.get("download_url", ""))
        if not filename or not download_url:
            continue
        if filename.lower() == local_log_name:
            # Log already persisted locally from response payload.
            continue
        dl = requests.get(f"{base_url}{download_url}", timeout=timeout)
        _ensure_ok(dl, f"download {filename}")
        target = _unique_path(cwd / filename)
        target.write_bytes(dl.content)
        saved_files.append(str(target))

    return {
        "success": bool(data.get("success")),
        "request_id": request_id,
        "log_file": log_file,
        "saved_files": saved_files,
        "artifacts": artifacts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute SAS code via service and download artifacts.")
    parser.add_argument("--code-file", required=True, help="Path to .sas file")
    parser.add_argument("--output-dir", required=True, help="Remote output dir for the SAS service")
    parser.add_argument("--base-url", default=os.getenv("SAS_SERVICE_URL", "http://115.190.133.229:8080"))
    parser.add_argument("--input-path", action="append", default=[], help="Repeatable input path")
    parser.add_argument("--timeout", type=int, default=600, help="HTTP timeout in seconds")
    args = parser.parse_args()

    result = run(
        base_url=args.base_url.rstrip("/"),
        code_file=Path(args.code_file),
        output_dir=args.output_dir,
        input_paths=args.input_path,
        timeout=args.timeout,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
