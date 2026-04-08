import os
import re
import ctypes
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import saspy
from dotenv import load_dotenv

from schemas import ArtifactItem, ExecuteRequest, ExecuteResponse

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = ROOT_DIR / "runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR = RUNTIME_DIR / "tmp"
TMP_DIR.mkdir(parents=True, exist_ok=True)

SAS_CFGNAME = os.getenv("SAS_CFGNAME", "").strip()
SAS_USER = os.getenv("SAS_USER", "").strip()
SAS_PW = os.getenv("SAS_PW", "").strip()


def safe_filename(name: str) -> str:
    return Path(name).name


def get_artifact_path(request_id: str, filename: str) -> Path:
    normalized = _normalize_artifact_relpath(filename)
    base_dir = (RUNTIME_DIR / request_id / "output").resolve()
    target = (base_dir / Path(normalized.replace("\\", "/"))).resolve()
    if not (target == base_dir or base_dir in target.parents):
        raise ValueError("invalid artifact path")
    return target


def execute_sas_job(request_id: str, payload: ExecuteRequest) -> ExecuteResponse:
    request_dir = RUNTIME_DIR / request_id
    request_dir.mkdir(parents=True, exist_ok=True)

    output_dir = _normalize_output_dir(payload.output_dir)
    sas = None
    com_initialized = False
    temp_env_backup: dict[str, str | None] = {}
    log_text = ""
    log_artifact_path: Path | None = None
    try:
        # å°† saspy ä¸´æ—¶æ–‡ä»¶å›ºå®šåˆ°é¡¹ç›® runtime ç›®å½•ï¼Œé¿å…ç³»ç»Ÿä¸´æ—¶ç›®å½•æƒé™/ç­–ç•¥é—®é¢˜ã€‚
        temp_env_backup = _set_temp_dir_for_saspy()
        # Windows/IOM åœºæ™¯ä¸‹ï¼Œå½“å‰çº¿ç¨‹éœ€å…ˆå®Œæˆ COM åˆå§‹åŒ–å†åˆ›å»º SASsessionã€‚
        com_initialized = _co_initialize_for_windows()
        sas = saspy.SASsession(**_sas_config())
        _ensure_remote_output_dir(sas, output_dir)
        _ensure_remote_output_layout(sas, output_dir)

        submit_result = sas.submit(payload.code)
        log_text = submit_result.get("LOG", "")
        log_artifact_path = _write_log(log_text, request_dir, request_id)

        _, exists_after, after_err = _snapshot_remote_dir(sas, output_dir)
        if not exists_after:
            detail = f"output_dir became unavailable after execution: {output_dir}"
            if after_err:
                detail = f"{detail}; SAS detail: {after_err}"
            raise RuntimeError(detail)

        all_files = _collect_output_files(sas, output_dir)
        artifacts = _download_artifacts(sas, request_id, output_dir, all_files)
        if log_artifact_path is not None:
            artifacts.append(_build_log_artifact(request_id, log_artifact_path))
        return ExecuteResponse(
            success=True,
            request_id=request_id,
            log=log_text,
            artifacts=artifacts,
        )
    except Exception:
        _write_log(log_text, request_dir, request_id)
        raise
    finally:
        if sas is not None:
            try:
                sas.endsas()
            except Exception:
                pass
        _co_uninitialize_for_windows(com_initialized)
        _restore_temp_dir(temp_env_backup)


def _co_initialize_for_windows() -> bool:
    if os.name != "nt":
        return False
    # CoInitialize è¿”å›ž 0/1 è¡¨ç¤ºå½“å‰çº¿ç¨‹ COM å¯ç”¨ï¼ˆS_OK/S_FALSEï¼‰ã€‚
    hr = ctypes.windll.ole32.CoInitialize(None)
    if hr not in (0, 1):
        raise RuntimeError(f"CoInitialize failed, HRESULT={hr}")
    return True


def _co_uninitialize_for_windows(initialized: bool) -> None:
    if os.name == "nt" and initialized:
        ctypes.windll.ole32.CoUninitialize()


def _set_temp_dir_for_saspy() -> dict[str, str | None]:
    # å…ˆå¤‡ä»½å†è¦†ç›–ä¸´æ—¶ç›®å½•çŽ¯å¢ƒå˜é‡ï¼Œç¡®ä¿ saspy åœ¨å¯æŽ§ç›®å½•åˆ›å»ºä¸´æ—¶æ–‡ä»¶ã€‚
    backup = {
        "TMP": os.environ.get("TMP"),
        "TEMP": os.environ.get("TEMP"),
        "TMPDIR": os.environ.get("TMPDIR"),
    }
    temp_path = str(TMP_DIR)
    os.environ["TMP"] = temp_path
    os.environ["TEMP"] = temp_path
    os.environ["TMPDIR"] = temp_path
    return backup


def _restore_temp_dir(backup: dict[str, str | None]) -> None:
    for key, val in backup.items():
        if val is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = val


def _sas_config() -> dict[str, str]:
    # ä»…ä½¿ç”¨é¡¹ç›®å†… sascfg.pyï¼Œä¸å†å¤åˆ¶æˆ–åŠ¨æ€æŒ‡å®š cfgfileã€‚
    sascfg = ROOT_DIR / "sascfg.py"
    if not sascfg.exists():
        raise RuntimeError(f"sascfg.py not found: {sascfg}")

    missing = []
    if not SAS_USER:
        missing.append("SAS_USER")
    if not SAS_PW:
        missing.append("SAS_PW")
    if missing:
        raise RuntimeError(f"Missing SAS config in .env: {', '.join(missing)}")

    config: dict[str, str] = {"user": SAS_USER, "pw": SAS_PW}
    if SAS_CFGNAME:
        config["cfgname"] = SAS_CFGNAME
    return config


def _sas_quote(value: str) -> str:
    return value.replace("'", "''")


def _normalize_output_dir(path: str) -> str:
    normalized = path.strip().replace("/", "\\")
    if not normalized:
        raise ValueError("output_dir cannot be empty")
    return normalized.rstrip("\\")


def _join_remote_path(output_dir: str, filename: str) -> str:
    return f"{output_dir}\\{filename}"


def _normalize_artifact_relpath(path: str) -> str:
    normalized = str(path or "").strip().replace("/", "\\").strip("\\")
    if not normalized:
        raise ValueError("artifact path cannot be empty")
    if re.match(r"^[A-Za-z]:", normalized) or normalized.startswith("\\\\"):
        raise ValueError("artifact path must be relative")

    parts = [part for part in normalized.split("\\") if part and part != "."]
    if not parts or any(part == ".." for part in parts):
        raise ValueError("invalid artifact path")
    return "\\".join(parts)


def _check_remote_dir_exists(sas: saspy.SASsession, path: str) -> tuple[bool, str]:
    quoted = _sas_quote(path)
    code = f"""
%global _codx_dir_exists;
%global _codx_dir_rc;
%global _codx_dir_msg;
%let _codx_dir_exists=0;
%let _codx_dir_rc=;
%let _codx_dir_msg=;
data _null_;
    length msg $512;
    rc = filename('_cdxchk', '{quoted}');
    did = dopen('_cdxchk');
    if did > 0 then do;
        call symputx('_codx_dir_exists', '1', 'G');
        rc = dclose(did);
    end;
    else do;
        call symputx('_codx_dir_exists', '0', 'G');
        call symputx('_codx_dir_rc', strip(put(did, best32.)), 'G');
        msg = sysmsg();
        call symputx('_codx_dir_msg', msg, 'G');
    end;
    rc = filename('_cdxchk');
run;
%put CODX_DIR_EXISTS=&_codx_dir_exists;
%put CODX_DIR_RC=&_codx_dir_rc;
%put CODX_DIR_MSG=&_codx_dir_msg;
"""
    submit_result = sas.submit(code)
    log_text = submit_result.get("LOG", "")
    return _parse_dir_exists(log_text), _parse_dir_error(log_text)


def _create_remote_dir(sas: saspy.SASsession, parent: str, leaf: str) -> str:
    quoted_parent = _sas_quote(parent)
    quoted_leaf = _sas_quote(leaf)
    code = f"""
%global _codx_mkdir_ok;
%global _codx_mkdir_msg;
%let _codx_mkdir_ok=0;
%let _codx_mkdir_msg=;
data _null_;
    length created $512 msg $512 parent $512;
    rc = filename('cdxpar', '{quoted_parent}');
    did = dopen('cdxpar');
    if did <= 0 then do;
        msg = cats('parent_not_accessible: ', sysmsg());
        call symputx('_codx_mkdir_msg', msg, 'G');
        rc = filename('cdxpar');
        stop;
    end;
    rc = dclose(did);
    parent = pathname('cdxpar');
    created = dcreate('{quoted_leaf}', parent);
    if missing(created) then do;
        msg = cats('dcreate_failed: ', sysmsg());
        call symputx('_codx_mkdir_msg', msg, 'G');
        rc = filename('cdxpar');
        stop;
    end;
    call symputx('_codx_mkdir_ok', '1', 'G');
    rc = filename('cdxpar');
run;
%put CODX_MKDIR_OK=&_codx_mkdir_ok;
%put CODX_MKDIR_MSG=&_codx_mkdir_msg;
"""
    submit_result = sas.submit(code)
    log_text = submit_result.get("LOG", "")
    ok = bool(re.search(r"CODX_MKDIR_OK=1", log_text))
    if ok:
        return ""
    msg_match = re.findall(r"CODX_MKDIR_MSG=([^\r\n]*)", log_text)
    for item in msg_match:
        value = item.strip()
        if "symget(" not in value:
            return value
    return "unknown SAS mkdir error"


def _ensure_remote_output_dir(sas: saspy.SASsession, output_dir: str) -> None:
    normalized = output_dir.strip().replace("/", "\\").rstrip("\\")
    if not normalized:
        raise ValueError("output_dir cannot be empty")

    exists, _ = _check_remote_dir_exists(sas, normalized)
    if exists:
        return

    # Create each missing level from drive root, e.g. D:\a\b\c
    segments = [seg for seg in normalized.split("\\") if seg]
    if not segments:
        raise RuntimeError(f"Invalid output_dir: {output_dir}")

    drive_pattern = re.compile(r"^[A-Za-z]:$")
    if drive_pattern.match(segments[0]):
        current = segments[0] + "\\"
        index = 1
    else:
        # Fallback: only attempt to create the final leaf under existing parent.
        parent = str(Path(normalized).parent).replace("/", "\\")
        leaf = Path(normalized).name
        if not leaf or not parent:
            raise RuntimeError(f"Unsupported output_dir format: {output_dir}")
        msg = _create_remote_dir(sas, parent, leaf)
        if msg:
            raise RuntimeError(f"Failed to create output_dir {normalized}: {msg}")
        exists_after, err_after = _check_remote_dir_exists(sas, normalized)
        if not exists_after:
            detail = f"Failed to create output_dir {normalized}"
            if err_after:
                detail = f"{detail}; SAS detail: {err_after}"
            raise RuntimeError(detail)
        return

    for leaf in segments[index:]:
        parent = current.rstrip("\\")
        candidate = f"{parent}\\{leaf}" if parent else leaf
        exists_candidate, _ = _check_remote_dir_exists(sas, candidate)
        if not exists_candidate:
            msg = _create_remote_dir(sas, parent if parent else current, leaf)
            if msg:
                raise RuntimeError(f"Failed to create output_dir segment {candidate}: {msg}")
            exists_after, err_after = _check_remote_dir_exists(sas, candidate)
            if not exists_after:
                detail = f"Failed to create output_dir segment {candidate}"
                if err_after:
                    detail = f"{detail}; SAS detail: {err_after}"
                raise RuntimeError(detail)
        current = candidate


def _ensure_remote_output_layout(sas: saspy.SASsession, output_dir: str) -> None:
    required_subdirs = [
        "data",
        "data\\adam",
        "data\\tlf",
        "program",
        "report",
    ]
    for subdir in required_subdirs:
        target = _join_remote_path(output_dir, subdir)
        _ensure_remote_output_dir(sas, target)


def _parse_dir_exists(log_text: str) -> bool:
    match = re.search(r"CODX_DIR_EXISTS=(\d)", log_text)
    return bool(match and match.group(1) == "1")


def _parse_dir_error(log_text: str) -> str:
    rc_candidates = re.findall(r"CODX_DIR_RC=([^\r\n]*)", log_text)
    msg_candidates = re.findall(r"CODX_DIR_MSG=([^\r\n]*)", log_text)
    rc = ""
    msg = ""
    for item in rc_candidates:
        value = item.strip()
        if "symget(" not in value:
            rc = value
    for item in msg_candidates:
        value = item.strip()
        if "symget(" not in value:
            msg = value
    if rc and msg:
        return f"rc={rc}, msg={msg}"
    if rc:
        return f"rc={rc}"
    if msg:
        return f"msg={msg}"
    return ""


def _snapshot_remote_dir_flat(
    sas: saspy.SASsession, output_dir: str
) -> tuple[list[dict[str, Any]], bool, str]:
    quoted_dir = _sas_quote(output_dir)
    code = f"""
%global _codx_dir_exists;
%global _codx_dir_rc;
%global _codx_dir_msg;
%let _codx_dir_exists=0;
%let _codx_dir_rc=;
%let _codx_dir_msg=;
data work._codx_snapshot;
    length filename $256 modified_time $128 size 8 size_txt $128 msg $512;
    length opt_name $128 opt_val $256;
    rc = filename('_cdxdir', '{quoted_dir}');
    did = dopen('_cdxdir');
    if did <= 0 then do;
        call symputx('_codx_dir_exists', '0', 'G');
        call symputx('_codx_dir_rc', strip(put(did, best32.)), 'G');
        msg = sysmsg();
        call symputx('_codx_dir_msg', msg, 'G');
        stop;
    end;
    call symputx('_codx_dir_exists', '1', 'G');
    do i = 1 to dnum(did);
        filename = dread(did, i);
        fid = mopen(did, filename);
        if fid > 0 then do;
            size_txt = finfo(fid, 'File Size (bytes)');
            if missing(size_txt) then size_txt = finfo(fid, 'File Size');
            modified_time = strip(finfo(fid, 'Last Modified'));
            if missing(modified_time) then modified_time = strip(finfo(fid, 'Last Modified Time'));
            if missing(size_txt) or missing(modified_time) then do _k = 1 to foptnum(fid);
                opt_name = strip(foptname(fid, _k));
                opt_val = strip(finfo(fid, opt_name));
                if missing(size_txt) then do;
                    if index(upcase(opt_name), 'SIZE') > 0 then size_txt = opt_val;
                end;
                if missing(modified_time) then do;
                    if index(upcase(opt_name), 'MOD') > 0 or index(upcase(opt_name), 'TIME') > 0 then modified_time = opt_val;
                end;
            end;
            size = input(compress(size_txt,,'kd'), best32.);
            if missing(size) then size = 0;
            output;
            rc = fclose(fid);
        end;
    end;
    rc = dclose(did);
    rc = filename('_cdxdir');
    keep filename size modified_time;
run;
%put CODX_DIR_EXISTS=&_codx_dir_exists;
%put CODX_DIR_RC=&_codx_dir_rc;
%put CODX_DIR_MSG=&_codx_dir_msg;
"""
    submit_result = sas.submit(code)
    log_text = submit_result.get("LOG", "")
    exists = _parse_dir_exists(log_text)
    detail = _parse_dir_error(log_text)
    if not exists:
        return [], False, detail

    try:
        frame = sas.sasdata2dataframe(table="_codx_snapshot", libref="work")
    except Exception as ex:
        message = str(ex)
        if "BOF or EOF is True" in message or "ADODB.Recordset" in message:
            return [], True, detail
        raise

    snapshot: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        filename = str(row.get("filename", "")).strip()
        if not filename:
            continue
        size_val = row.get("size", 0)
        modified = str(row.get("modified_time", "")).strip()
        try:
            size = int(float(size_val))
        except (TypeError, ValueError):
            size = 0
        snapshot.append(
            {
                "filename": filename,
                "size": size,
                "modified_time": modified,
            }
        )
    return snapshot, True, detail


def _snapshot_remote_dir(sas: saspy.SASsession, output_dir: str) -> tuple[list[dict[str, Any]], bool, str]:
    quoted_dir = _sas_quote(output_dir)
    code = f"""
%global _codx_dir_exists;
%global _codx_dir_rc;
%global _codx_dir_msg;
%let _codx_dir_exists=0;
%let _codx_dir_rc=;
%let _codx_dir_msg=;
data work._codx_snapshot;
    length filename $512 modified_time $128 size 8 size_txt $128 msg $512;
    length opt_name $128 opt_val $256;
    length cur_dir cur_rel rel_name sub_dir $512 entry $256;
    array qdir[10000] $512 _temporary_;
    array qrel[10000] $512 _temporary_;

    rc = filename('_cdxdir', '{quoted_dir}');
    did = dopen('_cdxdir');
    if did <= 0 then do;
        call symputx('_codx_dir_exists', '0', 'G');
        call symputx('_codx_dir_rc', strip(put(did, best32.)), 'G');
        msg = sysmsg();
        call symputx('_codx_dir_msg', msg, 'G');
        stop;
    end;
    call symputx('_codx_dir_exists', '1', 'G');

    root_dir = pathname('_cdxdir');
    rc = dclose(did);
    rc = filename('_cdxdir');

    qdir[1] = root_dir;
    qrel[1] = '';
    q_head = 1;
    q_tail = 1;

    do while (q_head <= q_tail);
        cur_dir = qdir[q_head];
        cur_rel = qrel[q_head];
        q_head + 1;

        rc = filename('_cdxdir', cur_dir);
        did = dopen('_cdxdir');
        if did > 0 then do;
            do i = 1 to dnum(did);
                entry = dread(did, i);
                if missing(cur_rel) then rel_name = entry;
                else rel_name = catx('\\', cur_rel, entry);

                fid = mopen(did, entry);
                if fid > 0 then do;
                    filename = rel_name;
                    size_txt = finfo(fid, 'File Size (bytes)');
                    if missing(size_txt) then size_txt = finfo(fid, 'File Size');
                    modified_time = strip(finfo(fid, 'Last Modified'));
                    if missing(modified_time) then modified_time = strip(finfo(fid, 'Last Modified Time'));

                    if missing(size_txt) or missing(modified_time) then do _k = 1 to foptnum(fid);
                        opt_name = strip(foptname(fid, _k));
                        opt_val = strip(finfo(fid, opt_name));
                        if missing(size_txt) then do;
                            if index(upcase(opt_name), 'SIZE') > 0 then size_txt = opt_val;
                        end;
                        if missing(modified_time) then do;
                            if index(upcase(opt_name), 'MOD') > 0 or index(upcase(opt_name), 'TIME') > 0 then modified_time = opt_val;
                        end;
                    end;

                    size = input(compress(size_txt,,'kd'), best32.);
                    if missing(size) then size = 0;
                    output;
                    rc = fclose(fid);
                end;
                else do;
                    sub_dir = catx('\\', cur_dir, entry);
                    rc2 = filename('_cdxsub', sub_dir);
                    did2 = dopen('_cdxsub');
                    if did2 > 0 then do;
                        if q_tail < dim(qdir) then do;
                            q_tail + 1;
                            qdir[q_tail] = sub_dir;
                            qrel[q_tail] = rel_name;
                        end;
                        rc2 = dclose(did2);
                    end;
                    rc2 = filename('_cdxsub');
                end;
            end;
            rc = dclose(did);
        end;
        rc = filename('_cdxdir');
    end;

    keep filename size modified_time;
run;
%put CODX_DIR_EXISTS=&_codx_dir_exists;
%put CODX_DIR_RC=&_codx_dir_rc;
%put CODX_DIR_MSG=&_codx_dir_msg;
"""
    submit_result = sas.submit(code)
    log_text = submit_result.get("LOG", "")
    exists = _parse_dir_exists(log_text)
    detail = _parse_dir_error(log_text)
    if not exists:
        return [], False, detail

    try:
        frame = sas.sasdata2dataframe(table="_codx_snapshot", libref="work")
    except Exception as ex:
        message = str(ex)
        if "BOF or EOF is True" in message or "ADODB.Recordset" in message:
            return [], True, detail
        raise

    snapshot: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        filename = str(row.get("filename", "")).strip()
        if not filename:
            continue
        size_val = row.get("size", 0)
        modified = str(row.get("modified_time", "")).strip()
        try:
            size = int(float(size_val))
        except (TypeError, ValueError):
            size = 0
        snapshot.append(
            {
                "filename": filename,
                "size": size,
                "modified_time": modified,
            }
        )
    return snapshot, True, detail


def _collect_output_files(sas: saspy.SASsession, output_dir: str) -> list[dict[str, Any]]:
    targets = [
        ("", output_dir),
        ("data", _join_remote_path(output_dir, "data")),
        ("data\\adam", _join_remote_path(output_dir, "data\\adam")),
        ("data\\tlf", _join_remote_path(output_dir, "data\\tlf")),
        ("program", _join_remote_path(output_dir, "program")),
        ("report", _join_remote_path(output_dir, "report")),
    ]
    merged: dict[str, dict[str, Any]] = {}
    for rel_prefix, remote_dir in targets:
        files, exists, _ = _snapshot_remote_dir_flat(sas, remote_dir)
        if not exists:
            continue
        for item in files:
            base_name = safe_filename(item.get("filename", ""))
            if not base_name:
                continue
            rel_name = base_name if not rel_prefix else f"{rel_prefix}\\{base_name}"
            normalized = _normalize_artifact_relpath(rel_name)
            merged[normalized] = {
                "filename": normalized,
                "size": item.get("size", 0),
                "modified_time": item.get("modified_time", ""),
            }
    return list(merged.values())


def _diff_artifacts(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # â€œå˜æ›´æ–‡ä»¶â€å®šä¹‰ï¼šæ–‡ä»¶æ–°å¢ž/ç¼ºå¤±ï¼Œæˆ– (size, modified_time) å‘ç”Ÿå˜åŒ–ã€‚
    before_map: dict[str, tuple[int, str]] = {}
    for item in before:
        before_map[item["filename"]] = (item["size"], item["modified_time"])

    changed: list[dict[str, Any]] = []
    for item in after:
        key = item["filename"]
        current = (item["size"], item["modified_time"])
        previous = before_map.get(key)
        if previous != current:
            changed.append(item)
    return changed


def _write_log(log_text: str, request_dir: Path, request_id: str) -> Path:
    log_path = request_dir / "execute.log"
    log_path.write_text(log_text, encoding="utf-8", errors="replace")
    local_output_dir = request_dir / "output"
    local_output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = local_output_dir / f"sas_log_{request_id}.log"
    artifact_path.write_text(log_text, encoding="utf-8", errors="replace")
    return artifact_path


def _build_log_artifact(request_id: str, log_path: Path) -> ArtifactItem:
    stat = log_path.stat()
    modified_time = datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds")
    filename = safe_filename(log_path.name)
    return ArtifactItem(
        filename=filename,
        remote_path=str(log_path),
        size=stat.st_size,
        modified_time=modified_time,
        download_url=f"/artifacts/{request_id}/{filename}",
    )


def _write_compare_log(
    request_dir: Path,
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
    changed: list[dict[str, Any]],
) -> None:
    before_map: dict[str, tuple[int, str]] = {
        item["filename"]: (item["size"], item["modified_time"]) for item in before
    }
    after_map: dict[str, tuple[int, str]] = {
        item["filename"]: (item["size"], item["modified_time"]) for item in after
    }
    changed_set = {item["filename"] for item in changed}

    all_names = sorted(set(before_map.keys()) | set(after_map.keys()))
    lines = [
        f"before_count={len(before)}",
        f"after_count={len(after)}",
        f"changed_count={len(changed)}",
        "",
        "status\tfilename\tbefore(size,time)\tafter(size,time)",
    ]

    for name in all_names:
        b = before_map.get(name)
        a = after_map.get(name)
        if b is None and a is not None:
            status = "NEW"
        elif b is not None and a is None:
            status = "DELETED"
        elif name in changed_set:
            status = "CHANGED"
        else:
            status = "UNCHANGED"
        lines.append(f"{status}\t{name}\t{b}\t{a}")

    compare_path = request_dir / "compare.log"
    compare_path.write_text("\n".join(lines) + "\n", encoding="utf-8", errors="replace")


def _download_artifacts(
    sas: saspy.SASsession,
    request_id: str,
    output_dir: str,
    changed_files: list[dict[str, Any]],
) -> list[ArtifactItem]:
    local_output_dir = RUNTIME_DIR / request_id / "output"
    local_output_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[ArtifactItem] = []

    for item in changed_files:
        rel_path = _normalize_artifact_relpath(item["filename"])
        remote_path = _join_remote_path(output_dir, rel_path)
        local_path = local_output_dir / Path(rel_path.replace("\\", "/"))
        local_path.parent.mkdir(parents=True, exist_ok=True)
        sas.download(str(local_path), remote_path)
        if not local_path.exists():
            continue

        display_name = rel_path.replace("\\", "/")
        artifacts.append(
            ArtifactItem(
                filename=display_name,
                remote_path=remote_path,
                size=local_path.stat().st_size,
                modified_time=item["modified_time"],
                download_url=f"/artifacts/{request_id}/{quote(display_name, safe='/')}",
            )
        )
    return artifacts
