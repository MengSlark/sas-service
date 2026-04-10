import os
import re
import ctypes
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import quote

import saspy
from dotenv import load_dotenv

from schemas import ArtifactItem, ExecuteRequest, ExecuteResponse

# 运行时目录约定：
# - runtime/<request_id>/execute.log: 执行日志（唯一日志文件，同时可下载）
# - runtime/<request_id>/output/*: 其他可下载产物
# - runtime/tmp: saspy 临时文件目录
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
    # 只保留文件名，防止路径注入
    return Path(name).name


def get_artifact_path(request_id: str, filename: str) -> Path:
    # 下载接口路径解析：
    # 1) execute.log 映射到 runtime/<request_id>/execute.log
    # 2) 其他路径只允许 runtime/<request_id>/output 下的相对路径
    normalized = _normalize_artifact_relpath(filename)
    if normalized == "execute.log":
        return (RUNTIME_DIR / request_id / "execute.log").resolve()
    base_dir = (RUNTIME_DIR / request_id / "output").resolve()
    target = (base_dir / Path(normalized.replace("\\", "/"))).resolve()
    if not (target == base_dir or base_dir in target.parents):
        raise ValueError("invalid artifact path")
    return target


def execute_sas_job(request_id: str, payload: ExecuteRequest) -> ExecuteResponse:
    request_dir = RUNTIME_DIR / request_id
    request_dir.mkdir(parents=True, exist_ok=True)

    total_started_at = perf_counter()
    output_dir = _normalize_output_dir(payload.output_dir)
    sas = None
    com_initialized = False
    temp_env_backup: dict[str, str | None] = {}
    log_text = ""
    log_artifact_path: Path | None = None
    timings: dict[str, float] = {}
    try:
        # 1) 固定 saspy 临时目录，避免系统临时目录权限/策略问题
        t = perf_counter()
        temp_env_backup = _set_temp_dir_for_saspy()
        timings["set_temp_dir"] = perf_counter() - t
        # 2) Windows/IOM 下需先初始化 COM，再创建 SASsession
        t = perf_counter()
        com_initialized = _co_initialize_for_windows()
        timings["co_initialize"] = perf_counter() - t
        t = perf_counter()
        sas = saspy.SASsession(**_sas_config())
        timings["create_session"] = perf_counter() - t
        # 3) 确保远端输出目录及标准子目录存在
        t = perf_counter()
        _ensure_remote_output_structure(sas, output_dir)
        timings["ensure_output_structure"] = perf_counter() - t

        # 4) 执行 SAS 代码并落盘日志
        t = perf_counter()
        submit_result = sas.submit(payload.code)
        timings["sas_submit"] = perf_counter() - t
        log_text = submit_result.get("LOG", "")
        t = perf_counter()
        log_artifact_path = _write_log(log_text, request_dir)
        timings["write_log"] = perf_counter() - t
        t = perf_counter()
        _upload_log_to_remote_program_dir(sas, output_dir, log_artifact_path)
        timings["upload_log_to_remote"] = perf_counter() - t

        # 5) 执行后一次递归扫描获取全量文件，并统计耗时
        t = perf_counter()
        all_files, exists_after, after_err = _snapshot_remote_dir(sas, output_dir)
        timings["recursive_scan"] = perf_counter() - t
        if not exists_after:
            detail = f"output_dir became unavailable after execution: {output_dir}"
            if after_err:
                detail = f"{detail}; SAS detail: {after_err}"
            raise RuntimeError(detail)

        # 6) 下载产物（含子目录）
        t = perf_counter()
        artifacts = _download_artifacts(sas, request_id, output_dir, all_files)
        timings["download_artifacts"] = perf_counter() - t
        timings["total"] = perf_counter() - total_started_at
        print(
            "[phase_timing] "
            f"request_id={request_id} "
            + " ".join(f"{k}={v:.3f}s" for k, v in timings.items())
            + f" file_count={len(all_files)}"
        )
        return ExecuteResponse(
            success=True,
            request_id=request_id,
            artifacts=artifacts,
        )
    except Exception:
        # 异常场景也尽量保留日志，方便定位问题
        _write_log(log_text, request_dir)
        timings["total"] = perf_counter() - total_started_at
        if timings:
            print(
                "[phase_timing] "
                f"request_id={request_id} "
                + " ".join(f"{k}={v:.3f}s" for k, v in timings.items())
                + " status=failed"
            )
        raise
    finally:
        if sas is not None:
            try:
                t = perf_counter()
                sas.endsas()
                timings["endsas"] = perf_counter() - t
            except Exception:
                pass
        _co_uninitialize_for_windows(com_initialized)
        _restore_temp_dir(temp_env_backup)


def _co_initialize_for_windows() -> bool:
    if os.name != "nt":
        return False
    # CoInitialize 返回 0/1 表示当前线程 COM 可用（S_OK/S_FALSE）
    hr = ctypes.windll.ole32.CoInitialize(None)
    if hr not in (0, 1):
        raise RuntimeError(f"CoInitialize failed, HRESULT={hr}")
    return True


def _co_uninitialize_for_windows(initialized: bool) -> None:
    if os.name == "nt" and initialized:
        ctypes.windll.ole32.CoUninitialize()


def _set_temp_dir_for_saspy() -> dict[str, str | None]:
    # 先备份，再覆盖临时目录环境变量，确保 saspy 在可控目录创建临时文件
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
    # 仅使用项目内 sascfg.py，不再复制或动态指定 cfgfile
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
    # 产物路径安全规则：必须是相对路径，且不允许 .. 穿越
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
    # 在 SAS 侧检查目录是否可访问，并从日志中解析状态码/错误信息
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
    # 通过 SAS dcreate 在指定 parent 下创建 leaf 目录
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


def _ensure_remote_output_structure(sas: saspy.SASsession, output_dir: str) -> None:
    # 一次性确保 output_dir 以及标准子目录存在
    normalized = output_dir.strip().replace("/", "\\").rstrip("\\")
    if not normalized:
        raise ValueError("output_dir cannot be empty")

    targets = [
        normalized,
        _join_remote_path(normalized, "data"),
        _join_remote_path(normalized, "data\\adam"),
        _join_remote_path(normalized, "data\\tlf"),
        _join_remote_path(normalized, "program"),
        _join_remote_path(normalized, "report"),
    ]
    _ensure_remote_dirs_batch(sas, targets)
    exists_after, err_after = _check_remote_dir_exists(sas, normalized)
    if not exists_after:
        detail = f"Failed to create output_dir {normalized}"
        if err_after:
            detail = f"{detail}; SAS detail: {err_after}"
        raise RuntimeError(detail)


def _expand_dir_chain(path: str) -> list[str]:
    normalized = path.strip().replace("/", "\\").rstrip("\\")
    if not normalized:
        return []
    segments = [seg for seg in normalized.split("\\") if seg]
    if not segments:
        return []

    drive_pattern = re.compile(r"^[A-Za-z]:$")
    if not drive_pattern.match(segments[0]):
        return [normalized]

    chain: list[str] = []
    current = segments[0] + "\\"
    for leaf in segments[1:]:
        parent = current.rstrip("\\")
        current = f"{parent}\\{leaf}" if parent else leaf
        chain.append(current)
    return chain or [normalized]


def _ensure_remote_dirs_batch(sas: saspy.SASsession, paths: list[str]) -> None:
    # 将多个路径展开为有序目录链，并在一次 sas.submit 中批量创建缺失目录
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        for candidate in _expand_dir_chain(raw):
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            ordered.append(candidate)

    if not ordered:
        return

    blocks: list[str] = []
    for target in ordered:
        parent = str(Path(target).parent).replace("/", "\\").rstrip("\\")
        leaf = Path(target).name
        if not parent or not leaf:
            continue
        q_target = _sas_quote(target)
        q_parent = _sas_quote(parent)
        q_leaf = _sas_quote(leaf)
        blocks.append(
            f"""
    dir = '{q_target}';
    rc = filename('_cdxdir', dir);
    did = dopen('_cdxdir');
    if did > 0 then do;
        rc = dclose(did);
    end;
    else do;
        parent = '{q_parent}';
        leaf = '{q_leaf}';
        rc = filename('_cdxpar', parent);
        pdid = dopen('_cdxpar');
        if pdid <= 0 then do;
            msg = cats('parent_not_accessible: ', parent, ' | ', sysmsg());
            put 'CODX_BATCH_ERR=' msg;
            call symputx('_codx_batch_ok', '0', 'G');
        end;
        else do;
            rc = dclose(pdid);
            parent = pathname('_cdxpar');
            created = dcreate(leaf, parent);
            if missing(created) then do;
                msg = cats('dcreate_failed: ', dir, ' | ', sysmsg());
                put 'CODX_BATCH_ERR=' msg;
                call symputx('_codx_batch_ok', '0', 'G');
            end;
        end;
        rc = filename('_cdxpar');
    end;
    rc = filename('_cdxdir');
"""
        )

    if not blocks:
        return

    code = f"""
%global _codx_batch_ok;
%let _codx_batch_ok=1;
data _null_;
    length dir parent leaf created msg $512;
    length rc did pdid 8;
{''.join(blocks)}
run;
%put CODX_BATCH_OK=&_codx_batch_ok;
"""
    submit_result = sas.submit(code)
    log_text = submit_result.get("LOG", "")
    ok = bool(re.search(r"CODX_BATCH_OK=1", log_text))
    if ok:
        return
    err_match = re.findall(r"CODX_BATCH_ERR=([^\r\n]*)", log_text)
    err_text = err_match[-1].strip() if err_match else "unknown SAS batch mkdir error"
    raise RuntimeError(f"Failed to create output directory structure: {err_text}")


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
    # 非递归扫描：仅获取当前目录第一层文件
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
    # 递归扫描：用于确认目录可访问并支持后续全量收集
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
    # 按约定目录收集文件：根目录、data、data/adam、data/tlf、program、report
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
    # “变更文件”定义：文件新增/缺失，或 (size, modified_time) 发生变化
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


def _write_log(log_text: str, request_dir: Path) -> Path:
    # 仅保留一份日志：execute.log（并作为可下载 artifact 暴露）
    log_path = request_dir / "execute.log"
    log_path.write_text(log_text, encoding="utf-8", errors="replace")
    return log_path


def _upload_log_to_remote_program_dir(sas: saspy.SASsession, output_dir: str, log_path: Path) -> None:
    # 将执行日志写入远端 output_dir\program\execute.log，供产物下载阶段统一收集
    remote_log_path = _join_remote_path(output_dir, "program\\execute.log")
    # 为避免 SAS 会话编码转换失败，统一上传 ASCII 安全文本。
    raw_text = log_path.read_text(encoding="utf-8", errors="replace")
    safe_text = raw_text.encode("ascii", errors="backslashreplace").decode("ascii")
    fallback_path = log_path.with_name("execute_upload_safe.log")
    fallback_path.write_text(safe_text, encoding="ascii", errors="strict")
    try:
        sas.upload(str(fallback_path), remote_log_path)
    finally:
        if fallback_path.exists():
            fallback_path.unlink()


def _write_compare_log(
    request_dir: Path,
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
    changed: list[dict[str, Any]],
) -> None:
    # 调试辅助：写入前后快照对比表（当前默认不对外暴露）
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
    # 按相对路径下载远端文件，并在本地保留目录结构
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
