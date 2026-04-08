# SAS FastAPI Execution Service

## Start

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

## API

- `GET /health`
- `POST /execute` with JSON body

```json
{
  "code": "data _null_; put 'hello'; run;",
  "output_dir": "D:\\SAS_Data\\project\\output",
  "input_paths": []
}
```

Response:

```json
{
  "success": true,
  "request_id": "a1b2c3d4e5f6",
  "log": "...",
  "artifacts": [
    {
      "filename": "result.rtf",
      "remote_path": "D:\\SAS_Data\\project\\output\\result.rtf",
      "size": 12345,
      "modified_time": "31Mar2026:15:20:01",
      "download_url": "/artifacts/a1b2c3d4e5f6/result.rtf"
    }
  ]
}
```

- `GET /artifacts/{request_id}/{filename}` to download local cached artifact

## Behavior

- Service only executes submitted SAS code; it does not rewrite code.
- `output_dir` is treated as a remote SAS server directory.
- Artifacts include only files created or modified during this execution.
- File content is not embedded; all files are returned as downloadable artifacts.
- Raw SAS log is returned as-is.

## Env

Required variables in `.env`:

- `SAS_CFGFILE`
- `SAS_USER`
- `SAS_PW`

## Structure

- `app.py`: API routes only
- `schemas.py`: request/response models
- `sas_service.py`: SAS execution, directory snapshot diff, artifact download
- `runtime/`: execution logs and cached artifacts
