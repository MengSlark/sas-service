from pydantic import BaseModel, Field


class ExecuteRequest(BaseModel):
    code: str = Field(..., min_length=1, description="SAS code to submit")
    output_dir: str = Field(..., min_length=1, description="Remote absolute output dir")
    input_paths: list[str] = Field(default_factory=list, description="Reserved for future use")


class ArtifactItem(BaseModel):
    filename: str
    remote_path: str
    size: int
    modified_time: str
    download_url: str


class ExecuteResponse(BaseModel):
    success: bool
    request_id: str
    artifacts: list[ArtifactItem]
