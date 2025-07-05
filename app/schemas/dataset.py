from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import List, Optional


class DatasetBase(BaseModel):
    pass


class DatasetCreate(DatasetBase):
    file_path: str
    is_sample: bool = False
    project_id: int


class DatasetUpdate(BaseModel):
    file_path: str | None = None
    is_sample: bool | None = None


class DatasetResponse(DatasetBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    file_path: str
    is_sample: bool
    columns: Optional[List[str]] = None
    project_id: int
    created_at: datetime
    updated_at: datetime
