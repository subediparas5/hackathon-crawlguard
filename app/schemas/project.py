from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, List

from app.models.project import ProjectStatus
from app.schemas.dataset import DatasetResponse
from app.schemas.rule import RuleResponse


class ProjectBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    status: ProjectStatus = ProjectStatus.ACTIVE


class ProjectCreate(ProjectBase):
    pass


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    status: Optional[ProjectStatus] = None


class ProjectResponse(ProjectBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime
    datasets: List[DatasetResponse] = []
    rules: List[RuleResponse] = []
    has_sample: bool = False
