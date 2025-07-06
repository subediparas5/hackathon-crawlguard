from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, List

from app.models.project import ProjectStatus
from app.schemas.dataset import DatasetResponse
from app.schemas.rule import RuleResponse


class ProjectSummary(BaseModel):
    """Summary statistics for a project"""

    total_datasets: int = Field(..., description="Total number of datasets in the project")
    total_rules: int = Field(..., description="Total number of rules in the project")
    total_issues: int = Field(..., description="Total number of validation issues across all datasets")
    overall_success_rate: float = Field(..., description="Overall success rate across all validations")
    datasets_with_issues: int = Field(..., description="Number of datasets that have validation issues")
    last_validation_date: Optional[datetime] = Field(None, description="Date of the most recent validation")


class ProjectBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    status: ProjectStatus = ProjectStatus.ACTIVE
    slack_channel: Optional[str] = Field(None, max_length=100, description="Slack channel for notifications")


class ProjectCreate(ProjectBase):
    pass


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    status: Optional[ProjectStatus] = None
    slack_channel: Optional[str] = Field(None, max_length=100, description="Slack channel for notifications")


class ProjectResponse(ProjectBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime
    datasets: List[DatasetResponse] = []
    rules: List[RuleResponse] = []
    has_sample: bool = False
    summary: Optional[ProjectSummary] = Field(None, description="Project summary statistics")
