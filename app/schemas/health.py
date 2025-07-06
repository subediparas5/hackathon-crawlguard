from typing import Optional
from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Response model for basic health check"""

    status: str = Field(..., description="Health status (healthy/unhealthy)")
    message: str = Field(..., description="Health check message")


class SlackHealthResponse(BaseModel):
    """Response model for Slack health check"""

    status: str = Field(..., description="Slack configuration status")
    message: str = Field(..., description="Detailed message about Slack configuration")


class SlackTestResponse(BaseModel):
    """Response model for Slack test notification"""

    status: str = Field(..., description="Test notification status")
    message: str = Field(..., description="Test notification message")


class DatabaseHealthResponse(BaseModel):
    """Response model for database health check"""

    status: str = Field(..., description="Database health status")
    database: str = Field(..., description="Database connection status")
    error: Optional[str] = Field(default=None, description="Error message if database is unhealthy")
