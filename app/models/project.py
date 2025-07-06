from sqlalchemy import Column, Integer, String, Text, DateTime, Enum, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum

from app.core.database import Base


class ProjectStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    ARCHIVED = "ARCHIVED"


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    description = Column(Text, nullable=True)
    status = Column(Enum(ProjectStatus), default=ProjectStatus.ACTIVE, nullable=False)
    slack_channel = Column(String(100), nullable=True, index=True)
    summary = Column(JSON, nullable=True, comment="Cached project summary statistics")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    datasets = relationship("Dataset", back_populates="project", cascade="all, delete-orphan")
    rules = relationship("Rule", back_populates="project", cascade="all, delete-orphan")
    suggested_rules = relationship("SuggestedRules", back_populates="project", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Project(id={self.id}, name={self.name}, status={self.status})>"
