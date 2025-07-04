from sqlalchemy import Column, Integer, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.core.database import Base


class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    file_path = Column(Text, nullable=False)
    is_sample = Column(Boolean, default=False, nullable=False)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationship
    project = relationship("Project", back_populates="datasets")

    def __repr__(self):
        return f"<Dataset(id={self.id}, project_id={self.project_id}, is_sample={self.is_sample})>"
