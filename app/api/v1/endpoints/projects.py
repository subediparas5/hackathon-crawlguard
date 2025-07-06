from app.core.slack import slack_service
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from typing import List
from datetime import datetime, timezone
import json

from app.core.database import get_db
from app.models.project import Project
from app.schemas.project import ProjectCreate, ProjectResponse, ProjectUpdate, ProjectSummary

router = APIRouter()


def get_cached_summary(project: Project) -> ProjectSummary:
    """Get cached summary from project or return default if not available"""
    if project.summary:
        try:
            # Parse cached summary
            if isinstance(project.summary, str):
                summary_data = json.loads(project.summary)
            else:
                summary_data = project.summary

            return ProjectSummary(
                total_datasets=summary_data.get("total_datasets", 0),
                total_rules=summary_data.get("total_rules", 0),
                total_issues=summary_data.get("total_issues", 0),
                overall_success_rate=summary_data.get("overall_success_rate", 0.0),
                datasets_with_issues=summary_data.get("datasets_with_issues", 0),
                last_validation_date=summary_data.get("last_validation_date"),
            )
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    # Return default summary if no cached data
    return ProjectSummary(
        total_datasets=len(project.datasets),
        total_rules=len([rule for rule in project.rules if not rule.is_deleted]),
        total_issues=0,
        overall_success_rate=0.0,
        datasets_with_issues=0,
        last_validation_date=None,
    )


@router.get("/", response_model=List[ProjectResponse])
async def get_projects(db: AsyncSession = Depends(get_db)):
    """Get all projects"""
    result = await db.execute(select(Project).options(selectinload(Project.datasets), selectinload(Project.rules)))
    projects = result.scalars().all()

    # Convert to response models with has_sample field and summary
    project_responses = []
    for project in projects:
        has_sample = any(dataset.is_sample for dataset in project.datasets)

        # Get cached project summary
        summary = get_cached_summary(project)

        project_dict = {
            "id": project.id,
            "name": project.name,
            "description": project.description,
            "status": project.status,
            "slack_channel": project.slack_channel,
            "created_at": project.created_at,
            "updated_at": project.updated_at,
            "datasets": project.datasets,
            "rules": project.rules,
            "has_sample": has_sample,
            "summary": summary,
        }
        project_responses.append(ProjectResponse(**project_dict))

    return project_responses


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(project_id: int, db: AsyncSession = Depends(get_db)):
    """Get project by ID"""
    result = await db.execute(
        select(Project)
        .where(Project.id == project_id)
        .options(selectinload(Project.datasets), selectinload(Project.rules))
    )
    project = result.scalar_one_or_none()

    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    # Calculate has_sample field and summary
    has_sample = any(dataset.is_sample for dataset in project.datasets)
    summary = get_cached_summary(project)

    project_dict = {
        "id": project.id,
        "name": project.name,
        "description": project.description,
        "status": project.status,
        "slack_channel": project.slack_channel,
        "created_at": project.created_at,
        "updated_at": project.updated_at,
        "datasets": project.datasets,
        "rules": project.rules,
        "has_sample": has_sample,
        "summary": summary,
    }

    return ProjectResponse(**project_dict)


@router.post("/", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project(project: ProjectCreate, db: AsyncSession = Depends(get_db)):
    """Create a new project"""
    # Check if project with same name already exists
    result = await db.execute(select(Project).where(Project.name == project.name))
    existing_project = result.scalar_one_or_none()

    if existing_project:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Project with this name already exists")

    # test if slack channel is valid
    if project.slack_channel:
        try:
            await slack_service.send_simple_notification(project.slack_channel, "Test message")
        except Exception:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Slack channel")

    # Create new project
    db_project = Project(
        name=project.name, description=project.description, status=project.status, slack_channel=project.slack_channel
    )

    db.add(db_project)
    await db.commit()
    await db.refresh(db_project)

    # Load all relationships for response
    result = await db.execute(
        select(Project)
        .where(Project.id == db_project.id)
        .options(selectinload(Project.datasets), selectinload(Project.rules))
    )
    db_project = result.scalar_one()

    # Calculate has_sample field and summary
    has_sample = any(dataset.is_sample for dataset in db_project.datasets)
    summary = get_cached_summary(db_project)
    project_dict = {
        "id": db_project.id,
        "name": db_project.name,
        "description": db_project.description,
        "status": db_project.status,
        "slack_channel": db_project.slack_channel,
        "created_at": db_project.created_at,
        "updated_at": db_project.updated_at,
        "datasets": db_project.datasets,
        "rules": db_project.rules,
        "has_sample": has_sample,
        "summary": summary,
    }

    return ProjectResponse(**project_dict)


@router.put("/{project_id}", response_model=ProjectResponse)
async def update_project(project_id: int, project: ProjectUpdate, db: AsyncSession = Depends(get_db)):
    """Update an existing project"""
    result = await db.execute(select(Project).where(Project.id == project_id))
    db_project = result.scalar_one_or_none()

    if not db_project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    # Update project fields
    if project.name is not None:
        db_project.name = project.name
    if project.description is not None:
        db_project.description = project.description
    if project.status is not None:
        db_project.status = project.status
    if project.slack_channel is not None:
        db_project.slack_channel = project.slack_channel

    # Set updated_at to current time
    db_project.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(db_project)

    # Load all relationships for response
    result = await db.execute(
        select(Project)
        .where(Project.id == db_project.id)
        .options(selectinload(Project.datasets), selectinload(Project.rules))
    )
    db_project = result.scalar_one()

    # Calculate has_sample field and summary
    has_sample = any(dataset.is_sample for dataset in db_project.datasets)
    summary = get_cached_summary(db_project)
    project_dict = {
        "id": db_project.id,
        "name": db_project.name,
        "description": db_project.description,
        "status": db_project.status,
        "slack_channel": db_project.slack_channel,
        "created_at": db_project.created_at,
        "updated_at": db_project.updated_at,
        "datasets": db_project.datasets,
        "rules": db_project.rules,
        "has_sample": has_sample,
        "summary": summary,
    }

    return ProjectResponse(**project_dict)


@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_project(project_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a project"""
    result = await db.execute(select(Project).where(Project.id == project_id))
    db_project = result.scalar_one_or_none()

    if not db_project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    await db.delete(db_project)
    await db.commit()

    return None
