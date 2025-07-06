import json
from typing import Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.models.project import Project


async def calculate_project_summary(project: Project, db: AsyncSession) -> Dict[str, Any]:
    """Calculate summary statistics for a project and return as dict"""
    total_datasets = len(project.datasets)
    total_rules = len([rule for rule in project.rules if not rule.is_deleted])

    # Initialize counters
    total_issues = 0
    datasets_with_issues = 0
    total_validations = 0
    successful_validations = 0
    last_validation_date = None

    # Calculate statistics from cached validation results
    for dataset in project.datasets:
        if dataset.validations:
            try:
                # Parse cached validation results
                if isinstance(dataset.validations, str):
                    validation_data = json.loads(dataset.validations)
                else:
                    validation_data = dataset.validations

                # Count issues and successful validations
                dataset_issues = 0
                dataset_successful = 0

                if isinstance(validation_data, list):
                    for result in validation_data:
                        if not result.get("passed", False):
                            dataset_issues += result.get("failed_records", 0)
                        else:
                            dataset_successful += 1

                total_issues += dataset_issues
                if dataset_issues > 0:
                    datasets_with_issues += 1

                total_validations += len(validation_data) if isinstance(validation_data, list) else 0
                successful_validations += dataset_successful

                # Track last validation date
                if dataset.last_validated_at:
                    if not last_validation_date or dataset.last_validated_at > last_validation_date:
                        last_validation_date = dataset.last_validated_at

            except (json.JSONDecodeError, TypeError, AttributeError):
                # Skip invalid validation data
                continue

    # Calculate overall success rate
    overall_success_rate = (successful_validations / total_validations * 100) if total_validations > 0 else 0.0

    return {
        "total_datasets": total_datasets,
        "total_rules": total_rules,
        "total_issues": total_issues,
        "overall_success_rate": round(overall_success_rate, 2),
        "datasets_with_issues": datasets_with_issues,
        "last_validation_date": last_validation_date.isoformat() if last_validation_date else None,
    }


async def update_project_summary(project_id: int, db: AsyncSession) -> None:
    """Update the cached summary for a specific project"""
    # Fetch project with all relationships
    result = await db.execute(
        select(Project)
        .where(Project.id == project_id)
        .options(selectinload(Project.datasets), selectinload(Project.rules))
    )
    project = result.scalar_one_or_none()

    if not project:
        return

    # Calculate new summary
    summary = await calculate_project_summary(project, db)

    # Update the project's summary field
    project.summary = summary
    await db.commit()


async def update_all_project_summaries(db: AsyncSession) -> None:
    """Update cached summaries for all projects"""
    # Fetch all projects with relationships
    result = await db.execute(select(Project).options(selectinload(Project.datasets), selectinload(Project.rules)))
    projects = result.scalars().all()

    for project in projects:
        summary = await calculate_project_summary(project, db)
        project.summary = summary

    await db.commit()
