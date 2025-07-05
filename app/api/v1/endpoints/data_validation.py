from app.core.data_quality.validator_factory import ValidatorFactory
from app.models.validation_models import ValidationRule, ValidationResult
from app.core.data_quality.file_loader import load_file

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select,update
from typing import List
from datetime import datetime, timezone
import os
import shutil

from app.core.database import get_db
from app.models.dataset import Dataset
from app.models.rule import Rule
from app.schemas.dataset import DatasetResponse, DatasetUpdate



router = APIRouter()

@router.get("/{project_id}/dataset/{dataset_id}/validate")
async def validate_data(
    project_id: int,
    dataset_id: int,
    db: AsyncSession = Depends(get_db)
):
    # check dataset exists
    try:
        result_project = await db.execute(
            select(Dataset).where(
                (Dataset.id == dataset_id) & (Dataset.project_id == project_id)
            )
        )
        dataset_project = result_project.scalar_one_or_none()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error while checking dataset: {str(e)}"
        )

    if dataset_project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} in project {project_id} not found"
        )

    # fetch rules
    try:
        stmt = (
            select(
                Rule.name,
                Rule.description,
                Rule.natural_language_rule,
                Rule.great_expectations_rule,
                Rule.type
            )
            .where(Rule.project_id == project_id)
        )
        result = await db.execute(stmt)
        rows = result.all()
    except Exception as  e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error while fetching rules: {str(e)}"
        )

    rules = [
        {
            "name": r[0],
            "description": r[1],
            "natural_language_rule": r[2],
            "great_expectations_rule": r[3],
            "type": r[4],
        }
        for r in rows
    ]

    # validate
    validator = ValidatorFactory.create_validator(
        dataset_project.file_path
    )

    try:
        results = validator.validate_rules(rules)
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No validation results found"
            )
        
        stmt_add = (
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(validations=results)
        )

        result = await db.execute(stmt_add)
        # return [ValidationResult(**result) for result in results]

        return results
        

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Validation error: {str(e)}"
        )

