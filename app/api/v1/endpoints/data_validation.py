from app.core.data_quality.validator_factory import ValidatorFactory

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.core.database import get_db
from app.models.dataset import Dataset
from app.models.rule import Rule


router = APIRouter()


@router.get("/")
async def validate_data(project_id: int, dataset_id: int, db: AsyncSession = Depends(get_db)):
    # check dataset exists
    try:
        result_project = await db.execute(
            select(Dataset).where((Dataset.id == dataset_id) & (Dataset.project_id == project_id))
        )
        dataset_project = result_project.scalar_one_or_none()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error while checking dataset: {str(e)}"
        )

    if not dataset_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Dataset {dataset_id} in project {project_id} not found"
        )

    # fetch rules
    try:
        rule_query = select(
            Rule.name,
            Rule.description,
            Rule.natural_language_rule,
            Rule.great_expectations_rule,
            Rule.type,
        ).where(Rule.project_id == project_id)
        result = await db.execute(rule_query)
        rows = result.all()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error while fetching rules: {str(e)}"
        )

    rules = [
        {
            "name": r.name,
            "description": r.description,
            "natural_language_rule": r.natural_language_rule,
            "great_expectations_rule": r.great_expectations_rule,
            "type": r.type,
        }
        for r in rows
    ]

    # validate
    validator = ValidatorFactory.create_validator(str(dataset_project.file_path))

    try:
        results = validator.validate_rules(rules)
        if not results:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No validation results found")

        validations_query = select(Dataset).where(Dataset.id == dataset_id)

        result = await db.execute(validations_query)
        dataset = result.scalar_one_or_none()
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        dataset.validations = results
        await db.commit()

        return results

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Validation error: {str(e)}")
