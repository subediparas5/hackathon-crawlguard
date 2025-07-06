import csv
import json
import asyncio
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
from datetime import datetime, timezone
import os

from app.core.database import get_db
from app.models.dataset import Dataset
from app.schemas.dataset import DatasetResponse, DatasetUpdate

router = APIRouter()

# Create uploads directory if it doesn't exist
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _flatten_json_keys(obj, prefix=""):
    """
    Recursively flatten JSON object keys using dot notation.

    Args:
        obj: JSON object to flatten
        prefix: Current prefix for nested keys

    Returns:
        List of flattened column names
    """
    flattened_keys = []

    if isinstance(obj, dict):
        for key, value in obj.items():
            new_prefix = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                # Recursively flatten nested objects
                flattened_keys.extend(_flatten_json_keys(value, new_prefix))
            elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                # Handle arrays of objects - flatten the first object
                flattened_keys.extend(_flatten_json_keys(value[0], new_prefix))
            else:
                # Add the current key
                flattened_keys.append(new_prefix)
    elif isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
        # Handle arrays of objects
        flattened_keys.extend(_flatten_json_keys(obj[0], prefix))

    return flattened_keys


@router.get("/", response_model=List[DatasetResponse])
async def get_datasets(project_id: int | None = None, db: AsyncSession = Depends(get_db)):
    """Get all datasets, optionally filtered by project_id"""
    if project_id:
        result = await db.execute(select(Dataset).where(Dataset.project_id == project_id))
    else:
        result = await db.execute(select(Dataset))
    datasets = result.scalars().all()
    return datasets


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: int, db: AsyncSession = Depends(get_db)):
    """Get dataset by ID"""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    dataset = result.scalar_one_or_none()

    if not dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    return dataset


@router.post("/upload-sample", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def upload_sample_dataset(project_id: int, file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    """Upload a sample dataset (first dataset) for a specific project"""
    from app.models.project import Project
    from app.core.rule_generator import trigger_rule_generation_for_project

    project_result = await db.execute(select(Project).where(Project.id == project_id))
    project = project_result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    # Check if a sample dataset already exists for this project
    result = await db.execute(select(Dataset).where(Dataset.is_sample.is_(True), Dataset.project_id == project_id))
    existing_sample = result.scalar_one_or_none()
    if existing_sample:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="A sample dataset already exists for this project"
        )
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")

    # Read file content for CSV parsing
    content = await file.read()
    content_str = content.decode("utf-8")

    # Parse columns based on file type
    columns = None
    if file.filename.endswith(".csv"):
        reader = csv.DictReader(content_str.splitlines())
        columns = reader.fieldnames
    elif file.filename.endswith(".json"):
        try:
            json_data = json.loads(content_str)
            if isinstance(json_data, list) and len(json_data) > 0:
                # Extract flattened keys from the first object in the array
                columns = list(_flatten_json_keys(json_data[0]))
            elif isinstance(json_data, dict):
                # For single JSON object, extract flattened top-level keys
                columns = list(_flatten_json_keys(json_data))
        except json.JSONDecodeError:
            # If JSON parsing fails, set columns to None
            columns = None

    # Write the file to disk
    file_path = os.path.join(UPLOAD_DIR, f"sample_{file.filename}")
    with open(file_path, "wb") as buffer:
        buffer.write(content)  # Write the content we already read

    db_dataset = Dataset(file_path=file_path, is_sample=True, project_id=project_id, columns=columns)
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)

    # Trigger rule generation in the background
    asyncio.create_task(trigger_rule_generation_for_project(project_id, force_regenerate=True))

    return db_dataset


async def trigger_validation_for_dataset(project_id: int, dataset_id: int) -> None:
    """
    Trigger validation for a dataset as a background task.
    This function can be called without awaiting to avoid blocking the main request.
    """

    async def _run():
        try:
            from app.api.v1.endpoints.data_validation import validate_data
            from app.core.database import AsyncSessionLocal

            async with AsyncSessionLocal() as db:
                # Call the validation function
                await validate_data(project_id=project_id, dataset_id=dataset_id, db=db)
        except Exception as e:
            print(f"Error during validation for dataset {dataset_id} in project {project_id}: {e}")

    try:
        asyncio.create_task(_run())
        print(f"Triggered background validation for dataset {dataset_id} in project {project_id}")
    except Exception as e:
        print(f"Error triggering validation for dataset {dataset_id} in project {project_id}: {e}")


@router.post("/", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(project_id: int, file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    """Create a new dataset (not sample) for a specific project"""
    from app.models.project import Project

    project_result = await db.execute(select(Project).where(Project.id == project_id))
    project = project_result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No file provided")

    # Read file content
    content = await file.read()

    # Write the file to disk
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        buffer.write(content)  # Write the content we already read

    db_dataset = Dataset(file_path=file_path, is_sample=False, project_id=project_id)
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)

    # Trigger validation in the background
    asyncio.create_task(trigger_validation_for_dataset(project_id, db_dataset.id))

    return db_dataset


@router.put("/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(dataset_id: int, dataset: DatasetUpdate, db: AsyncSession = Depends(get_db)):
    """Update an existing dataset"""
    from app.core.rule_generator import trigger_rule_generation_for_project

    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    db_dataset = result.scalar_one_or_none()

    if not db_dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    # Track if sample dataset was changed
    sample_dataset_changed = False

    # Update dataset fields
    if dataset.file_path is not None:
        db_dataset.file_path = dataset.file_path
        # If this is a sample dataset and file path changed, trigger rule regeneration
        if db_dataset.is_sample:
            sample_dataset_changed = True

    if dataset.is_sample is not None:
        # if is_sample is set to True, then we need to set the other datasets for this project to False
        if dataset.is_sample:
            result = await db.execute(
                select(Dataset).where(Dataset.project_id == db_dataset.project_id, Dataset.is_sample.is_(True))
            )
            datasets = result.scalars().all()
            for existing_dataset in datasets:
                existing_dataset.is_sample = False
        db_dataset.is_sample = dataset.is_sample
        # If sample status changed, trigger rule regeneration
        sample_dataset_changed = True

    # Set updated_at to current time
    db_dataset.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(db_dataset)

    # Trigger rule generation if sample dataset was changed
    if sample_dataset_changed:
        asyncio.create_task(trigger_rule_generation_for_project(db_dataset.project_id, force_regenerate=True))

    return db_dataset


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(dataset_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a dataset"""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    db_dataset = result.scalar_one_or_none()

    if not db_dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    # Delete the file if it exists
    if os.path.exists(str(db_dataset.file_path)):
        os.remove(str(db_dataset.file_path))

    await db.delete(db_dataset)
    await db.commit()

    return None
