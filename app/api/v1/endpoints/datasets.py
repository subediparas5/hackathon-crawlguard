from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
from datetime import datetime, timezone
import os
import shutil

from app.core.database import get_db
from app.models.dataset import Dataset
from app.schemas.dataset import DatasetResponse, DatasetUpdate

router = APIRouter()

# Create uploads directory if it doesn't exist
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


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
    file_path = os.path.join(UPLOAD_DIR, f"sample_{file.filename}")
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    db_dataset = Dataset(file_path=file_path, is_sample=True, project_id=project_id)
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)
    return db_dataset


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
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    db_dataset = Dataset(file_path=file_path, is_sample=False, project_id=project_id)
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)
    return db_dataset


@router.put("/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(dataset_id: int, dataset: DatasetUpdate, db: AsyncSession = Depends(get_db)):
    """Update an existing dataset"""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    db_dataset = result.scalar_one_or_none()

    if not db_dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    # Update dataset fields
    if dataset.file_path is not None:
        db_dataset.file_path = dataset.file_path

    if dataset.is_sample:
        # if is_sample is set to True, then we need to set the other datasets for this project to False
        if dataset.is_sample:
            result = await db.execute(
                select(Dataset).where(Dataset.project_id == db_dataset.project_id, Dataset.is_sample.is_(True))
            )
            datasets = result.scalars().all()
            for dataset in datasets:
                dataset.is_sample = False
        db_dataset.is_sample = True

    # Set updated_at to current time
    db_dataset.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(db_dataset)

    return db_dataset


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(dataset_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a dataset"""
    result = await db.execute(select(Dataset).where(Dataset.id == dataset_id))
    db_dataset = result.scalar_one_or_none()

    if not db_dataset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    # Delete the file if it exists
    if os.path.exists(db_dataset.file_path):
        os.remove(db_dataset.file_path)

    await db.delete(db_dataset)
    await db.commit()

    return None
