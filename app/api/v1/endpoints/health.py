from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.database import get_db

router = APIRouter()


@router.get("/")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "crawlguard-api"}


@router.get("/db")
async def database_health_check(db: AsyncSession = Depends(get_db)):
    """Database health check endpoint"""
    try:
        # Test database connection
        await db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}
