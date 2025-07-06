from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.database import get_db
from app.core.slack import slack_service
from app.schemas.health import HealthResponse, SlackHealthResponse, SlackTestResponse, DatabaseHealthResponse

router = APIRouter()


@router.get("/", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(status="healthy", message="Service is running")


@router.get("/slack", response_model=SlackHealthResponse)
async def slack_health_check():
    """Check Slack connectivity"""
    is_configured = slack_service.is_configured()

    if not is_configured:
        return SlackHealthResponse(
            status="not_configured",
            message="Slack is not configured. Please set SLACK_BOT_TOKEN in environment variables.",
        )

    return SlackHealthResponse(
        status="configured", message="Slack is properly configured and ready to send notifications."
    )


@router.post("/slack/test")
async def test_slack_notification(channel: str = "general"):
    """Test Slack notification (for development only)"""
    if not slack_service.is_configured():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Slack is not configured. Please set SLACK_BOT_TOKEN in environment variables.",
        )

    try:
        success = await slack_service.send_simple_notification(
            channel=channel, message="🧪 This is a test notification from CrawlGuard data validation service!"
        )

        if success:
            return SlackTestResponse(status="success", message=f"Test notification sent to #{channel}")
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to send test notification"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error sending test notification: {str(e)}"
        )


@router.get("/db", response_model=DatabaseHealthResponse)
async def database_health_check(db: AsyncSession = Depends(get_db)):
    """Database health check endpoint"""
    try:
        # Test database connection
        await db.execute(text("SELECT 1"))
        return DatabaseHealthResponse(status="healthy", database="connected")
    except Exception as e:
        return DatabaseHealthResponse(status="unhealthy", database="disconnected", error=str(e))
