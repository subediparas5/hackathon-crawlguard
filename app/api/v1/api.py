from fastapi import APIRouter

from app.api.v1.endpoints import health, projects, datasets, rules

api_router = APIRouter()

# Include endpoint routers
api_router.include_router(health.router, prefix="/health", tags=["Health"])
api_router.include_router(projects.router, prefix="/projects", tags=["Projects"])
api_router.include_router(datasets.router, prefix="/datasets", tags=["Datasets"])
api_router.include_router(rules.router, prefix="/projects/{project_id}/rules", tags=["Rules"])
