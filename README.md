# CrawlGuard

A FastAPI application with PostgreSQL database, containerized with Docker Compose.

## Features

- FastAPI web framework
- PostgreSQL database with async SQLAlchemy
- Docker and Docker Compose setup
- PgAdmin for database management
- Health check endpoints
- User management API
- Environment-based configuration

## Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)
- uv (recommended package manager)

## Quick Start

1. Clone the repository:
```bash
git clone <repository-url>
cd crawlguard
```

2. Install uv (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Create environment file:
```bash
cp env.example .env
```

4. Start the services:
```bash
docker-compose up -d
```

5. Access the application:
- API: http://localhost:8000
- API Documentation: http://localhost:8000/docs
- PgAdmin: http://localhost:5050 (admin@crawlguard.com / admin)

## Development

### Local Development Setup

1. Install dependencies (using uv for faster dependency resolution):
```bash
uv sync
uv sync --extra dev
```

2. Set up environment variables:
```bash
cp env.example .env
# Edit .env with your local database settings
```

3. Run the application:
```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Note**: Both Docker containers and local development use uv for fast and reproducible dependency management.

### Database Migrations

The application uses SQLAlchemy with async support. Database tables are created automatically on startup.

For production, consider using Alembic for migrations:

```bash
# Initialize Alembic (first time only)
uv run alembic init alembic

# Create a migration
uv run alembic revision --autogenerate -m "Initial migration"

# Apply migrations
uv run alembic upgrade head
```

## API Endpoints

### Health Checks
- `GET /` - Root endpoint
- `GET /health` - Health check
- `GET /api/v1/health/` - API health check
- `GET /api/v1/health/db` - Database health check

### Projects
- `GET /api/v1/projects/` - Get all projects
- `GET /api/v1/projects/{project_id}` - Get project by ID
- `POST /api/v1/projects/` - Create new project
- `PUT /api/v1/projects/{project_id}` - Update project
- `DELETE /api/v1/projects/{project_id}` - Delete project

### Datasets
- `GET /api/v1/datasets/` - Get all datasets
- `GET /api/v1/datasets/{dataset_id}` - Get dataset by ID
- `POST /api/v1/datasets/upload-sample` - Upload a sample dataset (first dataset)
- `POST /api/v1/datasets/` - Create a new dataset (not sample)
- `PUT /api/v1/datasets/{dataset_id}` - Update a dataset
- `DELETE /api/v1/datasets/{dataset_id}` - Delete a dataset

## Docker Services

- **app**: FastAPI application (port 8000)
- **db**: PostgreSQL database (port 5432)
- **pgadmin**: Database management interface (port 5050)

## Environment Variables

Key environment variables (see `env.example`):

- `DATABASE_URL`: PostgreSQL connection string
- `ENVIRONMENT`: Application environment (development/production)
- `SECRET_KEY`: Secret key for JWT tokens
- `DEBUG`: Enable debug mode

## Project Structure

```
crawlguard/
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── endpoints/
│   │       │   ├── health.py
│   │       │   └── users.py
│   │       └── api.py
│   ├── core/
│   │   ├── config.py
│   │   └── database.py
│   ├── models/
│   │   └── user.py
│   ├── schemas/
│   │   └── user.py
│   └── main.py
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
└── README.md
```

## Testing

### Running Tests

1. Install test dependencies:
```bash
uv sync --extra dev
```

2. Run all tests:
```bash
uv run pytest tests/ -v
```

3. Run only integration tests:
```bash
uv run pytest tests/integration/ -v
```

4. Run with coverage:
```bash
uv run pytest tests/ --cov=app --cov-report=html
```

### Test Structure

- `tests/integration/` - Integration tests for API endpoints
- `tests/unit/` - Unit tests for individual components
- `conftest.py` - Pytest fixtures and configuration

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.
