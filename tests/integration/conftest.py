import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.core.database import get_db, Base
from app.models.project import Project, ProjectStatus
from app.models.dataset import Dataset
from app.models.rule import Rule


# Test database URL
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

# Create test engine
test_engine = create_async_engine(
    TEST_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)

# Create test session factory
TestingSessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
async def setup_database():
    """Set up test database and create tables."""
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def db_session():
    """Create a test database session."""
    async with TestingSessionLocal() as session:
        yield session


@pytest.fixture
async def client(db_session):
    """Create a test client with database dependency override."""

    async def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
async def sample_project(db_session):
    """Create a sample project for testing."""
    project = Project(
        name="Test Project", description="A test project for integration tests", status=ProjectStatus.ACTIVE
    )
    db_session.add(project)
    await db_session.commit()
    await db_session.refresh(project)
    return project


@pytest.fixture
async def multiple_projects(db_session):
    """Create multiple sample projects for testing."""
    projects = [
        Project(name="Project Alpha", description="First test project", status=ProjectStatus.ACTIVE),
        Project(name="Project Beta", description="Second test project", status=ProjectStatus.INACTIVE),
        Project(name="Project Gamma", description="Third test project", status=ProjectStatus.ARCHIVED),
    ]

    for project in projects:
        db_session.add(project)

    await db_session.commit()

    # Refresh all projects to get their IDs
    for project in projects:
        await db_session.refresh(project)

    return projects


@pytest.fixture
async def sample_dataset(db_session, sample_project):
    """Create a sample dataset for testing."""
    dataset = Dataset(file_path="uploads/test_dataset.csv", is_sample=False, project_id=sample_project.id)
    db_session.add(dataset)
    await db_session.commit()
    await db_session.refresh(dataset)
    return dataset


@pytest.fixture
async def multiple_datasets(db_session, sample_project):
    """Create multiple sample datasets for testing."""
    datasets = [
        Dataset(file_path="uploads/dataset_alpha.csv", is_sample=False, project_id=sample_project.id),
        Dataset(file_path="uploads/dataset_beta.csv", is_sample=True, project_id=sample_project.id),
        Dataset(file_path="uploads/dataset_gamma.csv", is_sample=False, project_id=sample_project.id),
    ]

    for dataset in datasets:
        db_session.add(dataset)

    await db_session.commit()

    # Refresh all datasets to get their IDs
    for dataset in datasets:
        await db_session.refresh(dataset)

    return datasets


@pytest.fixture
async def sample_rule(db_session, sample_project):
    """Create a sample rule for testing."""
    rule = Rule(
        project_id=sample_project.id,
        name="Test Rule",
        description="A test rule for integration tests",
        natural_language_rule="All prices must be positive numbers",
        great_expectations_rule={
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "price", "min_value": 0, "max_value": None},
        },
        type="column_values_between",
    )
    db_session.add(rule)
    await db_session.commit()
    await db_session.refresh(rule)
    return rule


@pytest.fixture
async def multiple_rules(db_session, sample_project):
    """Create multiple sample rules for testing."""
    rules = [
        Rule(
            project_id=sample_project.id,
            name="Rule Alpha",
            description="First test rule",
            natural_language_rule="Categories must be valid",
            great_expectations_rule={"expectation_type": "test1", "kwargs": {}},
            type="type1",
        ),
        Rule(
            project_id=sample_project.id,
            name="Rule Beta",
            description="Second test rule",
            natural_language_rule="Prices must be positive",
            great_expectations_rule={"expectation_type": "test2", "kwargs": {}},
            type="type2",
        ),
        Rule(
            project_id=sample_project.id,
            name="Rule Gamma",
            description="Third test rule",
            natural_language_rule="Names must not be empty",
            great_expectations_rule={"expectation_type": "test3", "kwargs": {}},
            type="type3",
        ),
    ]

    for rule in rules:
        db_session.add(rule)

    await db_session.commit()

    # Refresh all rules to get their IDs
    for rule in rules:
        await db_session.refresh(rule)

    return rules
