import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestProjectsAPI:
    """Integration tests for Projects API endpoints."""

    async def test_get_projects_empty(self, client: AsyncClient):
        """Test getting projects when database is empty."""
        response = await client.get("/api/v1/projects/")
        assert response.status_code == 200
        data = response.json()
        assert data == []

    async def test_get_projects_with_data(self, client: AsyncClient, multiple_projects):
        """Test getting all projects."""
        response = await client.get("/api/v1/projects/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3

        # Check that all projects are returned
        project_names = [project["name"] for project in data]
        assert "Project Alpha" in project_names
        assert "Project Beta" in project_names
        assert "Project Gamma" in project_names

    async def test_get_project_by_id_success(self, client: AsyncClient, sample_project):
        """Test getting a specific project by ID."""
        response = await client.get(f"/api/v1/projects/{sample_project.id}")
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == sample_project.id
        assert data["name"] == sample_project.name
        assert data["description"] == sample_project.description
        assert data["status"] == sample_project.status.value

    async def test_get_project_by_id_not_found(self, client: AsyncClient):
        """Test getting a project that doesn't exist."""
        response = await client.get("/api/v1/projects/999")
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Project not found"

    async def test_create_project_success(self, client: AsyncClient):
        """Test creating a new project successfully."""
        project_data = {"name": "New Test Project", "description": "A new test project", "status": "ACTIVE"}

        response = await client.post("/api/v1/projects/", json=project_data)
        assert response.status_code == 201
        data = response.json()

        assert data["name"] == project_data["name"]
        assert data["description"] == project_data["description"]
        assert data["status"] == project_data["status"]
        assert "id" in data
        assert "created_at" in data
        assert "updated_at" in data

    async def test_create_project_without_description(self, client: AsyncClient):
        """Test creating a project without description."""
        project_data = {"name": "Project Without Description", "status": "ACTIVE"}

        response = await client.post("/api/v1/projects/", json=project_data)
        assert response.status_code == 201
        data = response.json()

        assert data["name"] == project_data["name"]
        assert data["description"] is None
        assert data["status"] == project_data["status"]

    async def test_create_project_duplicate_name(self, client: AsyncClient, sample_project):
        """Test creating a project with duplicate name."""
        project_data = {
            "name": sample_project.name,  # Use existing project name
            "description": "Another project with same name",
            "status": "ACTIVE",
        }

        response = await client.post("/api/v1/projects/", json=project_data)
        assert response.status_code == 400
        data = response.json()
        assert data["detail"] == "Project with this name already exists"

    async def test_create_project_invalid_status(self, client: AsyncClient):
        """Test creating a project with invalid status."""
        project_data = {
            "name": "Invalid Status Project",
            "description": "Project with invalid status",
            "status": "invalid_status",
        }

        response = await client.post("/api/v1/projects/", json=project_data)
        assert response.status_code == 422  # Validation error

    async def test_create_project_empty_name(self, client: AsyncClient):
        """Test creating a project with empty name."""
        project_data = {"name": "", "description": "Project with empty name", "status": "ACTIVE"}

        response = await client.post("/api/v1/projects/", json=project_data)
        assert response.status_code == 422  # Validation error

    async def test_update_project_success(self, client: AsyncClient, sample_project):
        """Test updating a project successfully."""
        update_data = {"name": "Updated Project Name", "description": "Updated description", "status": "INACTIVE"}

        response = await client.put(f"/api/v1/projects/{sample_project.id}", json=update_data)
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == sample_project.id
        assert data["name"] == update_data["name"]
        assert data["description"] == update_data["description"]
        assert data["status"] == update_data["status"]

    async def test_update_project_partial(self, client: AsyncClient, sample_project):
        """Test updating only some fields of a project."""
        update_data = {"description": "Only description updated"}

        response = await client.put(f"/api/v1/projects/{sample_project.id}", json=update_data)
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == sample_project.id
        assert data["name"] == sample_project.name  # Unchanged
        assert data["description"] == update_data["description"]  # Updated
        assert data["status"] == sample_project.status.value  # Unchanged

    async def test_update_project_not_found(self, client: AsyncClient):
        """Test updating a project that doesn't exist."""
        update_data = {"name": "Updated Name", "description": "Updated description"}

        response = await client.put("/api/v1/projects/999", json=update_data)
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Project not found"

    async def test_delete_project_success(self, client: AsyncClient, sample_project):
        """Test deleting a project successfully."""
        response = await client.delete(f"/api/v1/projects/{sample_project.id}")
        assert response.status_code == 204

        # Verify project is deleted
        get_response = await client.get(f"/api/v1/projects/{sample_project.id}")
        assert get_response.status_code == 404

    async def test_delete_project_not_found(self, client: AsyncClient):
        """Test deleting a project that doesn't exist."""
        response = await client.delete("/api/v1/projects/999")
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Project not found"

    async def test_project_status_enum_values(self, client: AsyncClient):
        """Test that all project status enum values work."""
        statuses = ["ACTIVE", "INACTIVE", "ARCHIVED"]

        for status in statuses:
            project_data = {"name": f"Project with status {status}", "status": status}

            response = await client.post("/api/v1/projects/", json=project_data)
            assert response.status_code == 201
            data = response.json()
            assert data["status"] == status

    async def test_project_created_at_updated_at(self, client: AsyncClient):
        """Test that created_at and updated_at fields are properly set."""
        project_data = {"name": "Timestamp Test Project", "description": "Testing timestamps"}

        # Create project
        create_response = await client.post("/api/v1/projects/", json=project_data)
        assert create_response.status_code == 201
        created_data = create_response.json()

        assert "created_at" in created_data
        assert "updated_at" in created_data
        assert created_data["created_at"] == created_data["updated_at"]

        # Update project
        update_data = {"description": "Updated description"}
        update_response = await client.put(f"/api/v1/projects/{created_data['id']}", json=update_data)
        assert update_response.status_code == 200
        updated_data = update_response.json()

        # updated_at should be different from created_at after update
        assert updated_data["created_at"] == created_data["created_at"]
        assert updated_data["updated_at"] != created_data["updated_at"]
