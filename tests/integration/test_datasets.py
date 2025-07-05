import pytest
import io
from httpx import AsyncClient


@pytest.mark.asyncio
class TestDatasetsAPI:
    """Integration tests for Datasets API endpoints."""

    async def test_get_datasets_empty(self, client: AsyncClient):
        """Test getting datasets when none exist."""
        response = await client.get("/api/v1/datasets/")
        assert response.status_code == 200
        assert response.json() == []

    async def test_get_datasets_with_data(self, client: AsyncClient, sample_project, multiple_datasets):
        """Test getting datasets with existing data."""
        response = await client.get("/api/v1/datasets/")
        assert response.status_code == 200
        datasets = response.json()
        assert len(datasets) == 3
        assert all("id" in dataset for dataset in datasets)
        assert all("project_id" in dataset for dataset in datasets)
        assert all("created_at" in dataset for dataset in datasets)

    async def test_get_datasets_filtered_by_project(self, client: AsyncClient, sample_project, multiple_datasets):
        """Test getting datasets filtered by project_id."""
        response = await client.get(f"/api/v1/datasets/?project_id={sample_project.id}")
        assert response.status_code == 200
        datasets = response.json()
        assert len(datasets) == 3
        assert all(dataset["project_id"] == sample_project.id for dataset in datasets)

    async def test_get_dataset_by_id_success(self, client: AsyncClient, sample_dataset):
        """Test getting a specific dataset by ID."""
        response = await client.get(f"/api/v1/datasets/{sample_dataset.id}")
        assert response.status_code == 200
        dataset = response.json()
        assert dataset["id"] == sample_dataset.id
        assert dataset["project_id"] == sample_dataset.project_id
        assert "created_at" in dataset

    async def test_get_dataset_by_id_not_found(self, client: AsyncClient):
        """Test getting a dataset that doesn't exist."""
        response = await client.get("/api/v1/datasets/999")
        assert response.status_code == 404
        assert "Dataset not found" in response.json()["detail"]

    async def test_upload_sample_dataset_success(self, client: AsyncClient, sample_project):
        """Test uploading a sample dataset."""
        file_content = b"name,age,city\nJohn,25,NYC\nJane,30,LA"
        files = {"file": ("sample.csv", io.BytesIO(file_content), "text/csv")}

        response = await client.post(f"/api/v1/datasets/upload-sample?project_id={sample_project.id}", files=files)
        assert response.status_code == 201
        dataset = response.json()
        assert dataset["is_sample"] is True
        assert dataset["project_id"] == sample_project.id
        assert "created_at" in dataset
        assert "updated_at" in dataset
        assert dataset["file_path"].endswith("sample.csv")
        # Check that columns were extracted from CSV
        assert dataset["columns"] == ["name", "age", "city"]

    async def test_upload_sample_dataset_project_not_found(self, client: AsyncClient):
        """Test uploading a sample dataset to non-existent project."""
        file_content = b"sample dataset content"
        files = {"file": ("sample.csv", io.BytesIO(file_content), "text/csv")}

        response = await client.post("/api/v1/datasets/upload-sample?project_id=999", files=files)
        assert response.status_code == 404
        assert "Project not found" in response.json()["detail"]

    async def test_upload_sample_dataset_duplicate(self, client: AsyncClient, sample_project):
        """Test uploading a second sample dataset to the same project."""
        # First, create a sample dataset
        file_content = b"first sample dataset content"
        files = {"file": ("first_sample.csv", io.BytesIO(file_content), "text/csv")}

        first_response = await client.post(
            f"/api/v1/datasets/upload-sample?project_id={sample_project.id}", files=files
        )
        assert first_response.status_code == 201

        # Now try to upload a second sample dataset
        file_content2 = b"another sample dataset content"
        files2 = {"file": ("another_sample.csv", io.BytesIO(file_content2), "text/csv")}

        response = await client.post(f"/api/v1/datasets/upload-sample?project_id={sample_project.id}", files=files2)
        assert response.status_code == 400
        assert "sample dataset already exists for this project" in response.json()["detail"]

    async def test_upload_sample_dataset_no_file(self, client: AsyncClient, sample_project):
        """Test uploading a sample dataset without a file."""
        response = await client.post(f"/api/v1/datasets/upload-sample?project_id={sample_project.id}")
        assert response.status_code == 422  # Validation error

    async def test_create_dataset_success(self, client: AsyncClient, sample_project):
        """Test creating a regular dataset."""
        file_content = b"regular dataset content"
        files = {"file": ("regular.csv", io.BytesIO(file_content), "text/csv")}

        response = await client.post(f"/api/v1/datasets/?project_id={sample_project.id}", files=files)
        assert response.status_code == 201
        dataset = response.json()
        assert dataset["is_sample"] is False
        assert dataset["project_id"] == sample_project.id
        assert "created_at" in dataset
        assert dataset["file_path"].endswith("regular.csv")

    async def test_create_dataset_project_not_found(self, client: AsyncClient):
        """Test creating a dataset for non-existent project."""
        file_content = b"dataset content"
        files = {"file": ("test.csv", io.BytesIO(file_content), "text/csv")}

        response = await client.post("/api/v1/datasets/?project_id=999", files=files)
        assert response.status_code == 404
        assert "Project not found" in response.json()["detail"]

    async def test_delete_dataset_success(self, client: AsyncClient, sample_dataset):
        """Test deleting a dataset."""
        response = await client.delete(f"/api/v1/datasets/{sample_dataset.id}")
        assert response.status_code == 204

        # Verify dataset is deleted
        get_response = await client.get(f"/api/v1/datasets/{sample_dataset.id}")
        assert get_response.status_code == 404

    async def test_delete_dataset_not_found(self, client: AsyncClient):
        """Test deleting a dataset that doesn't exist."""
        response = await client.delete("/api/v1/datasets/999")
        assert response.status_code == 404
        assert "Dataset not found" in response.json()["detail"]

    async def test_dataset_created_at_updated_at(self, client: AsyncClient, sample_project):
        """Test that created_at and updated_at fields are properly set."""
        file_content = b"timestamp test content"
        files = {"file": ("timestamp.csv", io.BytesIO(file_content), "text/csv")}

        # Create dataset
        create_response = await client.post(f"/api/v1/datasets/?project_id={sample_project.id}", files=files)
        assert create_response.status_code == 201
        created_data = create_response.json()

        assert "created_at" in created_data
        assert "updated_at" in created_data
        assert created_data["created_at"] == created_data["updated_at"]

        # Update dataset
        update_data = {"is_sample": True}
        update_response = await client.put(f"/api/v1/datasets/{created_data['id']}", json=update_data)
        assert update_response.status_code == 200
        updated_data = update_response.json()

        # updated_at should be different from created_at after update
        assert updated_data["created_at"] == created_data["created_at"]
        assert updated_data["updated_at"] != created_data["updated_at"]

    async def test_upload_sample_dataset_non_csv(self, client: AsyncClient, sample_project):
        """Test uploading a sample dataset with non-CSV file."""
        file_content = b"some binary content"
        files = {"file": ("sample.txt", io.BytesIO(file_content), "text/plain")}

        response = await client.post(f"/api/v1/datasets/upload-sample?project_id={sample_project.id}", files=files)
        assert response.status_code == 201
        dataset = response.json()
        assert dataset["is_sample"] is True
        assert dataset["project_id"] == sample_project.id
        # Check that columns is None for non-CSV files
        assert dataset["columns"] is None

    async def test_update_dataset_success(self, client: AsyncClient, sample_dataset):
        """Test updating a dataset."""
        update_data = {"is_sample": True}
        response = await client.put(f"/api/v1/datasets/{sample_dataset.id}", json=update_data)
        assert response.status_code == 200
        updated_dataset = response.json()
        assert updated_dataset["is_sample"] is True
        assert updated_dataset["id"] == sample_dataset.id

    async def test_update_dataset_not_found(self, client: AsyncClient):
        """Test updating a dataset that doesn't exist."""
        update_data = {"is_sample": True}
        response = await client.put("/api/v1/datasets/999", json=update_data)
        assert response.status_code == 404
        assert "Dataset not found" in response.json()["detail"]

    async def test_update_dataset_file_path(self, client: AsyncClient, sample_dataset):
        """Test updating a dataset's file path."""
        update_data = {"file_path": "uploads/updated_file.csv"}
        response = await client.put(f"/api/v1/datasets/{sample_dataset.id}", json=update_data)
        assert response.status_code == 200
        updated_dataset = response.json()
        assert updated_dataset["file_path"] == "uploads/updated_file.csv"
