import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
class TestRulesAPI:
    """Integration tests for Rules API endpoints."""

    async def test_get_suggested_rules_success(self, client: AsyncClient, sample_project):
        """Test getting suggested rules for a project."""
        response = await client.post(f"/api/v1/projects/{sample_project.id}/rules/suggested-rules")
        print(response.json())
        assert response.status_code == 200
        data = response.json()
        assert "rules" in data
        assert isinstance(data["rules"], list)
        assert len(data["rules"]) > 0

        # Check structure of suggested rules
        for rule in data["rules"]:
            assert "name" in rule
            assert "description" in rule
            assert "natural_language_rule" in rule
            assert "great_expectations_rule" in rule
            assert "type" in rule

    async def test_get_suggested_rules_project_not_found(self, client: AsyncClient):
        """Test getting suggested rules for non-existent project."""
        response = await client.post("/api/v1/projects/999/rules/suggested-rules")
        assert response.status_code == 200  # Should still work as it's a mock endpoint
        data = response.json()
        assert "rules" in data

    async def test_get_rules_empty(self, client: AsyncClient, sample_project):
        """Test getting rules when project has no rules."""
        response = await client.get(f"/api/v1/projects/{sample_project.id}/rules/")
        assert response.status_code == 200
        data = response.json()
        assert data == []

    async def test_get_rules_with_data(self, client: AsyncClient, sample_project, sample_rule):
        """Test getting rules when project has rules."""
        response = await client.get(f"/api/v1/projects/{sample_project.id}/rules/")
        assert response.status_code == 200
        data = response.json()

        assert len(data) == 1
        rule = data[0]
        assert rule["id"] == sample_rule.id
        assert rule["name"] == sample_rule.name
        assert rule["project_id"] == sample_project.id

    async def test_get_rules_project_not_found(self, client: AsyncClient):
        """Test getting rules for non-existent project."""
        response = await client.get("/api/v1/projects/999/rules/")
        assert response.status_code == 200
        data = response.json()
        assert data == []

    async def test_get_rule_by_id_success(self, client: AsyncClient, sample_project, sample_rule):
        """Test getting a specific rule by ID."""
        response = await client.get(f"/api/v1/projects/{sample_project.id}/rules/{sample_rule.id}")
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == sample_rule.id
        assert data["name"] == sample_rule.name
        assert data["project_id"] == sample_project.id
        assert data["natural_language_rule"] == sample_rule.natural_language_rule
        assert data["type"] == sample_rule.type

    async def test_get_rule_by_id_not_found(self, client: AsyncClient, sample_project):
        """Test getting a rule that doesn't exist."""
        response = await client.get(f"/api/v1/projects/{sample_project.id}/rules/999")
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Rule not found"

    async def test_get_rule_by_id_wrong_project(self, client: AsyncClient, sample_project, sample_rule):
        """Test getting a rule with wrong project ID."""
        response = await client.get(f"/api/v1/projects/999/rules/{sample_rule.id}")
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Rule not found"

    async def test_add_rule_success(self, client: AsyncClient, sample_project):
        """Test adding a new rule via prompt."""
        rule_data = {
            "project_id": sample_project.id,
            "prompt": "Validate that all prices are positive numbers",
            "note": "This rule ensures data quality for pricing",
        }

        response = await client.post(f"/api/v1/projects/{sample_project.id}/rules/add-rule", json=rule_data)
        assert response.status_code == 201
        data = response.json()

        assert "rule_id" in data
        assert isinstance(data["rule_id"], int)

    async def test_add_rule_project_not_found(self, client: AsyncClient):
        """Test adding a rule to non-existent project."""
        rule_data = {
            "project_id": 999,
            "prompt": "Validate that all prices are positive numbers",
            "note": "This rule ensures data quality for pricing",
        }

        response = await client.post("/api/v1/projects/999/rules/add-rule", json=rule_data)
        assert response.status_code == 201  # Should still work as it creates the rule

    async def test_add_rule_empty_prompt(self, client: AsyncClient, sample_project):
        """Test adding a rule with empty prompt."""
        rule_data = {"project_id": sample_project.id, "prompt": "", "note": "Empty prompt test"}

        response = await client.post(f"/api/v1/projects/{sample_project.id}/rules/add-rule", json=rule_data)
        assert response.status_code == 422  # Validation error

    async def test_create_rule_success(self, client: AsyncClient, sample_project):
        """Test creating a new rule with full data."""
        rule_data = {
            "name": "Price Validation Rule",
            "description": "Validates that prices are positive numbers",
            "natural_language_rule": "All prices must be greater than zero",
            "great_expectations_rule": {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "price", "min_value": 0, "max_value": None},
            },
            "type": "column_values_between",
        }

        response = await client.post(f"/api/v1/projects/{sample_project.id}/rules/", json=rule_data)
        assert response.status_code == 201
        data = response.json()

        assert data["name"] == rule_data["name"]
        assert data["description"] == rule_data["description"]
        assert data["natural_language_rule"] == rule_data["natural_language_rule"]
        assert data["type"] == rule_data["type"]
        assert data["project_id"] == sample_project.id
        assert "id" in data
        assert "created_at" in data
        assert "updated_at" in data

    async def test_create_rule_project_not_found(self, client: AsyncClient):
        """Test creating a rule for non-existent project."""
        rule_data = {
            "name": "Price Validation Rule",
            "description": "Validates that prices are positive numbers",
            "natural_language_rule": "All prices must be greater than zero",
            "great_expectations_rule": {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "price", "min_value": 0, "max_value": None},
            },
            "type": "column_values_between",
        }

        response = await client.post("/api/v1/projects/999/rules/", json=rule_data)
        assert response.status_code == 201  # Should still work as it creates the rule

    async def test_create_rule_invalid_data(self, client: AsyncClient, sample_project):
        """Test creating a rule with invalid data."""
        rule_data = {
            "name": "",  # Empty name
            "description": "Validates that prices are positive numbers",
            "natural_language_rule": "All prices must be greater than zero",
            "great_expectations_rule": {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "price", "min_value": 0, "max_value": None},
            },
            "type": "column_values_between",
        }

        response = await client.post(f"/api/v1/projects/{sample_project.id}/rules/", json=rule_data)
        assert response.status_code == 422  # Validation error

    async def test_update_rule_success(self, client: AsyncClient, sample_project, sample_rule):
        """Test updating a rule successfully."""
        update_data = {
            "name": "Updated Rule Name",
            "description": "Updated description",
            "natural_language_rule": "Updated natural language rule",
            "type": "updated_type",
        }

        response = await client.put(f"/api/v1/projects/{sample_project.id}/rules/{sample_rule.id}", json=update_data)
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == sample_rule.id
        assert data["name"] == update_data["name"]
        assert data["description"] == update_data["description"]
        assert data["natural_language_rule"] == update_data["natural_language_rule"]
        assert data["type"] == update_data["type"]

    async def test_update_rule_partial(self, client: AsyncClient, sample_project, sample_rule):
        """Test updating only some fields of a rule."""
        update_data = {"description": "Only description updated"}

        response = await client.put(f"/api/v1/projects/{sample_project.id}/rules/{sample_rule.id}", json=update_data)
        assert response.status_code == 200
        data = response.json()

        assert data["id"] == sample_rule.id
        assert data["name"] == sample_rule.name  # Unchanged
        assert data["description"] == update_data["description"]  # Updated
        assert data["natural_language_rule"] == sample_rule.natural_language_rule  # Unchanged

    async def test_update_rule_not_found(self, client: AsyncClient, sample_project):
        """Test updating a rule that doesn't exist."""
        update_data = {"name": "Updated Name", "description": "Updated description"}

        response = await client.put(f"/api/v1/projects/{sample_project.id}/rules/999", json=update_data)
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Rule not found"

    async def test_update_rule_wrong_project(self, client: AsyncClient, sample_project, sample_rule):
        """Test updating a rule with wrong project ID."""
        update_data = {"name": "Updated Name", "description": "Updated description"}

        response = await client.put(f"/api/v1/projects/999/rules/{sample_rule.id}", json=update_data)
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Rule not found"

    async def test_delete_rule_success(self, client: AsyncClient, sample_project, sample_rule):
        """Test deleting a rule successfully."""
        response = await client.delete(f"/api/v1/projects/{sample_project.id}/rules/{sample_rule.id}")
        assert response.status_code == 204

        # Verify rule is deleted
        get_response = await client.get(f"/api/v1/projects/{sample_project.id}/rules/{sample_rule.id}")
        assert get_response.status_code == 404

    async def test_delete_rule_not_found(self, client: AsyncClient, sample_project):
        """Test deleting a rule that doesn't exist."""
        response = await client.delete(f"/api/v1/projects/{sample_project.id}/rules/999")
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Rule not found"

    async def test_delete_rule_wrong_project(self, client: AsyncClient, sample_project, sample_rule):
        """Test deleting a rule with wrong project ID."""
        response = await client.delete(f"/api/v1/projects/999/rules/{sample_rule.id}")
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Rule not found"

    async def test_rule_created_at_updated_at(self, client: AsyncClient, sample_project):
        """Test that created_at and updated_at fields are properly set."""
        rule_data = {
            "name": "Timestamp Test Rule",
            "description": "Testing timestamps",
            "natural_language_rule": "Test rule for timestamp validation",
            "great_expectations_rule": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "test_column"},
            },
            "type": "column_values_not_null",
        }

        # Create rule
        create_response = await client.post(f"/api/v1/projects/{sample_project.id}/rules/", json=rule_data)
        assert create_response.status_code == 201
        created_data = create_response.json()

        assert "created_at" in created_data
        assert "updated_at" in created_data
        assert created_data["created_at"] == created_data["updated_at"]

        # Update rule
        update_data = {"description": "Updated description"}
        update_response = await client.put(
            f"/api/v1/projects/{sample_project.id}/rules/{created_data['id']}", json=update_data
        )
        assert update_response.status_code == 200
        updated_data = update_response.json()

        # updated_at should be different from created_at after update
        assert updated_data["created_at"] == created_data["created_at"]
        assert updated_data["updated_at"] != created_data["updated_at"]

    async def test_multiple_rules_for_project(self, client: AsyncClient, sample_project):
        """Test creating and managing multiple rules for a project."""
        rules_data = [
            {
                "name": "Rule 1",
                "description": "First rule",
                "natural_language_rule": "First natural language rule",
                "great_expectations_rule": {"expectation_type": "test1", "kwargs": {}},
                "type": "type1",
            },
            {
                "name": "Rule 2",
                "description": "Second rule",
                "natural_language_rule": "Second natural language rule",
                "great_expectations_rule": {"expectation_type": "test2", "kwargs": {}},
                "type": "type2",
            },
            {
                "name": "Rule 3",
                "description": "Third rule",
                "natural_language_rule": "Third natural language rule",
                "great_expectations_rule": {"expectation_type": "test3", "kwargs": {}},
                "type": "type3",
            },
        ]

        # Create multiple rules
        rule_ids = []
        for rule_data in rules_data:
            response = await client.post(f"/api/v1/projects/{sample_project.id}/rules/", json=rule_data)
            assert response.status_code == 201
            data = response.json()
            rule_ids.append(data["id"])

        # Get all rules
        response = await client.get(f"/api/v1/projects/{sample_project.id}/rules/")
        assert response.status_code == 200
        data = response.json()

        assert len(data) == 3

        # Verify all rules belong to the correct project
        for rule in data:
            assert rule["project_id"] == sample_project.id
            assert rule["id"] in rule_ids
