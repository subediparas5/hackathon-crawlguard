import pytest
from unittest.mock import AsyncMock, patch

from app.core.rule_generator import (
    generate_and_save_rules_for_project,
    trigger_rule_generation_for_project,
    remove_rule_from_suggested_rules,
)


@pytest.mark.asyncio
async def test_generate_and_save_rules_for_project_success():
    """Test successful rule generation and saving"""
    # Mock database session
    mock_db = AsyncMock()

    # Mock project
    mock_project = AsyncMock()
    mock_project.description = "Test project description"

    # Mock sample dataset
    mock_sample_data = AsyncMock()
    mock_sample_data.file_path = "test_file.csv"
    mock_sample_data.columns = ["name", "age", "city"]

    # Mock existing rules query result
    mock_existing_rules = None

    # Mock database queries
    mock_db.execute.side_effect = [
        AsyncMock(scalar_one_or_none=lambda: mock_project),  # Project query
        AsyncMock(scalar_one_or_none=lambda: mock_sample_data),  # Sample data query
        AsyncMock(scalar_one_or_none=lambda: mock_existing_rules),  # Existing rules query
    ]

    # Mock file reading
    with patch("builtins.open", create=True) as mock_open:
        mock_open.return_value.__enter__.return_value.read.return_value = "name,age,city\nJohn,25,NYC\nJane,30,LA"

        # Mock rule generation
        with patch("app.core.rule_generator.generate_rules_async") as mock_generate:
            mock_generate.return_value = [
                {
                    "name": "Test Rule",
                    "description": "Test description",
                    "natural_language_rule": "Test rule",
                    "great_expectations_rule": {
                        "expectation_type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": "name"},
                    },
                    "type": "completeness",
                }
            ]

            # Mock great expectations functions file
            with patch("builtins.open", create=True) as mock_ge_file:
                mock_ge_file.return_value.__enter__.return_value.read.return_value = (
                    '{"expect_column_values_to_not_be_null": {}}'
                )

                result = await generate_and_save_rules_for_project(1, mock_db, force_regenerate=False)

                assert result is not None
                assert len(result) == 1
                assert result[0]["name"] == "Test Rule"

                # Verify database operations were called
                mock_db.add.assert_called_once()
                mock_db.commit.assert_called_once()
                mock_db.refresh.assert_called_once()


@pytest.mark.asyncio
async def test_generate_and_save_rules_for_project_no_sample_data():
    """Test rule generation when no sample data exists"""
    mock_db = AsyncMock()

    # Mock project
    mock_project = AsyncMock()
    mock_project.description = "Test project description"

    # Mock no sample data
    mock_db.execute.side_effect = [
        AsyncMock(scalar_one_or_none=lambda: mock_project),  # Project query
        AsyncMock(scalar_one_or_none=lambda: None),  # No sample data
    ]

    result = await generate_and_save_rules_for_project(1, mock_db, force_regenerate=False)

    assert result is None


@pytest.mark.asyncio
async def test_generate_and_save_rules_for_project_existing_rules():
    """Test rule generation when rules already exist"""
    mock_db = AsyncMock()

    # Mock project
    mock_project = AsyncMock()
    mock_project.description = "Test project description"

    # Mock existing rules
    mock_existing_rules = AsyncMock()
    mock_existing_rules.rules = '[{"name": "Existing Rule"}]'

    # Mock database queries
    mock_db.execute.side_effect = [
        AsyncMock(scalar_one_or_none=lambda: mock_project),  # Project query
        AsyncMock(scalar_one_or_none=lambda: mock_existing_rules),  # Existing rules query
    ]

    result = await generate_and_save_rules_for_project(1, mock_db, force_regenerate=False)

    assert result is not None
    assert len(result) == 1
    assert result[0]["name"] == "Existing Rule"


@pytest.mark.asyncio
async def test_trigger_rule_generation_for_project():
    """Test triggering rule generation as background task"""
    mock_db = AsyncMock()

    with patch("asyncio.create_task") as mock_create_task:
        await trigger_rule_generation_for_project(1, mock_db, force_regenerate=True)

        # Verify that create_task was called
        mock_create_task.assert_called_once()


@pytest.mark.asyncio
async def test_remove_rule_from_suggested_rules_success():
    """Test successfully removing a rule from suggested rules"""
    mock_db = AsyncMock()

    # Mock suggested rules object
    mock_suggested_rules = AsyncMock()
    mock_suggested_rules.rules = '[{"name": "Rule 1"}, {"name": "Rule 2"}, {"name": "Rule 3"}]'

    # Mock database query
    mock_db.execute.return_value = AsyncMock(scalar_one_or_none=lambda: mock_suggested_rules)

    result = await remove_rule_from_suggested_rules(1, "Rule 2", mock_db)

    assert result is True
    # Verify the rules were updated (Rule 2 should be removed)
    assert '{"name": "Rule 2"}' not in mock_suggested_rules.rules
    assert '{"name": "Rule 1"}' in mock_suggested_rules.rules
    assert '{"name": "Rule 3"}' in mock_suggested_rules.rules
    mock_db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_remove_rule_from_suggested_rules_not_found():
    """Test removing a rule that doesn't exist in suggested rules"""
    mock_db = AsyncMock()

    # Mock suggested rules object
    mock_suggested_rules = AsyncMock()
    mock_suggested_rules.rules = '[{"name": "Rule 1"}, {"name": "Rule 2"}]'

    # Mock database query
    mock_db.execute.return_value = AsyncMock(scalar_one_or_none=lambda: mock_suggested_rules)

    result = await remove_rule_from_suggested_rules(1, "Non-existent Rule", mock_db)

    assert result is False
    # Verify the rules were not changed
    assert mock_suggested_rules.rules == '[{"name": "Rule 1"}, {"name": "Rule 2"}]'
    mock_db.commit.assert_not_called()


@pytest.mark.asyncio
async def test_remove_rule_from_suggested_rules_no_suggested_rules():
    """Test removing a rule when no suggested rules exist"""
    mock_db = AsyncMock()

    # Mock no suggested rules
    mock_db.execute.return_value = AsyncMock(scalar_one_or_none=lambda: None)

    result = await remove_rule_from_suggested_rules(1, "Any Rule", mock_db)

    assert result is False
    mock_db.commit.assert_not_called()
