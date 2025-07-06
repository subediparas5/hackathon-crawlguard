import json
import asyncio
import random
from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.core.database import AsyncSessionLocal

from app.models.project import Project
from app.models.dataset import Dataset
from app.models.suggested_rules import SuggestedRules
from app.api.v1.endpoints.rules import generate_rules_async


async def generate_and_save_rules_for_project(
    project_id: int, db: AsyncSession, force_regenerate: bool = False
) -> Optional[List[dict]]:
    """
    Generate and save rules for a project when sample data is available.

    Args:
        project_id: The project ID to generate rules for
        db: Database session
        force_regenerate: If True, regenerate rules even if they already exist

    Returns:
        List of generated rules or None if generation failed
    """
    try:
        # Get project details
        project_result = await db.execute(select(Project).where(Project.id == project_id))
        project = project_result.scalar_one_or_none()
        if not project:
            print(f"Project {project_id} not found")
            return None

        # Get sample dataset
        sample_data_result = await db.execute(
            select(Dataset).where(Dataset.project_id == project_id, Dataset.is_sample.is_(True))
        )
        sample_data = sample_data_result.scalar_one_or_none()

        if not sample_data:
            print(f"No sample dataset found for project {project_id}")
            return None

        # Check if rules already exist and we're not forcing regeneration
        if not force_regenerate:
            existing_rules_result = await db.execute(
                select(SuggestedRules)
                .where(SuggestedRules.project_id == project_id)
                .order_by(SuggestedRules.created_at.desc())
                .limit(1)
            )
            existing_rules = existing_rules_result.scalar_one_or_none()
            if existing_rules:
                print(f"Rules already exist for project {project_id}, skipping generation")
                return json.loads(existing_rules.rules)

        # Read sample data
        sample_data_str = ""
        sample_data_columns = None

        try:
            with open(str(sample_data.file_path), encoding="utf-8") as file:
                sample_data_str = file.read()

            # Handle different file types
            file_path = str(sample_data.file_path)
            if file_path.endswith(".csv"):
                # For CSV files: Randomize sample data (header + 10 random rows)
                lines = sample_data_str.splitlines()
                if len(lines) > 11:  # More than header + 10 rows
                    header = lines[0]
                    data_rows = lines[1:]
                    random_rows = random.sample(data_rows, min(10, len(data_rows)))
                    sample_data_str = "\n".join([header] + random_rows)
            elif file_path.endswith(".json"):
                # For JSON files: Sample from array or use single object
                try:
                    json_data = json.loads(sample_data_str)
                    if isinstance(json_data, list):
                        # For JSON arrays, sample up to 10 items
                        if len(json_data) > 10:
                            sample_data_str = json.dumps(random.sample(json_data, 10), indent=2)
                        else:
                            sample_data_str = json.dumps(json_data, indent=2)
                    elif isinstance(json_data, dict):
                        # For single JSON object, use as is
                        sample_data_str = json.dumps(json_data, indent=2)
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON file: {e}")
                    return None

            sample_data_columns = sample_data.columns
        except Exception as e:
            print(f"Error reading sample data file: {e}")
            return None

        print(f"Starting rule generation for project {project_id}")

        # Generate rules asynchronously
        project_description = str(project.description) if project.description else "No description provided"
        suggested_rules = await generate_rules_async(
            project_id, project_description, sample_data_str, sample_data_columns
        )

        if not suggested_rules:
            print(f"No rules generated for project {project_id}, using fallback rules")
            # Use fallback rules
            suggested_rules = [
                {
                    "name": "Data Completeness Check",
                    "description": "Ensure all required fields are populated",
                    "natural_language_rule": "All required fields should not be null",
                    "great_expectations_rule": {
                        "expectation_type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": "required_field"},
                    },
                    "type": "completeness",
                },
                {
                    "name": "Data Type Validation",
                    "description": "Validate data types match expected format",
                    "natural_language_rule": "Data should be in the correct format",
                    "great_expectations_rule": {
                        "expectation_type": "expect_column_values_to_be_of_type",
                        "kwargs": {"column": "data_column", "type_": "string"},
                    },
                    "type": "dtype",
                },
            ]

        # Filter rules based on available columns and valid functions
        if sample_data_columns:
            filtered_rules = []
            for rule in suggested_rules:
                column_name = rule.get("great_expectations_rule", {}).get("kwargs", {}).get("column", "")
                function_name = rule.get("great_expectations_rule", {}).get("expectation_type", "")

                try:
                    with open("great_expectation_functions.json", encoding="utf-8") as file:
                        great_expectation_functions = json.load(file)

                    if function_name not in great_expectation_functions:
                        print(
                            f"Removing rule for column {column_name} because it is not a valid great expectation function"  # noqa: E501
                        )
                        continue

                    if not column_name:
                        continue

                    if column_name not in sample_data_columns:
                        print(f"Removing rule for column {column_name} because it is not in the sample data")
                        continue

                    filtered_rules.append(rule)
                except Exception as e:
                    print(f"Error filtering rule {rule.get('name', 'unknown')}: {e}")
                    continue

            suggested_rules = filtered_rules

        # if already suggested rules exist, dont save new rules
        existing_rules_result = await db.execute(
            select(SuggestedRules)
            .where(SuggestedRules.project_id == project_id)
            .order_by(SuggestedRules.created_at.desc())
            .limit(1)
        )

        existing_rules = existing_rules_result.scalar_one_or_none()
        if existing_rules:
            print(f"Suggested rules already exist for project {project_id}, skipping generation")
            return existing_rules

        # Save rules to database
        suggested_rules_obj = SuggestedRules(project_id=project_id, rules=json.dumps(suggested_rules))
        db.add(suggested_rules_obj)
        await db.commit()
        await db.refresh(suggested_rules_obj)

        print(f"Successfully generated and saved {len(suggested_rules)} rules for project {project_id}")
        return suggested_rules

    except Exception as e:
        print(f"Error generating rules for project {project_id}: {e}")
        await db.rollback()
        return None


async def trigger_rule_generation_for_project(project_id: int, force_regenerate: bool = False) -> None:
    """
    Trigger rule generation for a project as a background task.
    This function can be called without awaiting to avoid blocking the main request.

    Args:
        project_id: The project ID to generate rules for
        force_regenerate: If True, regenerate rules even if they already exist
    """

    async def _run():
        async with AsyncSessionLocal() as db:
            await generate_and_save_rules_for_project(project_id, db, force_regenerate)

    try:
        asyncio.create_task(_run())
        print(f"Triggered background rule generation for project {project_id}")
    except Exception as e:
        print(f"Error triggering rule generation for project {project_id}: {e}")


async def remove_rule_from_suggested_rules(project_id: int, rule_name: str, db: AsyncSession) -> bool:
    """
    Remove a specific rule from the suggested rules list in the database.

    Args:
        project_id: The project ID
        rule_name: The name of the rule to remove
        db: Database session

    Returns:
        True if the rule was found and removed, False otherwise
    """
    try:
        # Get the latest suggested rules for the project
        result = await db.execute(
            select(SuggestedRules)
            .where(SuggestedRules.project_id == project_id)
            .order_by(SuggestedRules.created_at.desc())
            .limit(1)
        )
        suggested_rules_obj = result.scalar_one_or_none()

        if not suggested_rules_obj:
            print(f"No suggested rules found for project {project_id}")
            return False

        # Parse the rules
        rules = json.loads(suggested_rules_obj.rules)

        # Find and remove the rule with matching name
        original_count = len(rules)
        rules = [rule for rule in rules if rule.get("name") != rule_name]

        if len(rules) == original_count:
            print(f"Rule '{rule_name}' not found in suggested rules for project {project_id}")
            return False

        # Update the suggested rules in the database
        suggested_rules_obj.rules = json.dumps(rules)
        await db.commit()

        print(f"Removed rule '{rule_name}' from suggested rules for project {project_id}")
        return True

    except Exception as e:
        print(f"Error removing rule from suggested rules: {e}")
        await db.rollback()
        return False
