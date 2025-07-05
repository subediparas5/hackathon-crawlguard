import json
import asyncio
from app.core.prompts import DeepSeekRuleGenerator, PromptToRule
from app.models.dataset import Dataset
from fastapi import APIRouter, Depends, HTTPException, status, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
from datetime import datetime, timezone

from app.core.database import get_db
from app.models.project import Project
from app.models.rule import Rule
from app.models.suggested_rules import SuggestedRules
from app.schemas.rule import (
    RuleBase,
    RuleResponse,
    RuleUpdate,
    SuggestedRulesResponse,
    AddRuleRequest,
    AddRuleResponse,
)
import pandas as pd

router = APIRouter()


async def generate_rules_async(
    project_id: int, project_description: str, sample_data_str: str = "", sample_data_columns: List[str] | None = None
):
    """Asynchronously generate rules using AI"""
    try:
        rule_generator = DeepSeekRuleGenerator(project_description=project_description)

        # Run AI calls concurrently
        project_description_task = asyncio.create_task(
            asyncio.to_thread(rule_generator.get_suggested_rules_from_project_description)
        )

        sample_data_task = None
        if sample_data_str:
            sample_data_task = asyncio.create_task(
                asyncio.to_thread(
                    rule_generator.get_suggested_rules_from_sample_data,
                    sample_data_str,
                    {"columns": sample_data_columns},
                )
            )

        # Wait for both tasks to complete
        project_description_rule_str = await project_description_task
        print(f"Project description response: {project_description_rule_str[:200]}...")

        if sample_data_task:
            sample_data_rule_str = await sample_data_task
            print(f"Sample data response: {sample_data_rule_str[:200]}...")
        else:
            sample_data_rule_str = ""

        # Parse results with better error handling
        project_description_rules = []
        if project_description_rule_str and project_description_rule_str.strip():
            try:
                project_description_rules = json.loads(project_description_rule_str)
            except json.JSONDecodeError as e:
                print(f"Failed to parse project description rules JSON: {e}")
                print(f"Raw response: {project_description_rule_str}")

        sample_data_rules = []
        if sample_data_rule_str and sample_data_rule_str.strip():
            try:
                sample_data_rules = json.loads(sample_data_rule_str)
            except json.JSONDecodeError as e:
                print(f"Failed to parse sample data rules JSON: {e}")
                print(f"Raw response: {sample_data_rule_str}")

        # Merge rules
        suggested_rules = project_description_rules + sample_data_rules

        return suggested_rules
    except Exception as e:
        print(f"Error generating rules: {e}")
        return []


@router.get("/suggested-rules", response_model=SuggestedRulesResponse)
@router.post("/suggested-rules", response_model=SuggestedRulesResponse)
async def get_suggested_rules(project_id: int, db: AsyncSession = Depends(get_db)):
    """Get suggested rules for a project"""

    project = await db.execute(select(Project).where(Project.id == project_id))
    project = project.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    # check database for previously suggested rules, if available on db return that else generate new rules
    result = await db.execute(
        select(SuggestedRules)
        .where(SuggestedRules.project_id == project_id)
        .order_by(SuggestedRules.created_at.desc())
        .limit(1)
    )
    suggested_rules = result.scalar_one_or_none()
    if suggested_rules:
        return SuggestedRulesResponse(rules=json.loads(suggested_rules.rules))

    # get the sample data for the project
    sample_data = await db.execute(select(Dataset).where(Dataset.project_id == project_id, Dataset.is_sample.is_(True)))
    sample_data = sample_data.scalar_one_or_none()

    sample_data_str = ""
    sample_data_columns = None

    if sample_data:
        with open(str(sample_data.file_path), encoding="utf-8") as file:
            sample_data_str: str = file.read()

        # Randomize sample data (header + 10 random rows)
        lines = sample_data_str.splitlines()
        if len(lines) > 11:  # More than header + 10 rows
            import random

            header = lines[0]
            data_rows = lines[1:]
            random_rows = random.sample(data_rows, min(10, len(data_rows)))
            sample_data_str = "\n".join([header] + random_rows)

        sample_data_columns = sample_data.columns

    print("Starting to generate rules")

    # Generate rules asynchronously
    project_description = project.description if project.description else "No description provided"
    suggested_rules = await generate_rules_async(project_id, project_description, sample_data_str, sample_data_columns)

    print("Rules generated")
    print(suggested_rules)

    if not suggested_rules:
        print("No rules generated, returning mock rules")
        # Return mock rules as fallback
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

    # if random chatgpt generated rules for random columns
    for rule in suggested_rules:
        column_name = rule.get("great_expectations_rule", {}).get("kwargs", {}).get("column", "")
        if not column_name:
            continue

        if column_name not in sample_data_columns:
            print(f"Removing rule for column {column_name} because it is not in the sample data")
            suggested_rules.remove(rule)

    # save the rules to the database
    suggested_rules_obj = SuggestedRules(project_id=project_id, rules=json.dumps(suggested_rules))
    db.add(suggested_rules_obj)
    await db.commit()
    await db.refresh(suggested_rules_obj)

    return SuggestedRulesResponse(rules=suggested_rules)


@router.post("/prompt-to-rules", response_model=SuggestedRulesResponse)
async def prompt_to_rules(project_id: int = Path(...), prompt: str = "", db: AsyncSession = Depends(get_db)):
    """Convert a natural language prompt to rules"""

    # get the sample data for the project
    sample_data = await db.execute(select(Dataset).where(Dataset.project_id == project_id, Dataset.is_sample.is_(True)))
    sample_data = sample_data.scalar_one_or_none()
    if not sample_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sample data not found")

    # Read the actual file content
    with open(str(sample_data.file_path), encoding="utf-8") as file:
        df = pd.read_csv(file)

    # Check if DataFrame has data before sampling
    if len(df) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Sample data file is empty")

    sample_size = min(100, len(df))
    sample_df = df.sample(n=sample_size, random_state=42)

    sample_data_str = sample_df.to_csv(index=False)

    rule_generator = PromptToRule(sample_data_str)
    response = rule_generator.get_suggested_rules(user_prompt=prompt)

    return SuggestedRulesResponse(rules=response)


@router.get("/", response_model=List[RuleResponse])
async def get_rules(project_id: int = Path(...), db: AsyncSession = Depends(get_db)):
    """Get all rules for a specific project"""
    result = await db.execute(select(Rule).where(Rule.project_id == project_id))
    return result.scalars().all()


@router.get("/{rule_id}", response_model=RuleResponse)
async def get_rule_by_id(project_id: int = Path(...), rule_id: int = Path(...), db: AsyncSession = Depends(get_db)):
    """Get a specific rule by ID"""
    result = await db.execute(select(Rule).where(Rule.id == rule_id, Rule.project_id == project_id))
    rule = result.scalar_one_or_none()

    if not rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")

    return rule


@router.post("/add-rule", response_model=AddRuleResponse, status_code=status.HTTP_201_CREATED)
async def add_rule(request: AddRuleRequest, project_id: int = Path(...), db: AsyncSession = Depends(get_db)):
    """Add a new rule to a project"""
    # Use project_id from path
    rule_name = f"Rule from prompt: {request.prompt[:50]}..."
    rule_type = "custom_validation"

    # Mock Great Expectations rule structure
    great_expectations_rule = {
        "expectation_type": "expect_column_values_to_match_regex",
        "kwargs": {"column": "data_column", "regex": ".*"},
    }

    # Create the rule
    db_rule = Rule(
        project_id=project_id,
        name=rule_name,
        description=request.note,
        natural_language_rule=request.prompt,
        great_expectations_rule=great_expectations_rule,
        type=rule_type,
    )

    db.add(db_rule)
    await db.commit()
    await db.refresh(db_rule)

    return AddRuleResponse(rule_id=db_rule.id)


@router.post("/", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
async def create_rule(rule: RuleBase, project_id: int = Path(...), db: AsyncSession = Depends(get_db)):
    """Create a new rule"""
    # Use project_id from path, ignore any in body
    db_rule = Rule(
        project_id=project_id,
        name=rule.name,
        description=rule.description,
        natural_language_rule=rule.natural_language_rule,
        great_expectations_rule=rule.great_expectations_rule,
        type=rule.type,
    )

    db.add(db_rule)
    await db.commit()
    await db.refresh(db_rule)

    return db_rule


@router.put("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule: RuleUpdate, project_id: int = Path(...), rule_id: int = Path(...), db: AsyncSession = Depends(get_db)
):
    """Update an existing rule"""
    result = await db.execute(select(Rule).where(Rule.id == rule_id, Rule.project_id == project_id))
    db_rule = result.scalar_one_or_none()

    if not db_rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")

    # Update rule fields
    if rule.name is not None:
        db_rule.name = rule.name
    if rule.description is not None:
        db_rule.description = rule.description
    if rule.natural_language_rule is not None:
        db_rule.natural_language_rule = rule.natural_language_rule
    if rule.great_expectations_rule is not None:
        db_rule.great_expectations_rule = rule.great_expectations_rule
    if rule.type is not None:
        db_rule.type = rule.type

    # Set updated_at to current time
    db_rule.updated_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(db_rule)

    return db_rule


@router.delete("/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_rule(project_id: int = Path(...), rule_id: int = Path(...), db: AsyncSession = Depends(get_db)):
    """Delete a rule"""
    result = await db.execute(select(Rule).where(Rule.id == rule_id, Rule.project_id == project_id))
    db_rule = result.scalar_one_or_none()

    if not db_rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")

    await db.delete(db_rule)
    await db.commit()

    return None
