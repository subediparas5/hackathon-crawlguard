import asyncio
import json
from datetime import datetime, timezone
from typing import List, Callable, Any
import time

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Path, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.data_quality.validator_factory import ValidatorFactory

from app.core.database import get_db
from app.core.prompts import DeepSeekRuleGenerator, PromptToRule
from app.models.dataset import Dataset
from app.models.project import Project
from app.models.rule import Rule
from app.models.suggested_rules import SuggestedRules
from app.schemas.rule import (
    RuleBase,
    RuleResponse,
    RuleUpdate,
    SuggestedRulesResponse,
)

router = APIRouter()


async def with_retries_async(func: Callable, *args, retries: int = 3, delay: float = 1.0, **kwargs) -> Any:
    """Helper to retry async or sync functions with exponential backoff."""
    for attempt in range(retries):
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return await asyncio.to_thread(func, *args, **kwargs)
        except Exception:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(delay * (2**attempt))


async def generate_rules_async(
    project_id: int, project_description: str, sample_data_str: str = "", sample_data_columns: List[str] | None = None
):
    """Asynchronously generate rules using AI, with retries and timing logs."""
    try:
        rule_generator = DeepSeekRuleGenerator(project_description=project_description)

        # Start timing
        t0 = time.perf_counter()

        # Run AI calls concurrently with retries
        project_description_task = asyncio.create_task(
            with_retries_async(rule_generator.get_suggested_rules_from_project_description, retries=3)
        )

        sample_data_task = None
        if sample_data_str:
            sample_data_task = asyncio.create_task(
                with_retries_async(
                    rule_generator.get_suggested_rules_from_sample_data,
                    sample_data_str,
                    {"columns": sample_data_columns},
                    retries=3,
                )
            )

        # Wait for both tasks to complete
        project_description_rule_str = await project_description_task
        print(f"Project description response: {project_description_rule_str}")

        if sample_data_task:
            sample_data_rule_str = await sample_data_task
            print(f"Sample data response: {sample_data_rule_str}")
        else:
            sample_data_rule_str = ""

        # End timing
        t1 = time.perf_counter()
        print(f"AI rule generation took {t1-t0:.2f} seconds")

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

    project_result = await db.execute(select(Project).where(Project.id == project_id))
    project = project_result.scalar_one_or_none()
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

    # get rules of the project
    result = await db.execute(select(Rule).where(Rule.project_id == project_id))
    rules = result.scalars().all()

    if suggested_rules:
        return SuggestedRulesResponse(rules=json.loads(suggested_rules.rules))

    if rules:
        return SuggestedRulesResponse(rules=[])

    # If no rules exist, trigger generation and return empty response
    # Rules will be generated in the background
    from app.core.rule_generator import trigger_rule_generation_for_project

    asyncio.create_task(trigger_rule_generation_for_project(project_id, force_regenerate=False))

    return SuggestedRulesResponse(rules=[])


@router.post("/prompt-to-rules", response_model=SuggestedRulesResponse)
async def prompt_to_rules(project_id: int, prompt: str = "", db: AsyncSession = Depends(get_db)):
    """Convert a natural language prompt to rules"""

    # get the sample data for the project
    sample_data = await db.execute(select(Dataset).where(Dataset.project_id == project_id, Dataset.is_sample.is_(True)))
    sample_data = sample_data.scalar_one_or_none()
    if not sample_data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sample data not found")

    # Read the actual file content based on file type
    file_path = str(sample_data.file_path)
    file_extension = file_path.lower().split(".")[-1]

    try:
        if file_extension == "json":
            # Read JSON file
            df = pd.read_json(file_path)
        elif file_extension == "csv":
            # Read CSV file
            with open(file_path, encoding="utf-8") as file:
                df = pd.read_csv(file)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_extension}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading sample data file: {str(e)}")

    # Check if DataFrame has data before sampling
    if len(df) == 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Sample data file is empty")

    sample_size = min(100, len(df))
    sample_df = df.sample(n=sample_size, random_state=42)

    sample_data_str = sample_df.to_csv(index=False)

    rule_generator = PromptToRule(sample_data_str)
    response = rule_generator.get_suggested_rules(user_prompt=prompt)

    # Handle different response formats
    if isinstance(response, dict):
        response = [response]
    elif not isinstance(response, list):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unable to generate rules")

    # Filter and validate rules before saving
    valid_rules = []
    for rule in response:
        try:
            # Handle different rule formats from AI
            if "expectation_type" in rule and "kwargs" in rule:
                # AI returned direct Great Expectations format
                ge_rule = {"expectation_type": rule["expectation_type"], "kwargs": rule["kwargs"]}
                # Create a proper rule structure
                rule = {
                    "name": f"Generated Rule - {rule['expectation_type']}",
                    "description": f"Auto-generated rule for {rule['expectation_type']}",
                    "natural_language_rule": f"Validate {rule['expectation_type']}",
                    "great_expectations_rule": ge_rule,
                    "type": "validation",
                }
            elif not rule.get("great_expectations_rule"):
                print(f"Skipping rule without great_expectations_rule: {rule}")
                continue
            else:
                # Standard format with great_expectations_rule wrapper
                ge_rule = rule.get("great_expectations_rule", {})

            # Validate rule structure
            if not isinstance(ge_rule, dict) or "expectation_type" not in ge_rule:
                print(f"Skipping rule with invalid great_expectations_rule structure: {rule}")
                continue

            # Try to validate the rule
            validator = ValidatorFactory.create_validator(str(sample_data.file_path))
            validation_results = validator.validate_rules([rule])
            if not validation_results:
                print(f"Skipping rule that failed validation: {rule}")
                continue

            # Rule is valid, add to database
            db_rule = Rule(
                project_id=project_id,
                name=rule.get("name", "Generated Rule"),
                description=rule.get("description", ""),
                natural_language_rule=prompt,
                great_expectations_rule=rule.get("great_expectations_rule", {}),
                type=rule.get("type", "validation"),
            )

            db.add(db_rule)
            valid_rules.append(rule)

        except Exception as e:
            print(f"Error processing rule {rule}: {e}")
            continue

    # Commit all valid rules at once
    if valid_rules:
        await db.commit()

    return SuggestedRulesResponse(rules=valid_rules)


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


# For add rule with validation
@router.post("/", response_model=RuleResponse, status_code=status.HTTP_201_CREATED)
async def create_rule_validation(
    rule: RuleBase, project_id: int = Path(...), is_forced=False, db: AsyncSession = Depends(get_db)
):
    """Create a new rule with validation"""
    from app.core.rule_generator import remove_rule_from_suggested_rules

    # Use project_id from path, ignore any in body
    db_rule = Rule(
        project_id=project_id,
        name=rule.name,
        description=rule.description,
        natural_language_rule=rule.natural_language_rule,
        great_expectations_rule=rule.great_expectations_rule,
        type=rule.type,
    )

    if not is_forced:
        # For sample dataset
        sample_data_query = select(Dataset).where(Dataset.project_id == project_id)

        sample_data_result = await db.execute(sample_data_query)
        sample_data_datasets = sample_data_result.scalars().all()

        rule_to_be_applied = [
            {
                "name": db_rule.name,
                "description": db_rule.description,
                "natural_language_rule": db_rule.natural_language_rule,
                "great_expectations_rule": db_rule.great_expectations_rule,
                "type": db_rule.type,
            }
        ]

        # return sample_data_datasets
        validator = ValidatorFactory.create_validator(str(sample_data_datasets[0].file_path))

        try:
            validation_results = validator.validate_rules(rule_to_be_applied)
            if not validation_results:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No validation results found")

            if validation_results[0].get("passed", False):
                db.add(db_rule)
                await db.commit()
                await db.refresh(db_rule)

                # Remove the rule from suggested rules list
                await remove_rule_from_suggested_rules(project_id, rule.name, db)
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Validation error: {str(e)}")
    else:
        db.add(db_rule)
        await db.commit()
        await db.refresh(db_rule)

        # Remove the rule from suggested rules list even for forced creation
        await remove_rule_from_suggested_rules(project_id, rule.name, db)

    return db_rule


@router.put("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule: RuleUpdate,
    project_id: int = Path(...),
    rule_id: int = Path(...),
    update_flag: str = "",  # Optional query param: "natural_language" or "great_expectations_rule"
    db: AsyncSession = Depends(get_db),
):
    """Update an existing rule and optionally regenerate the rest of the rule based on a flag"""
    print("-----------------------", update_flag)
    result = await db.execute(select(Rule).where(Rule.id == rule_id, Rule.project_id == project_id))
    db_rule = result.scalar_one_or_none()

    if not db_rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")

    # --------------------------------------------
    # Shared sample data loader
    # --------------------------------------------
    async def get_sample_data_csv(project_id: int) -> str:
        sample_data_result = await db.execute(
            select(Dataset).where(Dataset.project_id == project_id, Dataset.is_sample.is_(True))
        )
        sample_data = sample_data_result.scalar_one_or_none()
        if not sample_data:
            raise HTTPException(status_code=404, detail="Sample data not found")

        # Determine file type based on extension
        file_path = str(sample_data.file_path)
        file_extension = file_path.lower().split(".")[-1]

        try:
            if file_extension == "json":
                # Read JSON file
                df = pd.read_json(file_path)
            elif file_extension == "csv":
                # Read CSV file
                df = pd.read_csv(file_path)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported file type: {file_extension}")

            if df.empty:
                raise HTTPException(status_code=400, detail="Sample data file is empty")

            sample_df = df.sample(min(100, len(df)), random_state=42)
            return sample_df.to_csv(index=False)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Error reading sample data file: {str(e)}")

    # --------------------------------------------
    # Handle smart update by update_flag
    # --------------------------------------------
    if update_flag == "natural_language" and rule.natural_language_rule:
        sample_data_str = await get_sample_data_csv(project_id)
        rule_generator = PromptToRule(sample_data_str)
        if rule.great_expectations_rule:
            regenerated = rule_generator.update_rules_using_natural_language(
                rule.great_expectations_rule.get("kwargs", {}).get("column", []), rule.natural_language_rule
            )

            db_rule.name = regenerated.get("name", db_rule.name)
            db_rule.description = regenerated.get("description", db_rule.description)
            db_rule.natural_language_rule = rule.natural_language_rule
            db_rule.great_expectations_rule = regenerated.get(
                "great_expectations_rule", db_rule.great_expectations_rule
            )
            db_rule.type = regenerated.get("type", db_rule.type)

    elif update_flag == "great_expectations_rule" and rule.great_expectations_rule:
        sample_data_str = await get_sample_data_csv(project_id)
        rule_generator = PromptToRule(sample_data_str)

        regenerated = rule_generator.update_rules_using_great_expetations_rule(rule.great_expectations_rule)

        db_rule.name = regenerated.get("name", db_rule.name)
        db_rule.description = regenerated.get("description", db_rule.description)
        db_rule.natural_language_rule = regenerated.get("natural_language_rule", db_rule.natural_language_rule)
        db_rule.great_expectations_rule = rule.great_expectations_rule
        db_rule.type = regenerated.get("type", db_rule.type)

    else:
        # --------------------------------------------
        # Default: simple field updates (existing logic)
        # --------------------------------------------
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

    # Timestamp
    db_rule.updated_at = datetime.now(timezone.utc)

    await db.flush()
    await db.commit()
    await db.refresh(db_rule)

    return db_rule


@router.delete("/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_rule(project_id: int = Path(...), rule_id: int = Path(...), db: AsyncSession = Depends(get_db)):
    """Soft delete a rule"""
    from datetime import datetime, timezone

    result = await db.execute(
        select(Rule).where(Rule.id == rule_id, Rule.project_id == project_id, Rule.is_deleted.is_(False))
    )
    db_rule = result.scalar_one_or_none()

    if not db_rule:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")

    # Soft delete: mark as deleted and set deleted timestamp
    db_rule.is_deleted = True
    db_rule.deleted_at = datetime.now(timezone.utc)
    db_rule.updated_at = datetime.now(timezone.utc)

    await db.flush()
    await db.commit()

    return None
