from fastapi import APIRouter, Depends, HTTPException, status, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List
from datetime import datetime, timezone

from app.core.database import get_db
from app.models.rule import Rule
from app.schemas.rule import (
    RuleBase,
    RuleResponse,
    RuleUpdate,
    SuggestedRulesResponse,
    EnhancePromptResponse,
    AddRuleRequest,
    AddRuleResponse,
)

router = APIRouter()


@router.post("/suggested-rules", response_model=SuggestedRulesResponse)
async def get_suggested_rules(project_id: int = Path(...), db: AsyncSession = Depends(get_db)):
    """Get suggested rules for a project (mock response)"""
    # Mock response with suggested rules
    suggested_rules = [
        {
            "name": "Category Validation",
            "description": "Validates categories are from allowed list",
            "natural_language_rule": "Categories must be Electronics, Clothing, or Books",
            "great_expectations_rule": {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {"column": "category", "value_set": ["Electronics", "Clothing", "Books"]},
            },
            "type": "column_values_in_set",
        },
        {
            "name": "Price Range Validation",
            "description": "Validates price is within acceptable range",
            "natural_language_rule": "Price must be between 0 and 10000",
            "great_expectations_rule": {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "price", "min_value": 0, "max_value": 10000},
            },
            "type": "column_values_between",
        },
        {
            "name": "Required Fields Validation",
            "description": "Validates required fields are not null",
            "natural_language_rule": "Product name and description must not be empty",
            "great_expectations_rule": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": ["product_name", "description"]},
            },
            "type": "column_values_not_null",
        },
    ]

    return SuggestedRulesResponse(rules=suggested_rules)


@router.post("/enhance-prompt", response_model=EnhancePromptResponse)
async def enhance_prompt(project_id: int = Path(...), prompt: str = ""):
    """Enhance a natural language prompt for rule creation"""
    enhanced_prompt = f"Enhanced: {prompt}"
    return EnhancePromptResponse(enhanced_prompt=enhanced_prompt)


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
