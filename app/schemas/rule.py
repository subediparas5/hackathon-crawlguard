from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, Dict, Any


class RuleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    natural_language_rule: str = Field(..., min_length=1)
    great_expectations_rule: Dict[str, Any] = Field(..., description="Great Expectations rule configuration")
    type: str = Field(..., min_length=1, max_length=100)


class RuleCreate(RuleBase):
    project_id: int = Field(..., description="ID of the project this rule belongs to")


class RuleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    natural_language_rule: Optional[str] = Field(None, min_length=1)
    great_expectations_rule: Optional[Dict[str, Any]] = Field(None, description="Great Expectations rule configuration")
    type: Optional[str] = Field(None, min_length=1, max_length=100)


class RuleResponse(RuleBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    project_id: int
    created_at: datetime
    updated_at: datetime


class SuggestedRulesRequest(BaseModel):
    project_id: int = Field(..., description="ID of the project to get suggested rules for")


class SuggestedRulesResponse(BaseModel):
    rules: list[Dict[str, Any]] = Field(..., description="List of suggested rules")


class EnhancePromptRequest(BaseModel):
    prompt: str = Field(..., min_length=1, description="Original prompt to enhance")


class EnhancePromptResponse(BaseModel):
    enhanced_prompt: str = Field(..., description="Enhanced prompt")


class AddRuleRequest(BaseModel):
    project_id: int = Field(..., description="ID of the project")
    prompt: str = Field(..., min_length=1, description="Natural language description of the rule")
    note: Optional[str] = Field(None, description="Additional notes about the rule")


class AddRuleResponse(BaseModel):
    rule_id: int = Field(..., description="ID of the created rule")
