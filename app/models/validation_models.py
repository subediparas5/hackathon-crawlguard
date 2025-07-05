# core/models/validation_models.py
from typing import Dict, Any
from pydantic import BaseModel


class ValidationRule(BaseModel):
    name: str
    description: str
    natural_language_rule: str
    great_expectations_rule: Dict[str, Any]
    type: str


class ValidationResult(BaseModel):
    rule_name: str
    natural_language_rule: str
    passed: bool
    expectation_type: str
    kwargs: Dict[str, Any]
    total_records: int
    failed_records: int
    success_rate: float
    error_message: str
