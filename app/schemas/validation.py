from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime


class ValidationRuleResult(BaseModel):
    """Model for individual rule validation result"""

    rule_name: str = Field(..., description="Name of the validation rule")
    natural_language_rule: str = Field(..., description="Human-readable description of the rule")
    passed: bool = Field(..., description="Whether the validation passed")
    expectation_type: str = Field(..., description="Great Expectations expectation type")
    kwargs: Dict[str, Any] = Field(..., description="Arguments passed to the expectation")
    columns: List[str] = Field(..., description="Columns on which the rule was applied")
    total_records: int = Field(..., description="Total number of records processed")
    failed_records: int = Field(..., description="Number of records that failed validation")
    success_rate: float = Field(..., description="Percentage of records that passed validation")
    error_message: Optional[str] = Field(None, description="Error message if validation failed")
    failed_records_sample: Optional[List[Dict[str, Any]]] = Field(
        None, description="Sample of failed records (up to 5)"
    )


class ValidationSummary(BaseModel):
    """Model for validation summary statistics"""

    total_rules: int = Field(..., description="Total number of rules validated")
    passed_rules: int = Field(..., description="Number of rules that passed")
    failed_rules: int = Field(..., description="Number of rules that failed")
    overall_success_rate: float = Field(..., description="Overall success rate across all rules")
    total_records_processed: int = Field(..., description="Total number of records processed across all rules")
    total_failed_records: int = Field(..., description="Total number of failed records across all rules")


class ValidationResponse(BaseModel):
    """Model for the complete validation response"""

    project_id: int = Field(..., description="ID of the project being validated")
    dataset_id: int = Field(..., description="ID of the dataset being validated")
    dataset_name: str = Field(..., description="Name of the dataset file")
    validation_timestamp: datetime = Field(
        default_factory=datetime.now, description="When the validation was performed"
    )
    summary: ValidationSummary = Field(..., description="Summary statistics of the validation")
    results: List[ValidationRuleResult] = Field(..., description="Detailed results for each rule")
    status: str = Field(..., description="Overall validation status (Passed/Failed/Imperfect)")

    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})


class ValidationRule(BaseModel):
    """Model for a validation rule"""

    name: str = Field(..., description="Name of the rule")
    description: str = Field(..., description="Description of what the rule validates")
    natural_language_rule: str = Field(..., description="Human-readable rule description")
    great_expectations_rule: Dict[str, Any] = Field(..., description="Great Expectations rule configuration")
    type: str = Field(..., description="Type of validation rule")


class ValidationRequest(BaseModel):
    """Model for validation request (if needed for POST endpoint)"""

    project_id: int = Field(..., description="ID of the project to validate")
    dataset_id: int = Field(..., description="ID of the dataset to validate")
    rules: Optional[List[ValidationRule]] = Field(None, description="Optional custom rules to validate against")
