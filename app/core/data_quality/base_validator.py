# core/data_quality/base_validator.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd


class BaseValidator(ABC):
    @abstractmethod
    def validate_rules(self, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

    def _extract_failed_records_sample(self, validation_result, kwargs: dict) -> Optional[list]:
        raise NotImplementedError

    def _ensure_json_serializable(self, obj: Any) -> Any:
        """Ensure an object is JSON serializable by converting non-serializable types"""
        import numpy as np

        if isinstance(obj, pd.DataFrame):
            # Handle NaN values in DataFrame
            return obj.replace([np.inf, -np.inf], np.nan).fillna(None).to_dict(orient="records")
        elif isinstance(obj, pd.Series):
            # Handle NaN values in Series
            return obj.replace([np.inf, -np.inf], np.nan).fillna(None).to_dict()
        elif isinstance(obj, np.floating) and np.isnan(obj):
            return None
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif hasattr(obj, "to_dict"):
            return obj.to_dict()
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        elif isinstance(obj, (list, tuple)):
            return [self._ensure_json_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self._ensure_json_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        else:
            return str(obj)

    def _clean_validation_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Clean validation result to ensure it's JSON serializable"""
        import numpy as np

        cleaned_result = {}

        for key, value in result.items():
            if key == "failed_records_sample" and value is not None:
                # Ensure failed records sample is serializable
                cleaned_value = []
                for record in value:
                    if isinstance(record, dict):
                        cleaned_record = {}
                        for k, v in record.items():
                            # Handle NaN values specifically
                            if isinstance(v, (pd.Series, pd.DataFrame)):
                                cleaned_record[k] = self._ensure_json_serializable(v)
                            elif isinstance(v, (np.floating, np.integer)):
                                if isinstance(v, np.floating) and np.isnan(v):
                                    cleaned_record[k] = None
                                else:
                                    cleaned_record[k] = float(v) if isinstance(v, np.floating) else int(v)
                            elif hasattr(v, "to_dict"):
                                cleaned_record[k] = v.to_dict()
                            else:
                                cleaned_record[k] = v
                        cleaned_value.append(cleaned_record)
                    else:
                        cleaned_value.append(str(record))
                cleaned_result[key] = cleaned_value
            else:
                cleaned_result[key] = self._ensure_json_serializable(value)

        # Test JSON serialization to ensure it's valid
        try:
            import json

            json.dumps(cleaned_result)
        except (TypeError, ValueError):
            # If serialization fails, create a minimal safe version
            safe_result = {
                "rule_name": cleaned_result.get("rule_name", "Unknown"),
                "natural_language_rule": cleaned_result.get("natural_language_rule", ""),
                "passed": cleaned_result.get("passed", False),
                "expectation_type": cleaned_result.get("expectation_type", ""),
                "kwargs": cleaned_result.get("kwargs", {}),
                "columns": cleaned_result.get("columns", []),
                "total_records": cleaned_result.get("total_records", 0),
                "failed_records": cleaned_result.get("failed_records", 0),
                "success_rate": cleaned_result.get("success_rate", 0.0),
                "error_message": cleaned_result.get("error_message", "Serialization error"),
                "failed_records_sample": None,  # Remove problematic sample
            }
            return safe_result

        return cleaned_result
