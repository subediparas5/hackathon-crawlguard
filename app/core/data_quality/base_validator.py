# core/data_quality/base_validator.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BaseValidator(ABC):
    @abstractmethod
    def validate_rules(self, rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass
