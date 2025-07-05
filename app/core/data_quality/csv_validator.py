import great_expectations as gx
import pandas as pd
import json
from .base_validator import BaseValidator

# from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler


import sys

class CSVValidator(BaseValidator):
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.context = gx.get_context()

        # Set up once
        self.datasource = self.context.data_sources.add_pandas("pandas")
        self.asset = self.datasource.add_dataframe_asset(name="pd_dataframe_asset")
        self.batch_def = self.asset.add_batch_definition_whole_dataframe("batch_definition")
        self.batch = self.batch_def.get_batch(batch_parameters={"dataframe": self.df})

    def validate_rules(self, rules: list) -> list:
        results = []
        for rule in rules:
            try:
                exp_type = rule["great_expectations_rule"]["expectation_type"]
                kwargs = rule["great_expectations_rule"]["kwargs"]

                exp_cls_name = "".join([part.capitalize() for part in exp_type.split("_")])
                exp_cls = getattr(gx.expectations, exp_cls_name)
                expectation = exp_cls(**kwargs)

                validation_result = self.batch.validate(expectation)

                passed = validation_result.success
                unexpected_count = validation_result.result.get("unexpected_count", 0)
                total_records = validation_result.result.get("element_count", len(self.df))
                failed_records = unexpected_count
                success_rate = 100.0 * (total_records - failed_records) / total_records if total_records else 0.0

                error_message = None if passed else f"Validation failed for {exp_type}"

            except Exception as e:
                passed = False
                failed_records = len(self.df)
                total_records = len(self.df)
                success_rate = 0.0
                error_message = str(e)

            rule_result = {
                "rule_name": rule.get("name", exp_type),
                "natural_language_rule": rule.get("natural_language_rule", ""),
                "passed": passed,
                "expectation_type": exp_type,
                "kwargs": rule["great_expectations_rule"]["kwargs"],
                "total_records": total_records,
                "failed_records": failed_records,
                "success_rate": success_rate,
                "error_message": error_message
            }

            results.append(rule_result)

        return results
