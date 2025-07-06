import great_expectations as gx
import pandas as pd
from .base_validator import BaseValidator

# from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler


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

                # Get detailed result information
                unexpected_count = validation_result.result.get("unexpected_count", 0)
                total_records = validation_result.result.get("element_count", len(self.df))
                failed_records = unexpected_count
                success_rate = 100.0 * (total_records - failed_records) / total_records if total_records else 0.0

                # Determine if validation actually passed based on business logic
                # Great Expectations success can be False even with 0 unexpected values
                # We consider it passed if no records failed the validation
                passed = failed_records == 0 and total_records > 0

                # Provide detailed error message for debugging
                if not passed:
                    if failed_records:
                        error_message = f"Validation failed: {failed_records} records failed {exp_type}"
                    elif total_records:
                        error_message = f"Validation failed: No records to validate for {exp_type}"
                    else:
                        error_message = f"""Validation failed:
                         Great Expectations reported failure for {exp_type} despite 0 failed records"""
                else:
                    error_message = None

            except Exception as e:
                passed = False
                failed_records = len(self.df)
                total_records = len(self.df)
                success_rate = 0.0
                error_message = f"Exception during validation: {str(e)}"

                # Add debugging info for column existence
                if "column" in kwargs:
                    column_name = kwargs["column"]
                    if column_name not in self.df.columns:
                        error_message += f""" - Column '{column_name}' not found in dataset.
                        Available columns: {list(self.df.columns)}"""

            # Extract columns from kwargs
            columns = self._extract_columns_from_kwargs(kwargs)

            rule_result = {
                "rule_name": rule.get("name", exp_type),
                "natural_language_rule": rule.get("natural_language_rule", ""),
                "passed": passed,
                "expectation_type": exp_type,
                "kwargs": rule["great_expectations_rule"]["kwargs"],
                "columns": columns,
                "total_records": total_records,
                "failed_records": failed_records,
                "success_rate": success_rate,
                "error_message": error_message,
            }

            results.append(rule_result)

        return results

    def _extract_columns_from_kwargs(self, kwargs: dict) -> list:
        """Extract column names from Great Expectations kwargs"""
        columns = []

        # Common column parameters in Great Expectations
        column_params = ["column", "columns", "column_A", "column_B", "column_list"]

        for param in column_params:
            if param in kwargs:
                value = kwargs[param]
                if isinstance(value, str):
                    columns.append(value)
                elif isinstance(value, list):
                    columns.extend(value)

        # Handle special cases for specific expectation types
        if "column" in kwargs:
            columns.append(kwargs["column"])
        elif "columns" in kwargs:
            if isinstance(kwargs["columns"], list):
                columns.extend(kwargs["columns"])
            else:
                columns.append(kwargs["columns"])

        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in columns:
            if col not in seen:
                unique_columns.append(col)
                seen.add(col)

        return unique_columns
