import great_expectations as gx
import pandas as pd
from typing import Optional
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
            failed_records_sample = None
            try:
                exp_type = rule["great_expectations_rule"]["expectation_type"]
                kwargs = rule["great_expectations_rule"]["kwargs"]

                exp_cls_name = "".join([part.capitalize() for part in exp_type.split("_")])
                exp_cls = getattr(gx.expectations, exp_cls_name)
                expectation = exp_cls(**kwargs)

                validation_result = self.batch.validate(expectation)

                # Get detailed result information
                unexpected_count = validation_result.result.get("unexpected_count", 0)
                missing_count = validation_result.result.get("missing_count", 0)
                total_records = validation_result.result.get("element_count", len(self.df))
                failed_records = unexpected_count + missing_count
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

                # Extract failed records sample if validation failed
                if not passed and failed_records > 0:
                    failed_records_sample = self._extract_failed_records_sample(validation_result, kwargs)

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
                "failed_records_sample": failed_records_sample,
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

    def _extract_failed_records_sample(self, validation_result, kwargs: dict) -> Optional[list]:
        """Extract a sample of failed records (up to 5) for debugging"""
        try:
            # Get various types of unexpected data from Great Expectations result
            unexpected_values = validation_result.result.get("unexpected_values", [])
            partial_unexpected_values = validation_result.result.get("partial_unexpected_values", [])
            unexpected_index_list = validation_result.result.get("unexpected_index_list", [])
            partial_unexpected_index_list = validation_result.result.get("partial_unexpected_index_list", [])

            # Try to get actual failed records from the dataframe
            failed_samples = []

            # Method 1: Use unexpected_index_list to get actual records
            if unexpected_index_list and hasattr(self, "df"):
                indices = []
                for item in unexpected_index_list:
                    if isinstance(item, dict) and "index" in item:
                        indices.append(item["index"])
                    elif isinstance(item, (int, float)):
                        indices.append(int(item))

                if indices:
                    # Get up to 5 failed records by index
                    sample_indices = indices[:5]
                    try:
                        sample_records = self.df.iloc[sample_indices].to_dict(orient="records")
                        failed_samples.extend(sample_records)
                    except Exception:
                        pass

            # Method 2: Use partial_unexpected_index_list
            if not failed_samples and partial_unexpected_index_list and hasattr(self, "df"):
                indices = []
                for item in partial_unexpected_index_list:
                    if isinstance(item, dict) and "index" in item:
                        indices.append(item["index"])
                    elif isinstance(item, (int, float)):
                        indices.append(int(item))

                if indices:
                    sample_indices = indices[:5]
                    try:
                        sample_records = self.df.iloc[sample_indices].to_dict(orient="records")
                        failed_samples.extend(sample_records)
                    except Exception:
                        pass

            # Method 3: Use unexpected_values directly
            if not failed_samples and unexpected_values:
                sample_values = unexpected_values[:5]
                for value in sample_values:
                    if isinstance(value, dict):
                        failed_samples.append(value)
                    else:
                        # For simple values, create a dict with the column name
                        column_name = kwargs.get("column", "unknown_column")
                        failed_samples.append({column_name: str(value)})

            # Method 4: Use partial_unexpected_values
            if not failed_samples and partial_unexpected_values:
                sample_values = partial_unexpected_values[:5]
                for value in sample_values:
                    if isinstance(value, dict):
                        failed_samples.append(value)
                    else:
                        column_name = kwargs.get("column", "unknown_column")
                        failed_samples.append({column_name: str(value)})

            # If we still don't have samples, try to get some sample data from the column
            if not failed_samples and "column" in kwargs:
                column_name = kwargs["column"]
                if column_name in self.df.columns:
                    # Get first 5 non-null values from the column as a fallback
                    column_data = self.df[column_name].dropna().head(5)
                    for value in column_data:
                        failed_samples.append({column_name: str(value)})

            return failed_samples if failed_samples else None

        except Exception:
            # If we can't extract samples, return None
            return None
