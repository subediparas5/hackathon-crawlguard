import great_expectations as gx
import pandas as pd
import numpy as np
from typing import Optional

# from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler

from .base_validator import BaseValidator


class JSONValidator(BaseValidator):
    def __init__(self, json_data: list[dict]):
        self.json_data = json_data
        self.df = pd.json_normalize(self.json_data)
        self.context = gx.get_context()

    def validate_rules(self, rules: list) -> list:
        results = []
        datasource = self.context.data_sources.add_pandas("pandas_json")
        asset = datasource.add_dataframe_asset(name="json_dataframe_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("batch_definition")
        for rule in rules:
            try:
                df_to_validate = self.df.copy()

                exp_type = rule["great_expectations_rule"]["expectation_type"]
                kwargs = rule["great_expectations_rule"]["kwargs"]
                column = kwargs.get("column")

                # If the field is a list → explode for per-element validation
                if (
                    pd.api.types.is_object_dtype(df_to_validate[column])
                    and df_to_validate[column].apply(lambda x: isinstance(x, list)).any()
                ):
                    df_to_validate["__record_id__"] = df_to_validate.index
                    df_to_validate = df_to_validate.explode(column).reset_index(drop=True)

                batch = batch_def.get_batch(batch_parameters={"dataframe": df_to_validate})

                exp_cls_name = "".join([part.capitalize() for part in exp_type.split("_")])
                exp_cls = getattr(gx.expectations, exp_cls_name)
                expectation = exp_cls(**kwargs)
                # Ensure result format is properly set
                if "result_format" in kwargs:
                    expectation.result_format = kwargs["result_format"]

                # record-level aggregation if exploded
                if "__record_id__" in df_to_validate.columns:
                    validation_result = batch.validate(
                        expectation,
                        result_format={
                            "result_format": "COMPLETE",
                            "unexpected_index_column_names": ["__record_id__"],
                            "return_unexpected_index_query": True,
                        },
                    )

                    unexpected_index_list = validation_result.get("result", {}).get("unexpected_index_list", [])

                    record_ids = [item["__record_id__"] for item in unexpected_index_list if "__record_id__" in item]

                    unexpected_indices = list(set(record_ids))

                    failed_ids = set(unexpected_indices)

                    failed_records = len(failed_ids)
                    total_records = self.df.shape[0] if hasattr(self, "df") else 0

                    passed = failed_records == 0
                    success_rate = (
                        100.0 * (total_records - failed_records) / total_records if total_records > 0 else 0.0
                    )

                else:
                    validation_result = batch.validate(expectation)
                    unexpected_count = validation_result.result.get("unexpected_count", 0)

                    missing_count = validation_result.result.get("missing_count", 0)

                    total_records = validation_result.result.get("element_count", len(df_to_validate))
                    failed_records = unexpected_count + missing_count
                    passed = failed_records == 0 and total_records > 0
                    success_rate = 100.0 * (total_records - failed_records) / total_records if total_records else 0.0

                error_message = None if passed else f"Validation failed for {exp_type}"

                # Extract failed records sample if validation failed
                failed_records_sample = None
                if not passed and failed_records > 0:
                    failed_records_sample = self._extract_failed_records_sample(validation_result, kwargs)

            except Exception as e:
                passed = False
                failed_records = len(self.df)
                total_records = len(self.df)
                success_rate = 0.0
                error_message = str(e)

                failed_records_sample = None
                if failed_records > 0:
                    failed_records_sample = self._extract_failed_records_sample(validation_result, kwargs)

            rule_result = {
                "rule_name": rule.get("name", exp_type),
                "natural_language_rule": rule.get("natural_language_rule", ""),
                "passed": passed,
                "expectation_type": exp_type,
                "kwargs": kwargs,
                "total_records": total_records,
                "failed_records": failed_records,
                "success_rate": success_rate,
                "error_message": error_message,
                "failed_records_sample": failed_records_sample,
            }

            # Clean the result to ensure JSON serializability
            cleaned_result = self._clean_validation_result(rule_result)
            results.append(cleaned_result)

        return results

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
                    if isinstance(item, dict):
                        # Handle different index formats
                        if "index" in item:
                            indices.append(item["index"])
                        elif "__record_id__" in item:
                            indices.append(item["__record_id__"])
                        elif "row_id" in item:
                            indices.append(item["row_id"])
                    elif isinstance(item, (int, float)):
                        indices.append(int(item))

                if indices:
                    # Get up to 5 failed records by index
                    sample_indices = indices[:5]
                    try:
                        # Handle NaN values properly when converting to dict
                        sample_df = self.df.iloc[sample_indices]
                        sample_records = (
                            sample_df.replace([np.inf, -np.inf], np.nan).fillna(None).to_dict(orient="records")
                        )
                        failed_samples.extend(sample_records)
                    except Exception:
                        pass

            # Method 2: Use partial_unexpected_index_list
            if not failed_samples and partial_unexpected_index_list and hasattr(self, "df"):
                indices = []
                for item in partial_unexpected_index_list:
                    if isinstance(item, dict):
                        if "index" in item:
                            indices.append(item["index"])
                        elif "__record_id__" in item:
                            indices.append(item["__record_id__"])
                        elif "row_id" in item:
                            indices.append(item["row_id"])
                    elif isinstance(item, (int, float)):
                        indices.append(int(item))

                if indices:
                    sample_indices = indices[:5]
                    try:
                        # Handle NaN values properly when converting to dict
                        sample_df = self.df.iloc[sample_indices]
                        sample_records = (
                            sample_df.replace([np.inf, -np.inf], np.nan).fillna(None).to_dict(orient="records")
                        )
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
