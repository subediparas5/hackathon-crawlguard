import great_expectations as gx
import pandas as pd
import json
# from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler

from .base_validator import BaseValidator

import sys

class JSONValidator(BaseValidator):
    def __init__(self, json_data: list[dict]):
        print(gx.__version__)

        self.json_data = json_data
        self.df = pd.json_normalize(self.json_data)
        self.context = gx.get_context()

    def validate_rules(self, rules: list) -> list:
        results = []
        for rule in rules:
            try:
                df_to_validate = self.df.copy()

                exp_type = rule["great_expectations_rule"]["expectation_type"]
                kwargs = rule["great_expectations_rule"]["kwargs"]
                column = kwargs.get("column")

                # If the field is a list → explode for per-element validation
                if pd.api.types.is_object_dtype(df_to_validate[column]) and df_to_validate[column].apply(lambda x: isinstance(x, list)).any():
                    df_to_validate["__record_id__"] = df_to_validate.index
                    df_to_validate = df_to_validate.explode(column).reset_index(drop=True)


                datasource = self.context.data_sources.add_pandas("pandas_json")
                asset = datasource.add_dataframe_asset(name="json_dataframe_asset")
                batch_def = asset.add_batch_definition_whole_dataframe("batch_definition")
                batch = batch_def.get_batch(batch_parameters={"dataframe": df_to_validate})

                exp_cls_name = "".join([part.capitalize() for part in exp_type.split("_")])
                # exp_cls = getattr(gx.expectations, exp_cls_name)
                # expectation = exp_cls(**kwargs)

                exp_cls = getattr(gx.expectations, exp_cls_name)
                expectation = exp_cls(**kwargs)
                # Ensure result format is properly set
                if "result_format" in kwargs:
                    expectation.result_format = kwargs["result_format"]


                validation_result = batch.validate(expectation)

                # record-level aggregation if exploded
                if "__record_id__" in df_to_validate.columns:
                    failed_ids = set(validation_result.unexpected_index_list)
                    failed_records = len(failed_ids)
                    total_records = self.df.shape[0]
                    passed = failed_records == 0
                    success_rate = 100.0 * (total_records - failed_records) / total_records if total_records else 0.0
                else:
                    passed = validation_result.success
                    unexpected_count = validation_result.result.get("unexpected_count", 0)
                    total_records = validation_result.result.get("element_count", len(df_to_validate))
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
                "kwargs": kwargs,
                "total_records": total_records,
                "failed_records": failed_records,
                "success_rate": success_rate,
                "error_message": error_message
            }

            results.append(rule_result)

        return results

    # def validate_rules(self, rules: list) -> list:
    #     results = []
    #     for rule in rules:
    #         try:
    #             df_to_validate = self.df.copy()
    #             exp_type = rule["great_expectations_rule"]["expectation_type"]
    #             kwargs = rule["great_expectations_rule"]["kwargs"]
    #             column = kwargs.get("column")

    #             # Add record ID before any transformations
    #             df_to_validate["__record_id__"] = df_to_validate.index

    #             # If the field is a list → explode for per-element validation
    #             if pd.api.types.is_object_dtype(df_to_validate[column]) and df_to_validate[column].apply(lambda x: isinstance(x, list)).any():
    #                 df_to_validate = df_to_validate.explode(column).reset_index(drop=True)

    #             print(df_to_validate.columns)
    #             print(df_to_validate.head())

    #             # Create datasource and batch (using modern 1.5.4 API)
    #             datasource = self.context.sources.add_pandas("pandas_json")
    #             asset = datasource.add_dataframe_asset(name="json_dataframe_asset")
    #             batch_request = asset.build_batch_request(dataframe=df_to_validate)
    #             batch = self.context.get_batch_list(batch_request=batch_request)[0]

    #             # Get expectation class
    #             exp_cls_name = "".join([part.capitalize() for part in exp_type.split("_")])
    #             exp_cls = getattr(gx.expectations, exp_cls_name)
                
    #             # Create expectation with enforced result format
    #             expectation_kwargs = kwargs.copy()
    #             expectation_kwargs["result_format"] = {
    #                 "result_format": "COMPLETE",
    #                 "unexpected_index_column_names": ["__record_id__", "id"],
    #                 "include_unexpected_rows": True
    #             }
    #             expectation = exp_cls(**expectation_kwargs)

    #             validation_result = batch.validate(expectation)
                
    #             # Debug print
    #             print(json.dumps(validation_result.to_json_dict(), indent=2))
                
    #             # Get unexpected index list
    #             result_dict = validation_result.to_json_dict()
    #             unexpected_list = result_dict["result"].get("unexpected_index_list", [])
    #             print(f"Unexpected index list: {unexpected_list}")

    #             # Calculate metrics
    #             if "__record_id__" in df_to_validate.columns:
    #                 failed_ids = {item["__record_id__"] for item in unexpected_list}
    #                 failed_records = len(failed_ids)
    #                 total_records = len(self.df)
    #             else:
    #                 failed_records = result_dict["result"].get("unexpected_count", 0)
    #                 total_records = result_dict["result"].get("element_count", len(df_to_validate))

    #             passed = failed_records == 0
    #             success_rate = 100.0 * (total_records - failed_records) / total_records if total_records else 0.0
    #             error_message = None if passed else f"Validation failed for {exp_type}"

    #         except Exception as e:
    #             passed = False
    #             failed_records = len(self.df)
    #             total_records = len(self.df)
    #             success_rate = 0.0
    #             error_message = str(e)
    #             unexpected_list = []

    #         rule_result = {
    #             "rule_name": rule.get("name", exp_type),
    #             "natural_language_rule": rule.get("natural_language_rule", ""),
    #             "passed": passed,
    #             "expectation_type": exp_type,
    #             "kwargs": kwargs,
    #             "total_records": total_records,
    #             "failed_records": failed_records,
    #             "success_rate": success_rate,
    #             "error_message": error_message,
    #             "unexpected_indices": unexpected_list  # Add this for debugging
    #         }

    #         results.append(rule_result)

    #     return results
