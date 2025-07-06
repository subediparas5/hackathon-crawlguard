# import pandas as pd
import json
from random import sample
from typing import Any, Dict

from openai import OpenAI

from app.core.config import settings

API_KEY = settings.deepseek_api_key

with open("basic_fields.csv", encoding="utf-8") as file:
    BASIC_FIELDS = file.read()


class DeepSeekRuleGenerator:
    """
    A class to generate data validation rules using DeepSeek AI model.
    """

    def __init__(self, project_description: str = ""):
        """
        Initialize the DeepSeek rule generator.

        Args:
            sample_data: Sample data to use for rule creation
        """
        base_url = "https://api.deepseek.com/v1"
        self.client = OpenAI(api_key=API_KEY, base_url=base_url)

        if not self.client:
            raise ValueError("Deepseek client not initialized")

        self.model = "deepseek-chat"
        self.project_description = project_description

        with open("great_expectations_docs.txt", encoding="utf-8") as file:
            self.great_expectation_docs = file.read()

    def get_suggested_rules_from_project_description(self) -> str:
        """
        Generate a comprehensive prompt for rule creation using Great Expectations.

        Args:
            project_id: ID of the project
            dataset_id: ID of the dataset to analyze
            user_prompt: Optional user instruction for specific validation rules

        Returns:
            Formatted prompt string for AI model
        """

        # Step 3: Build messages
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a data profiling assistant. Your job is to read a paragraph of natural-language description about a dataset, "
                    "and generate a structured JSON array that maps each mentioned or inferred column to a validation description.\n\n"
                    "You will also be given a sample CSV that represents an example of what the dataset looks like. "
                    "You can use the CSV to understand typical column naming conventions and data structure, but you are NOT required to strictly match only those column names.\n\n"
                    f"### csv:\n{BASIC_FIELDS}\n\n"
                    "Your task is to:\n"
                    "- Extract all columns mentioned in the paragraph and give them a `column_name` and a `validation_description`.\n"
                    "- If a column is implied (not explicitly mentioned), but you are confident about its existence or purpose (based on either the paragraph or the sample CSV), include it.\n"
                    "- If a column is mentioned but not described, still include it with a sensible default validation description based on its name.\n"
                    "- If a rule applies to the entire table, use `column_name: '__table__'`.\n\n"
                    "Output only a JSON array of objects with these keys:\n"
                    "- `column_name`: A column mentioned or inferred from the paragraph and/or sample structure.\n"
                    "- `validation_description`: A human-readable sentence summarizing the intended check or constraint.\n\n"
                    "Avoid adding columns or rules unless they are clearly justified by the paragraph or you are highly confident based on common patterns.\n"
                    "Do not include commentary or additional explanation — only the JSON array output."
                ),
            },
            {"role": "user", "content": f"### description:\n{self.project_description.strip()}"},
        ]

        response = self.client.chat.completions.create(model="deepseek-chat", messages=messages, stream=False,)

        base_rules_json = response.choices[0].message.content

        messages_with_user_prompt = [
            {
                "role": "system",
                "content": (
                    "You are a senior data quality engineer and Great Expectations expert. "
                    "Your task is to generate HIGH-QUALITY, TARGETED validation rules in structured JSON format for Great Expectations, "
                    "based only on a structured JSON array of column validation descriptions and the official Great Expectations documentation.\n\n"
                    "The input is a JSON array where each object has:\n"
                    "- `column_name`: Name of the column (or '__table__' for whole-table rules)\n"
                    "- `validation_description`: A natural-language description of what constraint should apply\n\n"
                    "You must convert each item into a proper GE rule. For each rule, include:\n"
                    "- `name`: A short, descriptive title\n"
                    "- `description`: Why this rule exists (based on the description input)\n"
                    "- `natural_language_rule`: Human-readable summary of the expectation\n"
                    "- `great_expectations_rule`: GE rule in structured format using `expectation_type` and `kwargs`\n"
                    "- `type`: One of: column_exists, uniqueness, dtype, regex, range, conditional, etc.\n\n"
                    "QUALITY GUIDELINES:\n"
                    "- Generate ONLY the most essential and impactful rules (aim for 3-5 high-quality rules)\n"
                    "- Focus on rules that would catch real data quality issues\n"
                    "- Prioritize schema validation, completeness, and data type rules\n"
                    "- Only include uniqueness rules for columns that should be unique (like IDs)\n"
                    "- Avoid overly specific regex patterns unless absolutely necessary\n"
                    "- Prefer simple, declarative expectations over complex regex\n"
                    "- Skip rules that are too generic or obvious\n\n"
                    "RULE PRIORITY:\n"
                    "1. Schema validation (column existence, table structure)\n"
                    "2. Completeness (required fields, null checks)\n"
                    "3. Data type validation (numeric, string, date formats)\n"
                    "4. Uniqueness (for primary keys or unique identifiers)\n"
                    "5. Format validation (emails, dates, currencies) - only if clearly needed\n\n"
                    "Return only a JSON array of the BEST Great Expectations rules. Do not include extra explanation."
                    "IMPORTANT: Return ONLY valid JSON, not a string representation. Do not include markdown formatting, code blocks, or any text outside the JSON array."
                    "\n\n### Great Expectations Documentation:\n" + self.great_expectation_docs.strip()
                ),
            },
            {"role": "user", "content": f"### column description-json:\n{base_rules_json}"},
        ]

        response2 = self.client.chat.completions.create(
            model="deepseek-chat", messages=messages_with_user_prompt, stream=False,
        )

        # remove ```json and ``` from the response
        content = response2.choices[0].message.content
        content = content.replace("```json", "").replace("```", "")

        return content

    def get_suggested_rules_from_sample_data(self, sample_data: str = "", metadata: dict = {}) -> str:
        """
        Generate a comprehensive prompt for rule creation using Great Expectations.

        Args:
            sample_data: Sample data to use for rule creation
        """
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a data profiling and metadata documentation expert.\n\n"
                    "You are given detailed column-wise metadata from a dataset. Your task is to analyze this metadata and produce "
                    "**short but highly descriptive summaries** of what each column represents. Each summary should combine:\n\n"
                    "- The likely semantic meaning of the column (based on the column name).\n"
                    "- Important data profiling insights observed from the metadata, such as null percentage, uniqueness, "
                    "sample value patterns, frequent values, or potential data quality issues.\n\n"
                    "Each description should be concise yet rich enough to capture both what the column represents and key observed characteristics from the metadata.\n\n"
                    "The metadata contains the following for each column:\n"
                    "- `dtype`: The data type of the column\n"
                    "- `non_null_count`: Number of non-null values\n"
                    "- `null_count`: Number of nulls\n"
                    "- `null_pct`: Percent of nulls\n"
                    "- `n_unique`: Number of unique values\n"
                    "- `sample_values`: A few example values from the column\n"
                    "- `most_freq_value`: The most frequent value in the column\n\n"
                    "### Output format:\n"
                    "Output only a JSON array of objects, each with exactly these keys:\n\n"
                    "- `column_name`: The column name from the metadata\n"
                    "- `validation_description`: A concise but rich description that merges what the column represents and important observed metadata characteristics.\n\n"
                    "Descriptions should be short but precise, e.g.:\n"
                    '- "Name of the plaintiff in a legal case, fully populated with 8,527 unique entries and no missing values"\n'
                    '- "Primary address line, mostly complete (0.85% missing), showing diverse street and PO Box formats"\n'
                    '- "Filing date of the court case, stored as strings with 1,322 unique dates and no nulls"\n'
                    '- "Secondary address info (apt/suite), sparse data with 99.87% missing but detailed unit info when present"\n\n'
                    "### Rules:\n"
                    "- Only return the JSON array, no extra commentary.\n"
                    "- Do not invent data beyond what metadata shows.\n"
                    "- Focus on combining semantic meaning + observed metadata insights in each description."
                ),
            },
            {
                "role": "user",
                "content": f"The metadata is provided as a Python dictionary like this:\n\n{metadata}\n\nBegin your response now.",
            },
        ]

        meta_data_json = self.client.chat.completions.create(model="deepseek-chat", messages=messages, stream=False,)

        messages_sample_only = [
            {
                "role": "system",
                "content": (
                    "You are a senior data quality engineer and Great Expectations expert. "
                    "Your task is to generate HIGH-QUALITY, TARGETED validation rules in structured JSON format for Great Expectations, "
                    "based on both:\n"
                    "- a clean sample of the data (assumed to be correct and complete)\n"
                    "- descriptive metadata for each column, including semantic meaning and profiling observations.\n\n"
                    "The metadata includes fields like:\n"
                    "- `column_name`: The name of the column\n"
                    "- `validation_description`: A concise but rich description that merges what the column represents and important observed metadata characteristics.\n\n"
                    "QUALITY GUIDELINES:\n"
                    "- Generate ONLY the most essential and impactful rules (aim for 3-5 high-quality rules)\n"
                    "- Focus on rules that would catch real data quality issues\n"
                    "- Prioritize schema validation, completeness, and data type rules\n"
                    "- Only include uniqueness rules for columns that should be unique (like IDs)\n"
                    "- Avoid overly specific regex patterns unless absolutely necessary\n"
                    "- Prefer simple, declarative expectations over complex regex\n"
                    "- Skip rules that are too generic or obvious\n\n"
                    "RULE PRIORITY:\n"
                    "1. Schema validation (column existence, table structure)\n"
                    "2. Completeness (required fields, null checks)\n"
                    "3. Data type validation (numeric, string, date formats)\n"
                    "4. Uniqueness (for primary keys or unique identifiers)\n"
                    "5. Format validation (emails, dates, currencies) - only if clearly needed\n\n"
                    "Use the following expectations where appropriate:\n"
                    "- expect_table_column_count_to_equal\n"
                    "- expect_table_columns_to_match_ordered_list\n"
                    "- expect_column_values_to_not_be_null\n"
                    "- expect_column_values_to_be_unique\n"
                    "- expect_column_values_to_match_regex\n"
                    "- expect_column_values_to_be_between\n"
                    "- expect_column_values_to_be_in_set\n"
                    "- expect_column_values_to_match_strftime_format\n"
                    "- expect_column_values_to_be_of_type\n"
                    "- expect_column_median_to_be_between\n"
                    "- expect_column_mean_to_be_between\n"
                    "- expect_column_min_to_be_between\n"
                    "- expect_column_max_to_be_between\n"
                    "- expect_column_value_lengths_to_be_between\n\n"
                    "Only use regex-based expectations when no other built-in GE expectation is appropriate. "
                    "Keep regex as simple and specific as possible.\n\n"
                    "For each rule, output a JSON object with:\n"
                    "- `name`: Short descriptive name\n"
                    "- `description`: Why the rule exists, optionally referencing metadata\n"
                    "- `natural_language_rule`: Human-readable summary\n"
                    "- `great_expectations_rule`: A JSON object using Great Expectations official format (`expectation_type`, `kwargs`, etc.)\n"
                    "- `type`: One of: column_exists, uniqueness, range, regex, conditional, dtype, etc.\n\n"
                    "Also use the documentation below to generate accurate Great Expectations syntax:\n\n"
                    "### Great Expectations Documentation:\n"
                    f"{self.great_expectation_docs.strip()}\n\n"
                    "Output only a **JSON array of the BEST expectations**. Do not include any explanation or extra text."
                    "IMPORTANT: Return ONLY valid JSON, not a string representation. Do not include markdown formatting, code blocks, or any text outside the JSON array."
                ),
            },
            {"role": "user", "content": f"### Sample CSV:\n{sample_data.strip()}"},
            {"role": "user", "content": f"### meta data json:\n{meta_data_json}"},
        ]

        response = self.client.chat.completions.create(
            model="deepseek-chat", messages=messages_sample_only, stream=False,
        )

        # remove ```json and ``` from the response
        content = response.choices[0].message.content
        content = content.replace("```json", "").replace("```", "")

        return content

class PromptToRule:
    def __init__(self, sample_data: str = ""):
        self.sample_data = sample_data.strip()
        base_url = "https://api.deepseek.com/v1"
        self.client = OpenAI(api_key=API_KEY, base_url=base_url)

        with open("great_expectations_docs.txt", encoding="utf-8") as file:
            self.great_expectation_docs = file.read()

    def update_rules_using_great_expetations_rule(self, great_expectations_rule:dict[str, Any])->dict[str, Any]:
        messages_with_user_prompt = [
{
  "role": "system",
  "content": (
    "You are a senior data quality engineer and expert in Great Expectations (GE).\n\n"

    "You are given a `great_expectations_rule` on the basis of which we need to make a rule block.\n"
    "Your task is to make a rule block based on this new great_expectations_rule, while ensuring the updated rule is valid and consistent with Great Expectations standards mentioned in the docs.\n\n"

    "### Great Expectations documentation:\n"
    f"{self.great_expectation_docs.strip()}\n\n"
    "You must:\n"
    "- Verify that the updated `great_expectations_rule` is valid for the given column using the official Great Expectations documentation and the sample data provided.\n"
    "### Sample CSV:\n"
    "- If the rule becomes invalid or unclear, use the provided sample data (assumed to be valid) to infer the correct expectation logic.\n\n"
    f"{self.sample_data.strip()}\n\n"

    "For rule, output a JSON object with:\n"
    "- `name`: Short descriptive name\n"
    "- `description`: Why the rule exists, optionally referencing metadata\n"
    "- `natural_language_rule`: Human-readable summary\n"
    "- `great_expectations_rule`: the great_expectations_rule provided below.\n"
    "- `type`: One of: column_exists, uniqueness, range, regex, conditional, dtype, etc.\n\n"
    "Only return the **updated rule block JSON**. Do not include explanations or comments.\n\n"
    "IMPORTANT: DO NOT INCLUDE DUPLICATE RULES. If there is a rule which checks same thing\n"
    "if there is a rule which checks length of a field, dont suggest rule which checks for null values\n"
    "if there is a rule which checks for null values, dont suggest rule which checks for length of a field\n"
    "if there is a rule which checks for uniqueness, dont suggest rule which checks for null values\n"
    "if there is a rule which checks for null values, dont suggest rule which checks for uniqueness\n"
    "if there is a rule which checks for uniqueness, dont suggest rule which checks for length of a field\n"
    "if there is a rule which checks for length of a field, dont suggest rule which checks for uniqueness\n"
    "if there is a rule which checks for uniqueness, dont suggest rule which checks for null values\n"
  )
},
        {
            "role": "user",
            "content": f"### old rule block:\n{great_expectations_rule}"
        }
                ]

        response = self.client.chat.completions.create(model="deepseek-chat", messages=messages_with_user_prompt, stream=False,)

        content = response.choices[0].message.content
        content = content.replace("```json", "").replace("```", "")

        return json.loads(content)

    def update_rules_using_natural_language(self, column_name:str, natural_language_rule:str) ->dict[str, Any]:
        messages_with_user_prompt = [
            {
            "role": "system",
            "content": (
                "You are a senior data quality engineer and expert in Great Expectations (GE).\n\n"

                "You are given a `natural_language_rule` which needs to make a new rule, in a column provided by the user.\n"
                "Your task is to make a rule block based on this new natural language instruction on the column also given, while ensuring the updated rule is valid and consistent with Great Expectations standards.\n\n"

                "You must:\n"
                "- use the natural language to change the rule and the rule needs to be applied to the given column\n"
                "- If the rule becomes invalid or unclear, use the provided sample data (assumed to be valid) to infer the correct expectation logic.\n\n"
                "### Great Expectations documentation:\n"
                f"{self.great_expectation_docs.strip()}\n\n"

                "### Sample CSV:\n"
                f"{self.sample_data.strip()}\n\n"

                "For rule, output a JSON object with:\n"
                "- `name`: Short descriptive name\n"
                "- `description`: Why the rule exists, optionally referencing metadata\n"
                "- `natural_language_rule`: the natural_language_rule provided below\n"
                "- `great_expectations_rule`: A JSON object using Great Expectations official format (`expectation_type`, `kwargs`, etc.)\n"
                "- `type`: One of: column_exists, uniqueness, range, regex, conditional, dtype, etc.\n\n"
                "Only return the **updated rule block JSON**. Do not include explanations or comments."
            )
            },
        {
            "role": "user",
            "content": f"### natural_language_rule:\n{natural_language_rule}"
        },
        {
            "role": "user",
            "content": f"### column_name:\n{column_name}"
        }


        ]

        response = self.client.chat.completions.create(model="deepseek-chat", messages=messages_with_user_prompt, stream=False,)

        content = response.choices[0].message.content
        content = content.replace("```json", "").replace("```", "")

        return json.loads(content)


    def get_suggested_rules(self, user_prompt: str, base_rules_json: str= "") -> dict[str, Any]:
        messages_with_user_prompt = [
            {
            "role": "system",
            "content": (
                "You are a senior data quality engineer and Great Expectations expert.\n\n"
                "You are given a set of base rules generated from clean sample data, and a user instruction. "
                "Your task is to revise or extend the rules based on the user instruction **strictly**.\n\n"
                "Rules must be updated as follows:\n"
                "- Only revise rules related to the columns explicitly mentioned in the user instruction.\n"
                "- If a matching rule exists for a column, attempt to **tweak** it (e.g., update regex, expand a value set, "
                "widen a numeric range, adjust a length limit, or modify a min/max threshold) to satisfy the user’s request — "
                "but only if the original rule is the same in nature (e.g., a regex should remain a regex, a range stays a range).\n"
                "- If no matching rule can be safely modified, **add a new rule** instead of replacing or removing the existing one.\n"
                "- Never modify or delete rules related to columns not mentioned in the instruction.\n"
                "- Prefer **declarative expectations** (e.g., `expect_column_values_to_be_of_type`, `expect_column_values_to_be_in_set`, "
                "`expect_column_values_to_be_between`, `expect_table_column_count_to_equal`, `expect_table_columns_to_match_ordered_list`) "
                "over regex-based rules when possible.\n"
                "- Use regex only if there is no simpler alternative, and keep it as minimal and readable as possible.\n\n"

                "also look at the documentation provided below to get the function for great expectations"
                f"### Great Expectations Documentation:\n{self.great_expectation_docs.strip()}"
                "Output only the updated JSON array of expectations. Do not include any explanation or extra text."
                )
                },
                {
                    "role": "user",
                    "content": f"### User Instruction:\n{user_prompt.strip()}"
                },
                {
                    "role": "user",
                    "content": f"### Sample CSV:\n{self.sample_data}"
                }
        ]

        if base_rules_json:
            messages_with_user_prompt.insert(1, {"role": "user", "content": f"### Original Rules:\n{base_rules_json}"})

        response = self.client.chat.completions.create(model="deepseek-chat", messages=messages_with_user_prompt, stream=False,)

        content = response.choices[0].message.content
        content = content.replace("```json", "").replace("```", "")

        return json.loads(content)
