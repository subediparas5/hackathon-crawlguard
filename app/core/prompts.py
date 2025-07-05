# import pandas as pd
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

        response = self.client.chat.completions.create(model="deepseek-chat", messages=messages, stream=False)

        base_rules_json = response.choices[0].message.content

        messages_with_user_prompt = [
            {
                "role": "system",
                "content": (
                    "You are a senior data quality engineer and Great Expectations expert. "
                    "Your task is to generate validation rules in structured JSON format for Great Expectations, "
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
                    "Guidelines:\n"
                    "- Match the rule intent with the most specific and declarative GE expectation available.\n"
                    "- Avoid regex if the rule can be expressed as type, value set, or range.\n"
                    "- Use table-level expectations like `expect_table_columns_to_match_ordered_list` if appropriate.\n"
                    "- If the description is vague, infer a reasonable default (e.g., for 'date of birth', expect date + not null).\n\n"
                    "Return only a JSON array of Great Expectations rules in the above format. Do not include extra explanation."
                    "IMPORTANT: Return ONLY valid JSON, not a string representation. Do not include markdown formatting, code blocks, or any text outside the JSON array."
                    "\n\n### Great Expectations Documentation:\n" + self.great_expectation_docs.strip()
                ),
            },
            {"role": "user", "content": f"### column description-json:\n{base_rules_json}"},
        ]

        response2 = self.client.chat.completions.create(
            model="deepseek-chat", messages=messages_with_user_prompt, stream=False
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

        meta_data_json = self.client.chat.completions.create(model="deepseek-chat", messages=messages, stream=False)

        messages_sample_only = [
            {
                "role": "system",
                "content": (
                    "You are a senior data quality engineer and Great Expectations expert. "
                    "Your task is to generate validation rules in structured JSON format for Great Expectations, "
                    "based on both:\n"
                    "- a clean sample of the data (assumed to be correct and complete)\n"
                    "- descriptive metadata for each column, including semantic meaning and profiling observations.\n\n"
                    "The metadata includes fields like:\n"
                    "- `column_name`: The name of the column\n"
                    "- `validation_description`: A concise but rich description that merges what the column represents and important observed metadata characteristics.\n\n"
                    "Use the metadata and sample values together to infer:\n"
                    "- schema constraints\n"
                    "- data types and formatting expectations\n"
                    "- completeness and uniqueness\n"
                    "- reasonable value ranges\n"
                    "- semantic validation (e.g., emails, dates, monetary amounts, court names, etc.)\n\n"
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
                    "Incorporate information like:\n"
                    "- 100% missing fields (to suggest potential deprecation)\n"
                    "- Common placeholder values\n"
                    "- Address patterns, date formats, dollar values, legal entities, etc.\n\n"
                    "Also use the documentation below to generate accurate Great Expectations syntax:\n\n"
                    "### Great Expectations Documentation:\n"
                    f"{self.great_expectation_docs.strip()}\n\n"
                    "Output only a **JSON array of expectations**. Do not include any explanation or extra text."
                    "IMPORTANT: Return ONLY valid JSON, not a string representation. Do not include markdown formatting, code blocks, or any text outside the JSON array."
                ),
            },
            {"role": "user", "content": f"### Sample CSV:\n{sample_data.strip()}"},
            {"role": "user", "content": f"### meta data json:\n{meta_data_json}"},
        ]

        response = self.client.chat.completions.create(
            model="deepseek-chat", messages=messages_sample_only, stream=False
        )

        # remove ```json and ``` from the response
        content = response.choices[0].message.content
        content = content.replace("```json", "").replace("```", "")

        return content
