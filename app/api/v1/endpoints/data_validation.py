from app.core.data_quality.validator_factory import ValidatorFactory
from app.core.slack import slack_service

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime, timezone

from app.core.database import get_db
from app.models.dataset import Dataset
from app.models.rule import Rule
from app.models.project import Project
from app.schemas.validation import ValidationResponse, ValidationSummary, ValidationRuleResult
from app.core.project_summary import update_project_summary


router = APIRouter()


@router.get("/", response_model=ValidationResponse)
async def validate_data(project_id: int, dataset_id: int, db: AsyncSession = Depends(get_db)):
    # check dataset exists
    try:
        result_project = await db.execute(
            select(Dataset).where((Dataset.id == dataset_id) & (Dataset.project_id == project_id))
        )
        dataset_project = result_project.scalar_one_or_none()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error while checking dataset: {str(e)}"
        )

    if not dataset_project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"Dataset {dataset_id} in project {project_id} not found"
        )

    # fetch project details for Slack notification
    try:
        project_result = await db.execute(select(Project).where(Project.id == project_id))
        project = project_result.scalar_one_or_none()
        if not project:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error while fetching project: {str(e)}"
        )

    # Check if dataset has cached validation results
    if dataset_project.validations and dataset_project.last_validated_at:
        # Check if rules have changed since last validation
        try:
            rule_query = select(
                Rule.name,
                Rule.description,
                Rule.natural_language_rule,
                Rule.great_expectations_rule,
                Rule.type,
            ).where(Rule.project_id == project_id)
            result = await db.execute(rule_query)
            current_rules = result.all()

            # Simple check: if number of rules changed, re-validate
            if len(current_rules) == len(dataset_project.validations):
                # Return cached results
                cached_results = dataset_project.validations

                # Calculate summary statistics from cached results
                total_rules = len(cached_results)
                passed_rules = sum(1 for result in cached_results if result.get("passed", False))
                failed_rules = total_rules - passed_rules

                total_records_processed = sum(result.get("total_records", 0) for result in cached_results)
                total_failed_records = sum(result.get("failed_records", 0) for result in cached_results)
                overall_success_rate = (
                    100.0 * (total_records_processed - total_failed_records) / total_records_processed
                    if total_records_processed > 0
                    else 0.0
                )

                # Determine overall status
                if failed_rules == 0:
                    validation_status = "Passed"
                elif passed_rules == 0:
                    validation_status = "Failed"
                else:
                    validation_status = "Imperfect"

                # Convert cached results to ValidationRuleResult models
                validation_results = []
                for result in cached_results:
                    validation_result = ValidationRuleResult(
                        rule_name=result.get("rule_name", ""),
                        natural_language_rule=result.get("natural_language_rule", ""),
                        passed=result.get("passed", False),
                        expectation_type=result.get("expectation_type", ""),
                        kwargs=result.get("kwargs", {}),
                        columns=result.get("columns", []),
                        total_records=result.get("total_records", 0),
                        failed_records=result.get("failed_records", 0),
                        success_rate=result.get("success_rate", 0.0),
                        error_message=result.get("error_message"),
                        failed_records_sample=result.get("failed_records_sample"),
                    )
                    validation_results.append(validation_result)

                # Create summary
                summary = ValidationSummary(
                    total_rules=total_rules,
                    passed_rules=passed_rules,
                    failed_rules=failed_rules,
                    overall_success_rate=overall_success_rate,
                    total_records_processed=total_records_processed,
                    total_failed_records=total_failed_records,
                )

                # Extract dataset name from file path
                dataset_name = (
                    dataset_project.file_path.split("/")[-1] if dataset_project.file_path else "Unknown Dataset"
                )

                return ValidationResponse(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    dataset_name=dataset_name,
                    summary=summary,
                    results=validation_results,
                    status=validation_status,
                )
        except Exception:
            # If there's an error checking cached results, continue with fresh validation
            pass

    # fetch rules for fresh validation
    try:
        rule_query = select(
            Rule.name,
            Rule.description,
            Rule.natural_language_rule,
            Rule.great_expectations_rule,
            Rule.type,
        ).where(Rule.project_id == project_id)
        result = await db.execute(rule_query)
        rows = result.all()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error while fetching rules: {str(e)}"
        )

    rules = [
        {
            "name": r.name,
            "description": r.description,
            "natural_language_rule": r.natural_language_rule,
            "great_expectations_rule": r.great_expectations_rule,
            "type": r.type,
        }
        for r in rows
    ]

    # validate
    validator = ValidatorFactory.create_validator(str(dataset_project.file_path))

    try:
        results = validator.validate_rules(rules)
        if not results:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No validation results found")

        validations_query = select(Dataset).where(Dataset.id == dataset_id)

        result = await db.execute(validations_query)
        dataset = result.scalar_one_or_none()
        if not dataset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

        dataset.validations = results  # type: ignore
        dataset.last_validated_at = datetime.now(timezone.utc)  # type: ignore
        await db.commit()

        # Update project summary after validation
        try:
            await update_project_summary(project_id, db)
        except Exception as e:
            # Log error but don't fail the validation
            import logging

            logger = logging.getLogger(__name__)
            logger.error(f"Failed to update project summary: {str(e)}")

        # Calculate summary statistics
        total_rules = len(results)
        passed_rules = sum(1 for result in results if result.get("passed", False))
        failed_rules = total_rules - passed_rules

        total_records_processed = sum(result.get("total_records", 0) for result in results)
        total_failed_records = sum(result.get("failed_records", 0) for result in results)
        overall_success_rate = (
            100.0 * (total_records_processed - total_failed_records) / total_records_processed
            if total_records_processed > 0
            else 0.0
        )

        # Determine overall status
        if failed_rules == 0:
            validation_status = "Passed"
        elif passed_rules == 0:
            validation_status = "Failed"
        else:
            validation_status = "Imperfect"

        # Convert results to ValidationRuleResult models
        validation_results = []
        for result in results:
            validation_result = ValidationRuleResult(
                rule_name=result.get("rule_name", ""),
                natural_language_rule=result.get("natural_language_rule", ""),
                passed=result.get("passed", False),
                expectation_type=result.get("expectation_type", ""),
                kwargs=result.get("kwargs", {}),
                columns=result.get("columns", []),
                total_records=result.get("total_records", 0),
                failed_records=result.get("failed_records", 0),
                success_rate=result.get("success_rate", 0.0),
                error_message=result.get("error_message"),
                failed_records_sample=result.get("failed_records_sample"),
            )
            validation_results.append(validation_result)

        # Create summary
        summary = ValidationSummary(
            total_rules=total_rules,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            overall_success_rate=overall_success_rate,
            total_records_processed=total_records_processed,
            total_failed_records=total_failed_records,
        )

        # Extract dataset name from file path
        dataset_name = dataset_project.file_path.split("/")[-1] if dataset_project.file_path else "Unknown Dataset"

        # Send Slack notification if project has a linked channel and dataset is not a sample
        if project.slack_channel and not dataset_project.is_sample:  # type: ignore
            await _send_slack_notification(
                project=project,
                dataset=dataset_project,
                validation_results={"results": results, "summary": summary.model_dump()},
                rules=rules,
            )

        # Return structured response
        return ValidationResponse(
            project_id=project_id,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            summary=summary,
            results=validation_results,
            status=validation_status,
        )

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Validation error: {str(e)}")


async def _send_slack_notification(project: Project, dataset: Dataset, validation_results: dict, rules: list):
    """Send Slack notification for validation results"""
    try:
        # Calculate validation statistics
        total_rules = len(rules)
        passed_rules = 0
        failed_rules = 0

        # Count passed/failed rules from results
        if validation_results and "results" in validation_results:
            for result in validation_results["results"]:
                if result.get("passed", False):  # Use "passed" instead of "success"
                    passed_rules += 1
                else:
                    failed_rules += 1

        # Extract dataset name from file path
        dataset_name = dataset.file_path.split("/")[-1] if dataset.file_path else "Unknown Dataset"

        # Send notification
        await slack_service.send_validation_report(
            channel=project.slack_channel,
            project_name=project.name,
            dataset_name=dataset_name,
            validation_results=validation_results,
            total_rules=total_rules,
            passed_rules=passed_rules,
            failed_rules=failed_rules,
        )

    except Exception as e:
        # Log error but don't fail the validation
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Failed to send Slack notification: {str(e)}")
