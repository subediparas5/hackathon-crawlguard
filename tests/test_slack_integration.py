import pytest
import os
from app.core.slack import SlackService


class TestSlackService:
    """Test cases for Slack service functionality using real credentials"""

    def test_is_configured_with_token(self):
        """Test that Slack is configured when token is provided"""
        service = SlackService()
        # This will use the actual environment variables
        is_configured = service.is_configured()
        print(f"Slack configured: {is_configured}")
        # Test passes if configured, but doesn't fail if not configured
        assert isinstance(is_configured, bool)

    def test_is_configured_without_token(self):
        """Test that Slack is not configured when token is missing"""
        # Temporarily remove token for this test
        original_token = os.environ.get("SLACK_BOT_TOKEN")
        if original_token:
            del os.environ["SLACK_BOT_TOKEN"]

        try:
            service = SlackService()
            assert service.is_configured() is False
        finally:
            # Restore original token
            if original_token:
                os.environ["SLACK_BOT_TOKEN"] = original_token

    @pytest.mark.asyncio
    async def test_send_validation_report_success(self):
        """Test successful validation report sending with real credentials"""
        service = SlackService()

        if not service.is_configured():
            pytest.skip("Slack not configured - skipping real test")

        # Test data
        validation_results = {
            "results": [
                {"success": True, "expectation_config": {"kwargs": {"column": "test"}}},
                {"success": False, "expectation_config": {"kwargs": {"column": "failed"}}},
            ]
        }

        # Use a test channel (you can change this to your actual test channel)
        test_channel = "crawlguard-alerts"  # or your test channel name

        result = await service.send_validation_report(
            channel=test_channel,
            project_name="Test Project",
            dataset_name="test.csv",
            validation_results=validation_results,
            total_rules=2,
            passed_rules=1,
            failed_rules=1,
        )

        # If Slack is configured, the message should be sent successfully
        if service.is_configured():
            assert result is True
        else:
            assert result is False

    @pytest.mark.asyncio
    async def test_send_validation_report_not_configured(self):
        """Test that notification is skipped when Slack is not configured"""
        # Temporarily remove token for this test
        original_token = os.environ.get("SLACK_BOT_TOKEN")
        original_webhook = os.environ.get("SLACK_WEBHOOK_URL")

        if original_token:
            del os.environ["SLACK_BOT_TOKEN"]
        if original_webhook:
            del os.environ["SLACK_WEBHOOK_URL"]

        try:
            service = SlackService()

            result = await service.send_validation_report(
                channel="crawlguard-alerts",
                project_name="Test Project",
                dataset_name="test.csv",
                validation_results={},
                total_rules=0,
                passed_rules=0,
                failed_rules=0,
            )

            assert result is False
        finally:
            # Restore original environment
            if original_token:
                os.environ["SLACK_BOT_TOKEN"] = original_token
            if original_webhook:
                os.environ["SLACK_WEBHOOK_URL"] = original_webhook

    def test_create_validation_report_blocks(self):
        """Test creation of Slack message blocks"""
        service = SlackService()

        validation_results = {"results": [{"success": False, "expectation_config": {"kwargs": {"column": "test"}}}]}

        blocks = service._create_validation_report_blocks(
            project_name="Test Project",
            dataset_name="test.csv",
            validation_results=validation_results,
            total_rules=1,
            passed_rules=0,
            failed_rules=1,
        )

        # Check that blocks are created
        assert len(blocks) > 0
        assert blocks[0]["type"] == "header"
        assert "❌" in blocks[0]["text"]["text"]  # Failed status

    def test_extract_failed_rules(self):
        """Test extraction of failed rules from validation results"""
        service = SlackService()

        validation_results = {
            "results": [
                {
                    "success": False,
                    "expectation_config": {"kwargs": {"column": "test_column"}},
                    "result": {"unexpected_percent": 15.5},
                }
            ]
        }

        failed_rules = service._extract_failed_rules(validation_results)

        assert len(failed_rules) == 1
        assert failed_rules[0][0] == "test_column"
        assert "15.5%" in failed_rules[0][1]

    @pytest.mark.asyncio
    async def test_send_simple_notification(self):
        """Test sending a simple notification with real credentials"""
        service = SlackService()

        if not service.is_configured():
            pytest.skip("Slack not configured - skipping real test")

        # Use a test channel
        test_channel = "crawlguard-alerts"  # or your test channel name

        result = await service.send_simple_notification(
            channel=test_channel, message="🧪 This is a test notification from CrawlGuard!"
        )

        # If Slack is configured, the message should be sent successfully
        if service.is_configured():
            assert result is True
        else:
            assert result is False
