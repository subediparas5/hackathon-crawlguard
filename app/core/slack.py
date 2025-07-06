import requests
from typing import Dict, List
from datetime import datetime, timezone
import pytz
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from app.core.config import settings


class SlackService:
    def __init__(self):
        self.client = None
        self.webhook_url = settings.slack_webhook_url
        self.nepal_tz = pytz.timezone("Asia/Kathmandu")  # Nepal timezone

        if settings.slack_bot_token:
            self.client = WebClient(token=settings.slack_bot_token)

    def _get_nepal_time(self) -> datetime:
        """Get current time in Nepal timezone"""
        utc_now = datetime.now(timezone.utc)
        nepal_time = utc_now.astimezone(self.nepal_tz)
        return nepal_time

    def is_configured(self) -> bool:
        """Check if Slack is properly configured (either bot token or webhook)"""
        return bool(settings.slack_bot_token and self.client) or bool(self.webhook_url)

    def _use_webhook(self) -> bool:
        """Check if we should use webhook instead of bot token"""
        return bool(self.webhook_url) and (not settings.slack_bot_token or not self.client)

    async def send_validation_report(
        self,
        channel: str,
        project_name: str,
        dataset_name: str,
        validation_results: Dict,
        total_rules: int,
        passed_rules: int,
        failed_rules: int,
    ) -> bool:
        """
        Send validation report to Slack channel

        Args:
            channel: Slack channel name (without #)
            project_name: Name of the project
            dataset_name: Name of the dataset
            validation_results: Validation results from Great Expectations
            total_rules: Total number of rules validated
            passed_rules: Number of rules that passed
            failed_rules: Number of rules that failed

        Returns:
            bool: True if message sent successfully, False otherwise
        """
        if not self.is_configured():
            return False

        try:
            if self._use_webhook():
                return await self._send_webhook_validation_report(
                    project_name, dataset_name, validation_results, total_rules, passed_rules, failed_rules
                )
            else:
                return await self._send_bot_validation_report(
                    channel, project_name, dataset_name, validation_results, total_rules, passed_rules, failed_rules
                )

        except Exception:
            return False

    async def _send_bot_validation_report(
        self,
        channel: str,
        project_name: str,
        dataset_name: str,
        validation_results: Dict,
        total_rules: int,
        passed_rules: int,
        failed_rules: int,
    ) -> bool:
        """Send validation report using bot token"""
        try:
            # Create message blocks
            blocks = self._create_validation_report_blocks(
                project_name, dataset_name, validation_results, total_rules, passed_rules, failed_rules
            )

            # Send message
            if not self.client:
                return False

            response = self.client.chat_postMessage(
                channel=f"#{channel}", text=f"Validation Report for {project_name} - {dataset_name}", blocks=blocks
            )

            if response["ok"]:
                return True
            else:
                print(f"Failed to send Slack message: {response.get('error')}")
                return False

        except SlackApiError as e:
            print(f"Slack API error: {e.response['error']}")
            return False

    async def _send_webhook_validation_report(
        self,
        project_name: str,
        dataset_name: str,
        validation_results: Dict,
        total_rules: int,
        passed_rules: int,
        failed_rules: int,
    ) -> bool:
        """Send validation report using webhook URL"""
        try:
            # Create simple text message for webhook
            message = self._create_webhook_message(project_name, dataset_name, total_rules, passed_rules, failed_rules)

            # Send via webhook
            response = requests.post(self.webhook_url, json={"text": message}, timeout=10)

            if response.status_code == 200:
                return True
            else:
                print(f"Failed to send webhook message: {response.status_code}")
                return False

        except Exception as e:
            print(f"Webhook error: {str(e)}")
            return False

    def _create_webhook_message(
        self, project_name: str, dataset_name: str, total_rules: int, passed_rules: int, failed_rules: int
    ) -> str:
        """Create simple text message for webhook"""
        status_emoji = "✅" if failed_rules == 0 else "❌"
        status_text = "PASSED" if failed_rules == 0 else "FAILED"
        nepal_time = self._get_nepal_time()

        message = f"""
{status_emoji} *Data Validation Report*

*Project:* {project_name}
*Dataset:* {dataset_name}
*Status:* {status_text}
*Results:* {passed_rules}/{total_rules} rules passed

Validation completed at {nepal_time.strftime('%Y-%m-%d %H:%M:%S')} (NPT)
        """.strip()

        return message

    def _create_validation_report_blocks(
        self,
        project_name: str,
        dataset_name: str,
        validation_results: Dict,
        total_rules: int,
        passed_rules: int,
        failed_rules: int,
    ) -> List[Dict]:
        """Create beautiful Slack message blocks for validation report"""

        # Determine overall status and colors
        if failed_rules == 0:
            status_emoji = "✅"
            status_text = "VALIDATION PASSED"
            progress_emoji = "🟢"
        else:
            status_emoji = "❌"
            status_text = "VALIDATION FAILED"
            progress_emoji = "🔴"

        # Calculate success percentage
        success_percentage = (passed_rules / total_rules * 100) if total_rules > 0 else 0

        # Create progress bar
        progress_bar = self._create_progress_bar(success_percentage)

        blocks = [
            # Header with status
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{status_emoji} Data Quality Validation Report", "emoji": True},
            },
            # Divider
            {"type": "divider"},
            # Project and Dataset info
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*📁 Project*\n`{project_name}`"},
                    {"type": "mrkdwn", "text": f"*📊 Dataset*\n`{dataset_name}`"},
                ],
            },
            # Status and Results
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*{status_emoji} Status*\n*{status_text}*"},
                    {"type": "mrkdwn", "text": f"*{progress_emoji} Success Rate*\n*{success_percentage:.1f}%*"},
                ],
            },
            # Progress bar
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Overall Success:* {progress_bar}"}},
            # Detailed results
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*✅ Passed Rules*\n*{passed_rules}* rules"},
                    {"type": "mrkdwn", "text": f"*❌ Failed Rules*\n*{failed_rules}* rules"},
                    {"type": "mrkdwn", "text": f"*📋 Total Rules*\n*{total_rules}* rules"},
                    {
                        "type": "mrkdwn",
                        "text": f"*⏱️ Validation Time*\n{self._get_nepal_time().strftime('%H:%M:%S')} (NPT)",
                    },
                ],
            },
        ]

        # Add failed rules details if any
        if failed_rules > 0:
            failed_rules_details = self._extract_failed_rules(validation_results)
            if failed_rules_details:
                blocks.extend(
                    [
                        {"type": "divider"},
                        {"type": "section", "text": {"type": "mrkdwn", "text": "*🚨 Failed Rules Details*"}},
                    ]
                )

                # Group failed rules by type for better organization
                for rule_name, details in failed_rules_details[:5]:  # Limit to first 5
                    blocks.append(
                        {"type": "section", "text": {"type": "mrkdwn", "text": f"• *{rule_name}*\n  `{details}`"}}
                    )

        # Add footer with timestamp
        nepal_time = self._get_nepal_time()
        blocks.extend(
            [
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": f"🕐 Validation completed at {nepal_time.strftime('%Y-%m-%d %H:%M:%S')} (NPT) | Powered by CrawlGuard",  # noqa
                        }
                    ],
                },
            ]
        )

        return blocks

    def _create_progress_bar(self, percentage: float) -> str:
        """Create a visual progress bar"""
        filled_blocks = int(percentage / 10)
        empty_blocks = 10 - filled_blocks

        if percentage == 100:
            return "🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩"
        elif percentage == 0:
            return "⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜"
        else:
            filled = "🟩" * filled_blocks
            empty = "⬜" * empty_blocks
            return filled + empty

    def _extract_failed_rules(self, validation_results: Dict) -> List[tuple]:
        """Extract failed rules from validation results"""
        failed_rules = []

        try:
            results = validation_results.get("results", [])
            for result in results:
                if not result.get("passed", False):  # Use "passed" instead of "success"
                    rule_name = result.get("rule_name", "Unknown")
                    failed_records = result.get("failed_records", 0)
                    total_records = result.get("total_records", 0)
                    error_message = result.get("error_message", "")

                    if failed_records > 0:
                        details = f"{failed_records}/{total_records} records failed"
                    else:
                        details = error_message or "Validation failed"

                    failed_rules.append((rule_name, details))
        except Exception as e:
            print(f"Error extracting failed rules: {str(e)}")

        return failed_rules

    async def send_simple_notification(self, channel: str, message: str) -> bool:
        """Send a simple text message to Slack channel"""
        if not self.is_configured():
            return False

        try:
            if self._use_webhook():
                # Send via webhook (ignores channel parameter)
                response = requests.post(self.webhook_url, json={"text": message}, timeout=10)

                if response.status_code == 200:
                    return True
                else:
                    print(f"Failed to send webhook message: {response.status_code}")
                    return False
            else:
                # Send via bot token
                if not self.client:
                    return False

                response = self.client.chat_postMessage(channel=f"#{channel}", text=message)

                if response["ok"]:
                    return True
                else:
                    print(f"Failed to send Slack message: {response.get('error')}")
                    return False

        except SlackApiError as e:
            print(f"Slack API error: {e.response['error']}")
            return False
        except Exception as e:
            print(f"Error sending Slack notification: {str(e)}")
            return False


# Global instance
slack_service = SlackService()
