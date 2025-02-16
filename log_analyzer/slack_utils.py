from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
import traceback
import ssl

class SlackNotifier:
    def __init__(self, channel="#logs-monitoring-v2"):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        self.client = WebClient(
            token=os.getenv('SLACK_BOT_TOKEN'),
            ssl=ssl_context
        )
        self.channel = channel

    def send_notification(self, blocks):
        try:
            print(blocks)
            response = self.client.chat_postMessage(
                channel=self.channel,
                blocks=blocks
            )
            return response
        except SlackApiError as e:
            traceback.print_exc()
            print(f"Error sending message to Slack: {e.response['error']}")

