def format_slack_blocks(log_data, ai_analysis):
    error_message = log_data['message'][:1500] + "..." if len(log_data['message']) > 1500 else log_data['message']
    error_block = f"**Error Message:**\n```{error_message}```"

    ai_analysis_truncated = ai_analysis[:2000] + "..." if len(ai_analysis) > 2000 else ai_analysis
    ai_block = f"**AI Analysis:**\n{ai_analysis_truncated}"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 Error Alert",
                "emoji": True
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Service:*\n{log_data['service_type']}"},
                {"type": "mrkdwn", "text": f"*Instance:*\n{log_data['instance_name']}"},
                {"type": "mrkdwn", "text": f"*Region:*\n{log_data['region']}"},
                {"type": "mrkdwn", "text": f"*Cloud:*\n{log_data['cloud_provider']}"}
            ]
        },
        {
            "type": "markdown",
            "text": error_block
        },
        {
            "type": "divider"
        },
        {
            "type": "markdown",
            "text": ai_block
        }
    ]

    return blocks
