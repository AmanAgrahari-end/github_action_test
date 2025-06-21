import boto3

class NotificationSender:
    @staticmethod
    def send_email_via_ses(
        email_to,
        email_cc,
        email_bcc,
        subject_line,
        html_body,
        role_arn,
        role_session_name,
        region,
        sender_email
    ):
        try:
            # Step 1: Assume IAM Role using STS
            sts_client = boto3.client('sts', region_name=region)
            assumed_role = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=role_session_name
            )

            session = boto3.Session(
                aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
                aws_session_token=assumed_role['Credentials']['SessionToken'],
                region_name=region
            )

            # Step 2: Create SES client using assumed role session
            ses_client = session.client('ses')

            # Step 3: Send email via SES
            response = ses_client.send_email(
                Source=sender_email,
                Destination={
                    'ToAddresses': [email_to],
                    'CcAddresses': [email_cc] if email_cc else [],
                    'BccAddresses': [email_bcc] if email_bcc else []
                },
                Message={
                    'Subject': {
                        'Data': subject_line,
                        'Charset': 'utf-8'
                    },
                    'Body': {
                        'Html': {
                            'Data': html_body,
                            'Charset': 'utf-8'
                        }
                    }
                }
            )

            print("✅ Email sent successfully. Message ID:", response['MessageId'])

        except Exception as e:
            print("❌ Failed to send email via SES:", str(e))


# Example usage
if __name__ == "__main__":
    NotificationSender.send_email_via_ses(
        email_to="recipient@example.com",
        email_cc="cc@example.com",
        email_bcc="bcc@example.com",
        subject_line="Test Email Subject",
        html_body="""
            <html>
                <body>
                    <h2>Hello from SES!</h2>
                    <p>This email was sent using an assumed IAM role and AWS SES.</p>
                </body>
            </html>
        """,
        role_arn="arn:aws:iam::123456789012:role/YourSESSendingRole",
        role_session_name="EmailSession",
        region="us-east-1",
        sender_email="sender@example.com"
    )
