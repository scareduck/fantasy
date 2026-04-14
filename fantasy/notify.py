from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from fantasy.config import _pw

_SMTP_ACCOUNT = "rlm@scareduck.com"


def send_alert(subject: str, body: str) -> None:
    """Send a plain-text alert email to the configured account via Fastmail."""
    smtp_host_raw = _pw(_SMTP_ACCOUNT, "smtp_host", "")
    if not smtp_host_raw:
        raise RuntimeError(f"No smtp_host configured for {_SMTP_ACCOUNT} in ~/.passwords.json")

    if ":" in smtp_host_raw:
        smtp_host, smtp_port = smtp_host_raw.rsplit(":", 1)
        smtp_port = int(smtp_port)
    else:
        smtp_host = smtp_host_raw
        smtp_port = 587

    smtp_password = _pw(_SMTP_ACCOUNT, "password", "")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = _SMTP_ACCOUNT
    msg["To"] = _SMTP_ACCOUNT
    msg.attach(MIMEText(body, "plain", "utf-8"))

    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(_SMTP_ACCOUNT, smtp_password)
            server.sendmail(_SMTP_ACCOUNT, [_SMTP_ACCOUNT], msg.as_string())
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(_SMTP_ACCOUNT, smtp_password)
            server.sendmail(_SMTP_ACCOUNT, [_SMTP_ACCOUNT], msg.as_string())
