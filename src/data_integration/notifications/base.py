from __future__ import annotations

import logging
from dataclasses import dataclass

from data_integration.logging_setup import log_fields

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class NotificationMessage:
    subject: str
    body: str
    recipients: list[str]


class Notifier:
    def send(self, message: NotificationMessage) -> None:
        raise NotImplementedError


class LoggingNotifier(Notifier):
    def send(self, message: NotificationMessage) -> None:
        _LOGGER.warning(
            "Notification (dry-run) | %s",
            log_fields(
                subject=message.subject,
                recipients=",".join(message.recipients) or "-",
                body=message.body,
            ),
        )
