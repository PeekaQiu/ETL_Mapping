from __future__ import annotations

import logging
from dataclasses import dataclass


@dataclass(frozen=True)
class NotificationMessage:
    subject: str
    body: str
    recipients: list[str]


class Notifier:
    def send(self, message: NotificationMessage) -> None:
        raise NotImplementedError


class LoggingNotifier(Notifier):
    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def send(self, message: NotificationMessage) -> None:
        self._logger.warning(
            "Dry-run notification. subject=%s recipients=%s body=%s",
            message.subject,
            ",".join(message.recipients),
            message.body,
        )
