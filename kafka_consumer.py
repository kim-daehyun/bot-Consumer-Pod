from __future__ import annotations

import json
from typing import Any, Dict, Generator, Iterable, Optional

from kafka import KafkaConsumer


class RawKafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topics: Iterable[str],
        group_id: str,
        auto_offset_reset: str = "latest",
    ) -> None:
        self.consumer = KafkaConsumer(
            *list(topics),
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=self._deserialize,
        )

    def _deserialize(self, value: bytes) -> Dict[str, Any]:
        return json.loads(value.decode("utf-8"))

    def iter_messages(self) -> Generator[tuple[str, Dict[str, Any]], None, None]:
        for message in self.consumer:
            yield message.topic, message.value