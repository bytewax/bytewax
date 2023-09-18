"""Connectors for writing local-first demo dataflows."""
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition


@dataclass
class _RandomMetricPartition(StatefulSourcePartition):
    metric_name: str
    interval: timedelta
    awake_at: Optional[datetime]

    def next_batch(self):
        self.awake_at = datetime.now(tz=timezone.utc) + self.interval

        value = random.randrange(0, 10)
        return [(self.metric_name, value)]

    def next_awake(self):
        return self.awake_at

    def snapshot(self):
        return self.awake_at


@dataclass
class RandomMetricSource(FixedPartitionedSource):
    metric_name: str
    interval: timedelta = timedelta(seconds=0.7)

    def list_parts(self):
        return [self.metric_name]

    def build_part(self, for_part, resume_state):
        awake_at = resume_state
        return _RandomMetricPartition(for_part, self.interval, awake_at)
