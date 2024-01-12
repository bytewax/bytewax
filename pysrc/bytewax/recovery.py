"""Failure recovery."""

import argparse
from pathlib import Path

from bytewax._bytewax import (
    InconsistentPartitionsError,
    MissingPartitionsError,
    NoPartitionsError,
    RecoveryConfig,
    init_db_dir,
)

__all__ = [
    "InconsistentPartitionsError",
    "NoPartitionsError",
    "MissingPartitionsError",
    "RecoveryConfig",
    "init_db_dir",
]


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.recovery",
        description="Create and init a set of empty recovery partitions.",
        epilog="""See the `bytewax.recovery` module docstring for more
        info.""",
    )
    parser.add_argument(
        "db_dir",
        type=Path,
        help="Local directory to create partitions in",
    )
    parser.add_argument(
        "part_count",
        type=int,
        help="Number of partitions to create",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    init_db_dir(args.db_dir, args.part_count)
