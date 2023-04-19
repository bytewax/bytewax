"""Low-level recovery interfaces.

"""

import argparse
from pathlib import Path

from .bytewax import init_db_dir, RecoveryConfig  # noqa: F401

__all__ = [
    "RecoveryConfig",
    "init_db_dir",
]


def _parse_args():
    parser = argparse.ArgumentParser(
        prog="python -m bytewax.recovery",
        description="Create and init a set of empty recovery partitions.",
        epilog="""You can move these around so that each partition is
        accessible by at least one worker. See the `bytewax.recovery`
        module docstring for more info.""",
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
    parser.add_argument(
        "dataflow_id",
        type=str,
        help="Dataflow ID to label these recovery partitions with; use the "
        "same ID when creating a dataflow",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    init_db_dir(args.db_dir, args.part_count, args.dataflow_id)
