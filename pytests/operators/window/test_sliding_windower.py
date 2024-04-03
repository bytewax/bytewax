from datetime import datetime, timedelta, timezone

from bytewax.operators.window import _SlidingWindowerLogic, _SlidingWindowerState


def test_intersect_overlap_offset_divisible_by_length_bulk_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #              9:00:13
    #              I
    # [0--------)
    #      [1--------)
    #           [2--------)
    #                [3--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 13, tzinfo=timezone.utc)) == [
        1,
        2,
    ]


def test_intersect_overlap_offset_divisible_by_length_bulk_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #             8:59:57
    #             I
    # [--------3)
    #      [--------2)
    #           [--------1)
    #                [0--------)
    assert logic.intersects(datetime(2023, 3, 16, 8, 59, 57, tzinfo=timezone.utc)) == [
        -2,
        -1,
    ]


def test_intersect_overlap_offset_divisible_by_length_bulk_zero_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #              9:00:03
    #              I
    # [--------2)
    #      [--------1)
    #           [0--------)
    #                [1--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 3, tzinfo=timezone.utc)) == [
        -1,
        0,
    ]


def test_intersect_overlap_offset_divisible_by_length_bulk_zero_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #             9:00:07
    #             I
    # [--------1)
    #      [0--------)
    #           [1--------)
    #                [2--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 7, tzinfo=timezone.utc)) == [
        0,
        1,
    ]


def test_intersect_overlap_offset_divisible_by_length_edge_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                9:00:15
    #                I
    # [0--------)
    #      [1--------)
    #           [2--------)
    #                [3--------)
    #                     [4--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 15, tzinfo=timezone.utc)) == [
        2,
        3,
    ]


def test_intersect_overlap_offset_divisible_by_length_edge_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #           8:59:55
    #           I
    # [--------3)
    #      [--------2)
    #           [--------1)
    #                [0--------)
    assert logic.intersects(datetime(2023, 3, 16, 8, 59, 55, tzinfo=timezone.utc)) == [
        -2,
        -1,
    ]


def test_intersect_overlap_offset_divisible_by_length_edge_start_zero():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #           9:00:00
    #           I
    # [--------2)
    #      [--------1)
    #           [0--------)
    #                [1--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc)) == [
        -1,
        0,
    ]


def test_intersect_overlap_offset_divisible_by_length_edge_end_zero():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=5),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #           9:00:10
    #           I
    # [0--------)
    #      [1--------)
    #           [2--------)
    #                [3--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 10, tzinfo=timezone.utc)) == [
        1,
        2,
    ]


def test_intersect_overlap_offset_indivisible_by_length_bulk_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=3),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #            9:00:11
    #            I
    # [0--------)
    #    [1--------)
    #       [2--------)
    #          [3--------)
    #             [4--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 11, tzinfo=timezone.utc)) == [
        1,
        2,
        3,
    ]


def test_intersect_overlap_offset_indivisible_by_length_bulk_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=3),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #            8:59:59
    #            I
    # [--------4)
    #    [--------3)
    #       [--------2)
    #          [--------1)
    #             [0--------)
    assert logic.intersects(datetime(2023, 3, 16, 8, 59, 59, tzinfo=timezone.utc)) == [
        -3,
        -2,
        -1,
    ]


def test_intersect_overlap_offset_indivisible_by_length_bulk_zero():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=3),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #            9:00:05
    #            I
    # [--------2)
    #    [--------1)
    #       [0--------)
    #          [1--------)
    #             [2--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 5, tzinfo=timezone.utc)) == [
        -1,
        0,
        1,
    ]


def test_intersect_overlap_offset_indivisible_by_length_edge_start_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=7),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #               9:00:14
    #               I
    # [0--------)
    #        [1--------)
    #               [2--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 14, tzinfo=timezone.utc)) == [
        1,
        2,
    ]


def test_intersect_overlap_offset_indivisible_by_length_edge_start_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=7),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #        8:59:53
    #        I
    # [--------2)
    #        [--------1)
    #               [0--------)
    assert logic.intersects(datetime(2023, 3, 16, 8, 59, 53, tzinfo=timezone.utc)) == [
        -2,
        -1,
    ]


def test_intersect_overlap_offset_indivisible_by_length_edge_start_zero():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=7),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #        9:00:00
    #        I
    # [--------1)
    #        [0--------)
    #               [1--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc)) == [
        -1,
        0,
    ]


def test_intersect_overlap_offset_indivisible_by_length_edge_end_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=7),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                  9:00:17
    #                  I
    # [0--------)
    #        [1--------)
    #               [2--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 17, tzinfo=timezone.utc)) == [
        2,
    ]


def test_intersect_overlap_offset_indivisible_by_length_edge_end_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=7),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #           8:59:56
    #           I
    # [--------2)
    #        [--------1)
    #               [0--------)
    assert logic.intersects(datetime(2023, 3, 16, 8, 59, 56, tzinfo=timezone.utc)) == [
        -1,
    ]


def test_intersect_overlap_offset_indivisible_by_length_edge_end_zero():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=7),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                  9:00:10
    #                  I
    # [--------1)
    #        [0--------)
    #               [1--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 10, tzinfo=timezone.utc)) == [
        1,
    ]


def test_intersect_tumble_bulk_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=10),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                9:00:15
    #                I
    # [0--------)
    #           [1--------)
    #                     [2--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 15, tzinfo=timezone.utc)) == [
        1,
    ]


def test_intersect_tumble_bulk_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=10),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                8:59:55
    #                I
    # [--------2)
    #           [--------1)
    #                     [0--------)
    assert logic.intersects(datetime(2023, 3, 16, 8, 59, 55, tzinfo=timezone.utc)) == [
        -1
    ]


def test_intersect_tumble_bulk_zero():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=10),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                9:00:05
    #                I
    # [--------1)
    #           [0--------)
    #                     [1--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 5, tzinfo=timezone.utc)) == [0]


def test_intersect_tumble_edge_positive():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=10),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                     9:00:20
    #                     I
    # [0--------)
    #           [1--------)
    #                     [2--------)
    #                               [3--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 20, tzinfo=timezone.utc)) == [2]


def test_intersect_tumble_edge_negative():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=10),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                     8:59:50
    #                     I
    # [--------3)
    #           [--------2)
    #                     [--------1)
    #                               [0--------)
    assert logic.intersects(datetime(2023, 3, 16, 8, 59, 50, tzinfo=timezone.utc)) == [
        -1
    ]


def test_intersect_tumble_edge_zero_start():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=10),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #           9:00:00
    #           I
    # [--------1)
    #           [0--------)
    #                     [1--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc)) == [0]


def test_intersect_tumble_edge_zero_end():
    logic = _SlidingWindowerLogic(
        length=timedelta(seconds=10),
        offset=timedelta(seconds=10),
        align_to=datetime(2023, 3, 16, 9, 0, 0, tzinfo=timezone.utc),
        state=_SlidingWindowerState(),
    )

    #                     9:00:10
    #                     I
    # [--------1)
    #           [0--------)
    #                     [1--------)
    #                               [2--------)
    assert logic.intersects(datetime(2023, 3, 16, 9, 0, 10, tzinfo=timezone.utc)) == [1]
