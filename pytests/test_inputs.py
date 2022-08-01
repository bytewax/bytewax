from bytewax.inputs import distribute


def test_distribute():
    inp = ["a", "b", "c"]

    out1 = distribute(inp, 0, 2)

    assert list(out1) == ["a", "c"]

    out2 = distribute(inp, 1, 2)

    assert list(out2) == ["b"]
