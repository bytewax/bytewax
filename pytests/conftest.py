from multiprocess import get_context
from pytest import fixture


@fixture
def mp_ctx():
    return get_context("spawn")
