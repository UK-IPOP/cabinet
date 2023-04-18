import pytest

from cabinet.umls_drawer.knowledge_base import Knowledge


@pytest.fixture
def knowledge() -> Knowledge:
    return Knowledge()


def test_convert(knowledge: Knowledge) -> None:
    assert knowledge.convert("C0011892") == "60881009"


def test_tree_parents(knowledge: Knowledge) -> None:
    assert knowledge.convert("138875005") == None
