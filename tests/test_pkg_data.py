from pathlib import Path
import pytest

from cabinet.umls_drawer import Knowledge


@pytest.fixture
def knowledge() -> Knowledge:
    return Knowledge()


@pytest.fixture
def active_concepts() -> set[str]:
    active_concepts: set[str] = set()
    with open(
        Path().cwd().parent
        / "knowledge_base_data"
        / "SnomedCT_USEditionRF2_PRODUCTION_20220901T120000Z"
        / "Snapshot"
        / "Terminology"
        / "sct2_Concept_Snapshot_US1000124_20220901.txt",
        "r",
    ) as f:
        for line in f:
            parts = line.split("\t")
            if parts[2] == "1":
                active_concepts.add(parts[0])
    return active_concepts


def test_snomed_tree(knowledge: Knowledge, active_concepts: set[str]) -> None:
    tree_concepts = set(knowledge._snomed_tree.keys())
    # manually add root concept here to pass tests
    # this is okay because
    # this should NOT be in the keys of the tree because it is not a child (is_a) :)
    tree_concepts.add("138875005")
    assert tree_concepts.issubset(active_concepts), "Tree keys not in active concepts"
    assert active_concepts.issubset(tree_concepts), "Active concepts not in tree keys"
    assert len(tree_concepts) == len(
        active_concepts
    ), f"Tree keys length {len(tree_concepts):,} != active concepts length {len(active_concepts):,}"
    for value_set in knowledge._snomed_tree.values():
        tree_concepts.update(value_set)
    assert tree_concepts.issubset(active_concepts), "Tree values not in active concepts"
    assert active_concepts.issubset(tree_concepts), "Tree values not in active concepts"
    assert len(tree_concepts) == len(active_concepts), "Tree values != active concepts"
    for k, v in knowledge._snomed_tree.items():
        assert len(v) > 0, f"Empty value set for key {k}"
