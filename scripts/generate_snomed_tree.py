# now generate snomed tree from snomed files
# follow similar pattern as `generate_cui_to_snomed_map.py`


import lzma
from pathlib import Path
from typing import Iterable
import orjson
from rich.console import Console
from tqdm import tqdm


console = Console()

# type aliases because I'm lazy
LINE_PARTS = list[str]
CONCEPT_TREE = dict[str, set[str]]


def load_lines(path: Path) -> Iterable[LINE_PARTS]:
    with open(path, "r") as f:
        for line in f:
            yield line.split("\t")


def filter_lines(parts: LINE_PARTS) -> None | LINE_PARTS:
    # active and "is_a" relationship
    if parts[2] == "1" and parts[7] == "116680003":
        return parts
    return None


def check_tree():
    assert True


def make_tree(lines: Iterable[LINE_PARTS]) -> CONCEPT_TREE:
    tree: CONCEPT_TREE = {}
    for line_parts in tqdm(
        filter(None, map(filter_lines, lines)), desc="Making tree..."
    ):
        source = line_parts[4]
        target = line_parts[5]
        source_set = tree.get(source, set())
        source_set.add(target)
        tree[source] = source_set
    return tree


def write_map_to_file(tree: CONCEPT_TREE) -> None:
    dest_dir = Path().cwd() / "src" / "cabinet" / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)
    tree_bytes = orjson.dumps({k: list(v) for k, v in tree.items()})
    with lzma.open(dest_dir / "snomed_tree.xz", "wb") as f:
        f.write(tree_bytes)
    console.log(f"[green]Wrote [blue]{dest_dir / 'snomed_tree.xz'}")


def main():
    lines = load_lines(
        Path().cwd().parent
        / "knowledge_base_data"
        / "SnomedCT_USEditionRF2_PRODUCTION_20220901T120000Z"
        / "Snapshot"
        / "Terminology"
        / "sct2_Relationship_Snapshot_US1000124_20220901.txt"
    )
    tree = make_tree(lines)
    console.log("[yellow]Writing tree...")
    write_map_to_file(tree)
    console.log("[green]Done!")


if __name__ == "__main__":
    main()
