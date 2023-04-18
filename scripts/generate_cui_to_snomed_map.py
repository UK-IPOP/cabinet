import lzma
from pathlib import Path
from typing import Iterable
import orjson
from rich.console import Console
from tqdm import tqdm


console = Console()

# type aliases because I'm lazy
CONCEPT_MAP = dict[str, str]
LINE_PARTS = list[str]


def load_lines(path: Path) -> Iterable[LINE_PARTS]:
    with open(path, "r") as f:
        for line in f:
            yield line.split("|")


def filter_lines(parts: LINE_PARTS) -> None | LINE_PARTS:
    if parts[11] != "SNOMEDCT_US":
        return None
    if parts[6] != "Y":
        return None
    if parts[2] != "P":
        return None
    if parts[12] != "PT":
        return None
    if parts[4] != "PF":
        return None
    return parts


def make_maps(lines: Iterable[LINE_PARTS]) -> CONCEPT_MAP:
    cui_to_snomed: CONCEPT_MAP = {}
    for line_parts in tqdm(
        filter(None, map(filter_lines, lines)), desc="Making Maps..."
    ):
        cui = line_parts[0]
        scui = line_parts[13]
        cui_to_snomed[cui] = scui
    return cui_to_snomed


def write_map_to_file(cui_map: CONCEPT_MAP) -> None:
    dest_dir = Path().cwd() / "src" / "cabinet" / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)
    cui_map_bytes = orjson.dumps(cui_map)
    with lzma.open(dest_dir / "cui_to_snomed.xz", "wb") as f:
        f.write(cui_map_bytes)
    console.log(f"[green]Wrote [blue]{dest_dir / 'cui_to_snomed.xz'}")


def main():
    lines = load_lines(Path().cwd().parent / "knowledge_base_data" / "MRCONSO.RRF")
    cui_to_snomed = make_maps(lines)
    console.log("[yellow]Writing maps...")
    write_map_to_file(cui_to_snomed)
    console.log("[green]Done!")


if __name__ == "__main__":
    main()
