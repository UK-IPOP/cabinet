def cui_to_snomed(cui: str) -> str:
    ...


def tree_parents(snomed_id: str) -> list[str]:
    ...


def traverse_tree(snomed_id: str, terminal_id: str) -> list[str]:
    ...


def resolve_nearest_parent(scui1: str, scui2: str) -> str:
    ...


from importlib import resources


def load_data():
    package_dir = resources.files("cabinet")
    data_path = package_dir.joinpath("data").joinpath("test.txt")
    data1 = data_path.read_bytes()
    data2 = data_path.read_text()
    print(data1)
    print(data2)


if __name__ == "__main__":
    load_data()
