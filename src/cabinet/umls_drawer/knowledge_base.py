"""This module contains the knowledge base class.


Currently it simply loads the concept conversion map and SNOMED tree from disk
and provides a few functions to access them.

In the future, we want to support tree traversal specifically. This will
allow us to get paths to the root, paths to any given terminal node, and paths 
to common ancestors/children. We can do this in a recursive way, but the exact 
implementation is still undecided for how to best handle the interface. 

We also want to support "prettifying" these paths using string formatting such as 
'/' or '->'. This will allow us to get paths like 'A/B/C' or 'A->B->C' instead of
['A', 'B', 'C'].
"""

# TODO: Add support for tree traversal.

from importlib import resources
import lzma
import orjson

from pydantic import BaseModel, PrivateAttr

CONCEPT_MAP = dict[str, str]
TREE = dict[str, set[str]]


def load_cui_map() -> CONCEPT_MAP:
    package_dir = resources.files("cabinet")
    data_path = package_dir.joinpath("data").joinpath("cui_to_snomed.xz")
    with resources.as_file(data_path) as f:
        with lzma.open(f, "rb") as f:
            file_bytes = f.read()
    data = orjson.loads(file_bytes)
    return data


def load_snomed_tree() -> TREE:
    package_dir = resources.files("cabinet")
    data_path = package_dir.joinpath("data").joinpath("snomed_tree.xz")
    with resources.as_file(data_path) as f:
        with lzma.open(f, "rb") as f:
            file_bytes = f.read()
    data = orjson.loads(file_bytes)
    return data


class Knowledge(BaseModel):
    """A class to hold the knowledge base.

    This class loads the data from disk for you and provides a few functions to
    access the concept map and SNOMED tree.
    """

    _cui_to_snomed: CONCEPT_MAP = PrivateAttr(default_factory=dict)
    """A map from CUI to SNOMED code."""
    _snomed_tree: TREE = PrivateAttr(default_factory=dict)
    """A map from SNOMED code to its parents."""

    def __init__(self):
        """Load the data from disk."""
        super().__init__()
        self._cui_to_snomed = load_cui_map()
        self._snomed_tree = load_snomed_tree()

    def convert(self, cui: str) -> str | None:
        """Convert a CUI to a SNOMED code.

        Will return None if the CUI is not in the map. This will occur if it is the
        root SNOMED concept (138875005) as it has no parents.

        Args:
            cui (str): The CUI to convert.

        Returns:
            str | None: The SNOMED code, or None if the CUI is not in the map. None will
            occur if it is the root SNOMED concept as it has no parents.
        """
        return self._cui_to_snomed.get(cui, None)

    def tree_get_parents(self, sctid: str) -> set[str] | None:
        """Get the parents of a SNOMED code.

        Args:
            sctid (str): The SNOMED code to get the parents of.

        Returns:
            set[str] | None: The parents of the SNOMED code, or None if the SNOMED code
            is not in the tree.
        """
        return self._snomed_tree.get(sctid, None)

    def tree_get_children(self, sctid: str) -> set[str] | None:
        """Get the children of a SNOMED code.

        Args:
            sctid (str): The SNOMED code to get the children of.

        Returns:
            set[str] | None: The children of the SNOMED code, or None if the SNOMED code
            has no children.
        """
        return set([k for k, v in self._snomed_tree.items() if sctid in v])
