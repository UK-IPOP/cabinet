


from dataclasses import dataclass


@dataclass
class MetaMap:
    """Class for running MetaMap on a text string."""

    input_command_list: list[str]
    metamap_command_list: list[str]


