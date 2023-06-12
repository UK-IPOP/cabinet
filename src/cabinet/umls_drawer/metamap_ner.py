# """This module is for working with MetaMap.

# MetaMap is a tool for extracting structured information from biomedical text provided
# by the National Library of Medicine (NLM). MetaMap is a text processing
# engine is a natural language processing (NLP) system that uses a set of rules and
# heuristics to identify and extract concepts from unstructured text.

# You can download MetaMap by purchasing a NLM License (or accessing via your institution)
# and downloading the binary from [here](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/run-locally/MainDownload.html).

# For more information, see the [MetaMap documentation](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap.html).
# """
# from __future__ import annotations


# import enum
# import os
# import sys
# import time
# from pathlib import Path
# from subprocess import PIPE, Popen
# from typing import Iterator, Literal

# from pydantic import BaseModel, PrivateAttr, validate_arguments, validator

# from cabinet.utils import c


# def _check_metamap_servers_status() -> bool:
#     """Check if MetaMap servers are running."""
#     pse_command = Popen(["ps", "-ef"], stdout=PIPE)
#     grep_command = Popen(["grep", "java"], stdin=pse_command.stdout, stdout=PIPE)
#     output, err = grep_command.communicate()
#     if err is not None:
#         raise Exception(err)
#     results = output.decode("utf-8")
#     if "taggerServer" in results and "wsd.server.DisambiguatorServer" in results:
#         return True
#     return False


# class MMOutputType(enum.Enum):
#     """Enum for MetaMap output types."""

#     MMI = "mmi"
#     """Fielded MMI output format, see [here](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/MMI_Output.pdf) for more information."""
#     JSON = "json"
#     """Json output, see [here](https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/JSON.pdf) for more information."""


# def _run_process_command(command: Popen[bytes]) -> str:
#     """Run a process command.

#     Args:
#         command (Popen[bytes]): The command to run.

#     Returns:
#         str: The output of the command as utf-8 decoded string.
#     """
#     output, err = command.communicate()
#     if err is not None:
#         raise Exception(err)
#     return output.decode("utf-8")


# class MetaMap(BaseModel):
#     """Class for running MetaMap on a text string.

#     Args:
#         metamap_location (str): The path to the MetaMap installation. This should be the path to the `public_mm` directory.

#     Example:
#         ```python
#         from cabinet.umls_drawer import MetaMap
#         mm = MetaMap(metamap_location="/Users/username/metamap/public_mm")
#         ```
#     """

#     metamap_location: Path
#     _initialized: bool = PrivateAttr(default=False)

#     @validator("metamap_location")
#     def _check_metamap_location(cls, v: str) -> Path:
#         """Check the MetaMap location."""
#         path = Path(v).expanduser()
#         if path.exists() is False:
#             raise FileNotFoundError(
#                 "`metamap_location` does not exist. Please check the path."
#             )
#         elif path.is_dir() is False:
#             raise NotADirectoryError(
#                 "`metamap_location` is not a directory. Please check the path. We are expecting the path to the `public_mm` directory."
#             )
#         return path

#     def initialize(self) -> None:
#         """Initialize MetaMap.

#         This function must be run to start the MetaMap servers.
#         It will check if the servers are already running and if not, it will start them.
#         """
#         # use generic "metamap" so supports any version
#         # but make check here that not older than 2016
#         # if older than 2016v2, raise error
#         # check initialization cache-file- to see if already initialized
#         if _check_metamap_servers_status() is True:
#             c.print(
#                 "[green bold]INFO:[/] [green]MetaMap servers are already running.[/]"
#             )
#             self._initialized = True
#             return None
#         else:
#             c.print(
#                 "[yellow bold]WARNING:[/] [yellow]MetaMap servers are not running. Starting servers now.[/]"
#             )
#             skrmed_server = self.metamap_location / "bin" / "skrmedpostctl"
#             wsd_server = self.metamap_location / "bin" / "wsdserverctl"
#             os.system(skrmed_server.as_posix() + " start")
#             os.system(wsd_server.as_posix() + " start")
#             with c.status(
#                 "Waiting for servers...", spinner="dots", spinner_style="yellow"
#             ):
#                 time.sleep(60)
#             if _check_metamap_servers_status() is True:
#                 c.print(
#                     "[green bold]INFO:[/] [green]MetaMap servers are now running.[/]"
#                 )
#                 self._initialized = True
#                 return None
#             else:
#                 c.print(
#                     "[red bold]ERROR:[/] [red]MetaMap servers failed to start.[/] Please check the MetaMap installation and try again."
#                 )
#                 return None

#     @validate_arguments
#     def run(
#         self, text: str, output_type: Literal["mmi", "json"] = "mmi"
#     ) -> list[str] | str | None:
#         """Run MetaMap on a text string.

#         This will return None if no results were found, otherwise the return type will
#         match the output_type argument. Future work will include returning a dataclass
#         for each result, but for now:
#             MMOutputType.JSON -> str  (the json data itself)
#             MMOutputType.MMI -> list[str] (each line of the MMI output)

#         Args:
#             text (str): The text to run MetaMap on.
#             output_type (Literal["mmi", "json"], optional): The output type. Defaults to "mmi".

#         Returns:
#             list[str] | str | None: The output of MetaMap. The type of the output depends on the `output_type` argument.

#         Example:
#             ```python
#             >>> from cabinet.umls_drawer import MetaMap, MMOutputType
#             >>> mm = MetaMap(metamap_location="/Users/username/metamap/public_mm")
#             >>> mm.initialize()
#             >>> results = mm.run(text="I have a headache.", output_type=MMOutputType.MMI)
#             >>> print(results)
#             ```
#         """
#         if self._initialized is False:
#             c.print(
#                 "[red bold]ERROR:[/] [red]MetaMap is not initialized.[/] Please try running `initialize()` first."
#             )
#             sys.exit(1)

#         input_command = Popen(["echo", text], stdout=PIPE)
#         # match MMOutputType(output_type):
#         #     case MMOutputType.MMI:
#         #         mm_command = Popen(
#         #             # metamap, silent, MMI, word sense disambiguation, negation auto on for MMI
#         #             ["metamap", "--silent", "-N", "-y"],
#         #             stdin=input_command.stdout,
#         #             stdout=PIPE,
#         #         )
#         #         output = _run_process_command(mm_command)
#         #         # skip the first line which is command
#         #         return output.splitlines()[1:]
#         #     case MMOutputType.JSON:
#         #         mm_command = Popen(
#         #             # metamap, silent, JSON (no format), word sense disambiguation, negation
#         #             ["metamap", "--silent", "--JSONn", "-y", "--negex"],
#         #             stdin=input_command.stdout,
#         #             stdout=PIPE,
#         #         )
#         #         output = _run_process_command(mm_command)
#         #         return output
#         return None

#     @validate_arguments
#     def run_many(
#         self, texts: list[str], output_type: Literal["mmi", "json"] = "mmi"
#     ) -> Iterator[list[str] | str | None]:
#         """Runs MetaMap on multiple strings.

#         Calls thread_map from tqdm.contrib.concurrent to run MetaMap on multiple strings.

#         Returns an iterator that must be consumed.

#         Args:
#             texts (list[str]): The texts to run MetaMap on.
#             output_type (Literal["mmi", "json"], optional): The output type. Defaults to "mmi".

#         Returns:
#             Iterator[list[str] | str | None]: An iterator that must be consumed. The type of the output depends on the `output_type` argument.

#         Example:
#             ```python
#             >>> from cabinet.umls_drawer import MetaMap
#             >>> mm = MetaMap(metamap_location="/Users/username/metamap/public_mm")
#             >>> mm.initialize()
#             >>> results = mm.run_many(texts=["I have a headache.", "I have a fever."])
#             >>> for result in results:
#                 print(result)
#             ```
#         """
#         # local import to avoid exposing function and since only used here for now
#         from tqdm.contrib.concurrent import thread_map, process_map

#         # allow default selection of max-workers
#         return thread_map(self.run, texts)
