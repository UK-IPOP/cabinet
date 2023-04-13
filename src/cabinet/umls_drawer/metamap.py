from __future__ import annotations

import sys

if sys.platform == "win32":
    raise ImportError("This module is not supported on Windows")

import enum
import importlib
import os
import time
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Literal

from pydantic import BaseModel, PrivateAttr, validate_arguments, validator

from cabinet.utils import console

# this is great and is the easiest working thing.
# it also requires no dependencies and can be made into a function/class taking the metamap binary path
# (the folder technically so we can start servers) and the input text
# further we can just put the output directly and only need a switch for JSOn vs MMI


def check_metamap_servers_status() -> bool:
    """Check if MetaMap servers are running."""
    pse_command = Popen(["ps", "-ef"], stdout=PIPE)
    grep_command = Popen(["grep", "java"], stdin=pse_command.stdout, stdout=PIPE)
    output, err = grep_command.communicate()
    if err is not None:
        raise Exception(err)
    results = output.decode("utf-8")
    if "taggerServer" in results and "wsd.server.DisambiguatorServer" in results:
        return True
    return False


class MMOutputType(enum.Enum):
    MMI = "mmi"
    JSON = "json"


def run_process_command(command: Popen[bytes]) -> str:
    """Run a process command."""
    output, err = command.communicate()
    if err is not None:
        raise Exception(err)
    return output.decode("utf-8")


class MetaMap(BaseModel):
    """Class for running MetaMap on a text string."""

    metamap_location: Path
    _initialized: bool = PrivateAttr(default=False)

    @validator("metamap_location")
    def check_metamap_location(cls, v: str) -> Path:
        path = Path(v).expanduser()
        if path.exists() is False:
            raise FileNotFoundError(
                "`metamap_location` does not exist. Please check the path."
            )
        elif path.is_dir() is False:
            raise NotADirectoryError(
                "`metamap_location` is not a directory. Please check the path. We are expecting the path to the `public_mm` directory."
            )
        return path

    def initialize(self) -> None:
        """Initialize MetaMap."""
        # use generic "metamap" so supports any version
        # but make check here that not older than 2016
        # if older than 2016v2, raise error
        # check initialization cache-file- to see if already initialized
        # TODO: WE NEED a check here to avoid restarting servers and crashing
        if check_metamap_servers_status() is True:
            console.print(
                "[green bold]INFO:[/] [green]MetaMap servers are already running.[/]"
            )
            self._initialized = True
            return None
        else:
            console.print(
                "[yellow bold]WARNING:[/] [yellow]MetaMap servers are not running. Starting servers now.[/]"
            )
            skrmed_server = self.metamap_location / "bin" / "skrmedpostctl"
            wsd_server = self.metamap_location / "bin" / "wsdserverctl"
            os.system(skrmed_server.as_posix() + " start")
            os.system(wsd_server.as_posix() + " start")
            with console.status(
                "Waiting for servers...", spinner="dots", spinner_style="yellow"
            ):
                time.sleep(60)
            if check_metamap_servers_status() is True:
                console.print(
                    "[green bold]INFO:[/] [green]MetaMap servers are now running.[/]"
                )
                self._initialized = True
                return None
            else:
                console.print(
                    "[red bold]ERROR:[/] [red]MetaMap servers failed to start.[/] Please check the MetaMap installation and try again."
                )
                return None

    @validate_arguments
    def run(
        self, text: str, output_type: Literal["mmi", "json"] = "mmi"
    ) -> list[str] | str | None:
        """Run MetaMap on a text string."""
        if self._initialized is False:
            console.print(
                "[red bold]ERROR:[/] [red]MetaMap is not initialized.[/] Please try running `initialize()` first."
            )
            sys.exit(1)

        input_command = Popen(["echo", text], stdout=PIPE)
        match MMOutputType(output_type):
            case MMOutputType.MMI:
                mm_command = Popen(
                    # metamap, silent, MMI, word sense disambiguation, negation auto on for MMI
                    ["metamap", "--silent", "-N", "-y"],
                    stdin=input_command.stdout,
                    stdout=PIPE,
                )
                output = run_process_command(mm_command)
                # skip the first line which is command
                return output.splitlines()[1:]
            case MMOutputType.JSON:
                mm_command = Popen(
                    # metamap, silent, JSON (no format), word sense disambiguation, negation
                    ["metamap", "--silent", "--JSONn", "-y", "--negex"],
                    stdin=input_command.stdout,
                    stdout=PIPE,
                )
                output = run_process_command(mm_command)
                return output


# write a decorator to check if pandas is installed
# if not installed, raise an error
# if installed, run the function
# @check_pandas
# def create_dataframe(pd: ModuleType):
#     return pd.DataFrame()
def check_pandas(func):
    def wrapper(*args, **kwargs):
        try:
            pd = importlib.import_module("pandas")
        except ImportError:
            raise ImportError("pandas is required to use this function")
        return func(*args, **kwargs, pd=pd)

    return wrapper


if __name__ == "__main__":
    from tqdm import tqdm
    from tqdm.contrib.concurrent import thread_map

    mm = MetaMap(metamap_location=Path("~/public_mm"))
    mm.initialize()
    result = mm.run("lung cancer", output_type="mmi")
    print(result)

    for item in tqdm(["lung cancer", "heart attack"] * 100):
        r = mm.run(item)

    # TODO: test parallelism on this... it relies on WSD server so not sure if it will help...
    # this seems to be faster 🙂 but still needs checking
    results = thread_map(mm.run, ["lung cancer", "heart attack"] * 100, max_workers=4)

    # default worker count
    results = thread_map(mm.run, ["lung cancer", "heart attack"] * 100)

    # more workers
    results = thread_map(mm.run, ["lung cancer", "heart attack"] * 100, max_workers=40)
