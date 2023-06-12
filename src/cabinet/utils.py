"""Utility functions and classes for the cabinet package.

Mostly used to initialize the environment and console.
"""
import os

from rich import pretty
from rich.console import Console
import dotenv


dotenv.load_dotenv()
pretty.install()

c = Console()

# TODO: can we use a secure (ssl) websocket connection?
# does the un-secure work in prod?
_mode = os.getenv("MODE", "PROD")
if _mode == "PROD":
    _API_URL = "http://127.0.0.1"
    _WS_URL = "ws://127.0.0.1"
elif _mode == "DEV":
    os.environ["NO_PROXY"] = "127.0.0.1"
    _API_URL = "http://127.0.0.1:8000"
    _WS_URL = "ws://127.0.0.1:8000"
else:
    raise ValueError(f"MODE must be either PROD or DEV, not {_mode}")
