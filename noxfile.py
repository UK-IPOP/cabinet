import tempfile
from typing import Any

# add type ignores since these are global imports
from nox import session  # type: ignore
from nox.sessions import Session  # type: ignore


@session(python="3.11")
def lint(session):
    session.run("poetry", "install", "--with=dev", external=True)
    session.run("black", ".", "--check")
    session.run("ruff", ".")
    session.run(
        "mypy",
        ".",
    )


@session(python=["3.9", "3.10", "3.11"])
def test(session):
    # ! In order for this to work, you need to have a local instance of the API running
    # ! and the MODE environment variable set to DEV in your .env file AND you need to
    # ! have a locally installed MetaMap instance.
    # TODO: Write tests that check for invalid states and the appropriate error messages
    # but for now we assume that the API is running and MetaMap is installed.
    # this will be good practice for integration tests... but to be honest
    # probably won't hold a lot of value right now without CI for windows as well...
    session.run("poetry", "install", external=True)
    session.run("pytest")
