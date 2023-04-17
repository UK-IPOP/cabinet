import tempfile
from typing import Any
from nox import session
from nox.sessions import Session


def install_with_constraints(session: Session, *args: str, **kwargs: Any) -> None:
    """Install packages constrained by Poetry's lock file.

    This function is a wrapper for nox.sessions.Session.install. It
    invokes pip to install packages inside of the session's virtualenv.
    Additionally, pip is passed a constraints file generated from
    Poetry's lock file, to ensure that the packages are pinned to the
    versions specified in poetry.lock. This allows you to manage the
    packages as Poetry development dependencies.

    Arguments:
        session: The Session object.
        args: Command-line arguments for pip.
        kwargs: Additional keyword arguments for Session.install.

    """
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--with=dev",
            "--format=requirements.txt",
            f"--output={requirements.name}",
            external=True,
        )
        session.install(f"--constraint={requirements.name}", *args, **kwargs)


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
