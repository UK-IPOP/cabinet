# add type ignores since these are global imports
import nox  # type: ignore


@nox.session(python="3.11", reuse_venv=True)
def lint(session):
    # we need the main dependencies to run mypy so it doesn't fail
    # on imports
    session.run("poetry", "install", "--only", "dev", external=True)
    session.run("black", ".", "--check")
    session.run("ruff", ".")
    # ... , but we ignore that for now and use `--ignore-missing-imports`
    session.run(
        "mypy",
        ".",
        "--ignore-missing-imports",
    )


# TODO: python 3.9 fails on match statement
@nox.session(python=["3.9", "3.10", "3.11"], reuse_venv=True)
def test(session):
    # ! In order for this to work, you need to have a local instance of the API running
    # ! and the MODE environment variable set to DEV in your .env file AND you need to
    # ! have a locally installed MetaMap instance.
    # TODO: Write tests that check for invalid states and the appropriate error messages
    # but for now we assume that the API is running and MetaMap is installed.
    # this will be good practice for integration tests... but to be honest
    # probably won't hold a lot of value right now without CI for windows as well...
    session.run("poetry", "install", "--without", "docs,dev", external=True)
    session.run("pytest")
