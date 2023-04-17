default:
    just --list


format:
    poetry run black .

lint:
    poetry run ruff .
    poetry run mypy .

test:
    poetry run pytest

test-cov:
    poetry run pytest --cov=src/cabinet --cov-report=term-missing

report-coverage:
    poetry run poetry run pytest --cov=src/cabinet --cov-report=term-missing --cov-report=html
    open htmlcov/index.html

clean-pkg-data:
    rm -r src/cabinet/data/