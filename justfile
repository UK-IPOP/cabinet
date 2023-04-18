default:
    just --list


format:
    poetry run black .

lint:
    poetry run ruff .
    poetry run mypy .

test:
    poetry run pytest -v

test-cov:
    poetry run pytest -v --cov=src/cabinet --cov-report=term-missing

report-coverage:
    poetry run poetry run pytest --cov=src/cabinet --cov-report=term-missing --cov-report=html
    open htmlcov/index.html

clean-pkg-data:
    rm -r src/cabinet/data/

generate-pkg-data:
    poetry run python scripts/generate_snomed_tree.py
    poetry run python scripts/generate_cui_to_snomed_map.py

build-docs:
    poetry run sphinx-apidoc -f -o docs/source/ src/cabinet
    sphinx-build -b html docs/source/ docs/build/html
    cd docs && make latexpdf
    open docs/build/html/index.html
    cp docs/build/latex/cabinet.pdf docs
