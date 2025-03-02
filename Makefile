.PHONY: venv install lint format clean

venv:
	python -m venv .venv
	. .venv/bin/activate && python -m pip install --upgrade pip

install: venv
	. .venv/bin/activate && python -m pip install -e ".[dev]"

lint:
	ruff check .
	mypy .

format:
	ruff format .
	ruff check --fix .

clean:
	rm -rf .mypy_cache .ruff_cache dist build __pycache__
	rm -rf .venv
	rm -rf *.egg-info **/*.egg-info
