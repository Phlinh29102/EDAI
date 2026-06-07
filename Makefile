.PHONY: install test run clean typecheck

install:
	uv sync

test:
	PYTHONPATH=src python -m pytest tests/ -x -q --tb=short

run:
	PYTHONPATH=src python main.py $(ARGS)

clean:
	rm -rf data/offline data/streaming data/features data/reports
