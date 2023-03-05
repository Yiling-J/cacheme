.PHONY: test
test:
	poetry run pytest --benchmark-skip

.PHONY: testx
testx:
	poetry run pytest --benchmark-skip -x

.PHONY: benchmark
benchmark:
	poetry run pytest --benchmark-only

.PHONY: lint
lint:
	poetry run mypy --check-untyped-defs --ignore-missing-imports .

.PHONY: trace
trace:
	poetry run python -m benchmarks.trace
