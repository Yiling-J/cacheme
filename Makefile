.PHONY: test
test:
	poetry run pytest --benchmark-skip

.PHONY: benchmark
benchmark:
	poetry run pytest --benchmark-only
