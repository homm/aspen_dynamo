.PHONY: lint
lint:
	isort ./aspen_dynamo ./tests
	flake8 ./aspen_dynamo ./tests
	pyright ./aspen_dynamo ./tests

.PHONY: test
test:
	pytest ./tests --cov=aspen_dynamo
