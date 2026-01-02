.PHONY: lint
lint:
	isort ./aspen_dynamo
	flake8 ./aspen_dynamo
	pyright ./aspen_dynamo

.PHONY: test
test:
	pytest --cov=aspen_dynamo
