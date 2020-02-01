SHELL := /usr/bin/env bash

#######
# Help
#######

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sorted(sys.stdin):
	match = re.match(r'^([$()a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-40s %s" % (target, help))

endef
export PRINT_HELP_PYSCRIPT

help:
	@ python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)





.PHONY: create-conda-env-dev
create-conda-env-dev: .venv ## Create conda env for development purposes
.venv:
	conda create -p .venv -y --copy python=3.6 && \
	  source activate ./.venv && \
	  pip install -r requirements.txt

.PHONY: clean-conda-env-dev
clean-conda-env-dev:  ## Clean conda env built for development purposes
	rm -rf .venv

.PHONY: test
test: ## Run tests
	source activate ./.venv && \
	  python -m pytest -s --durations=0

.PHONY: clean-dist
clean-dist: ## Clean dist outputs
	rm -rf ./dist ./pyNakadi.egg-info

.PHONY: dist
dist: ## Prepare dist package
	source activate ./.venv && \
	  python setup.py sdist

.PHONY: dist-upload
dist-upload: ## Prepare dist package
	source activate ./.venv && \
	  python setup.py sdist upload