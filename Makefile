.PHONY: all build ci clean test

ci: lint test build ## Run only ci targets that lint/build/test the code

all: clean install_tools update lint_fix doc ci benchmarks ## Run all the targets including the ones that generate files/docs etc ...

help:  ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m  %s\n", $$1    , $$2}'

doc:  ## Build source and api documentation
	build/docs.sh

lint: install_tools ## Run golang linters in checking mode
	build/lint.sh

lint_fix: ## Run golang linters in fix mode
	build/lint_fix.sh

test:  install_tools ## Run unit-tests
	LOG_LEVEL=trace build/unit_tests.sh

benchmarks:
	build/benchmarks.sh

build:
	build/build.sh

clean:  ## Clean up the temporary files
	rm -rf vendor test-results

update: ## updates all the go module versions
	build/go_update.sh

install_tools:
	build/install_tools.sh

mlint: ## Run miscellaneous md/yaml linters in view-only mode
	yamllint *.yaml .github/workflows/*.yml || true
	mdl -s .mdl_style.rb *.md docs/*.md || true
