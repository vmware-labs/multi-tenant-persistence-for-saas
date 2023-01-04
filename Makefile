.PHONY: build
ci: lint unit_tests build ## Run only ci targets that lint/build/test the code

all: clean update lint_fix doc ci benchmarks ## Run all the targets including the ones that generate files/docs etc ...

help:  ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m  %s\n", $$1    , $$2}'

doc:  ## Build source and api documentation
	build/docs.sh

lint: install_tools ## Run golang linters in checking mode
	build/lint.sh

lint_fix: ## Run golang linters in fix mode
	build/lint_fix.sh

unit_tests:  install_tools ## Run unit-tests
	build/unit_tests.sh

benchmarks:
	build/benchmarks.sh

build:
	build/build.sh

clean:  ## Clean up the temporary files
	rm -rf vendor test-results

update: $(TARGETS) ## updates all the go module versions
	build/go_update.sh

install_tools:
	build/install_tools.sh

mdlint:
	mdl -s .mdl_style.rb *.md docs/*.md
