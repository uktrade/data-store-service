PORT ?= 5050
TEST ?=.
COV ?= --cov
BLACK_CONFIG ?= --exclude=venv --skip-string-normalization --line-length 100
CHECK ?= --check

.PHONY: run_server
run_server: compile_assets
	exec gunicorn 'data_engineering.common.application:get_or_create()' -b 0.0.0.0:${PORT} --config 'app/config/gunicorn.conf'


.PHONY: run_dev_server
run_dev_server:
	FLASK_DEBUG=1 FLASK_APP='data_engineering.common.application:get_or_create()' flask run --host 0.0.0.0 --port ${PORT}

.PHONY: run_tests
run_tests:
	TESTING=1 pytest -p no:sugar ${TEST} ${COV}

.PHONY: run_tests_local
run_tests_local:
	USE_DOTENV=1 TESTING=1 pytest -s ${TEST}

check: flake8 black

compile_assets: install_node
	./scripts/compile_assets.sh

format: CHECK=
format: black

black:
	black ${BLACK_CONFIG} ${CHECK} .

flake8:
	flake8 .

install_node:
	./scripts/install_node.sh
