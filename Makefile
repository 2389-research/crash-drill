PYTHON ?= python3
PIPELINE := $(PYTHON) examples/spool/pipeline.py
INSPECT  := $(PYTHON) examples/spool/inspect.py
DRILL    := $(PYTHON) drill/drill.py --pipeline "$(PIPELINE)" --inspect "$(INSPECT)"

.PHONY: help drill sabotage check clean

help:
	@echo "make drill      run the drill against the intact reference pipeline (must pass)"
	@echo "make sabotage   run the same drill against four broken spools (must fail)"
	@echo "make check      both, in that order: the gate"
	@echo "make clean      remove __pycache__"

drill:
	$(DRILL)

sabotage:
	./scripts/sabotage.sh

check: drill sabotage
	@echo "check: the drill passes an intact spool and fails a broken one"

clean:
	find . -name "*.pyc" -delete
	find . -name __pycache__ -type d -empty -delete
