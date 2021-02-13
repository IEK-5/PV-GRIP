init:
	git submodule update --init --recursive
	git submodule foreach 'make'
	mkdir -p temp

	TMPDIR=./temp pip install -r requirements.txt
	TMPDIR=./temp pip install -e .
	rmdir temp

.PHONY: init
