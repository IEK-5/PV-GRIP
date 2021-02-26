init:
	git submodule update --init --recursive
	git submodule foreach 'make'
	mkdir -p temp

	TMPDIR=./temp pip3 install -r requirements.txt
	TMPDIR=./temp pip3 install -e .
	rmdir temp

.PHONY: init
