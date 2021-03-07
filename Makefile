submodule:
	git submodule update --init --recursive

init: submodule
	git submodule foreach 'make'
	mkdir -p temp

	TMPDIR=./temp pip3 install -r requirements.txt
	TMPDIR=./temp pip3 install -e .
	rmdir temp

	./scripts/install_smrender.sh


.PHONY: init
