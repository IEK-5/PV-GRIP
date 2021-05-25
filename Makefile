submodule:
	git submodule update --init --recursive

init: submodule
	git submodule foreach 'make || echo "Failed to run make!"'

	mkdir -p temp
	TMPDIR=./temp pip3 install -r requirements.txt
	TMPDIR=./temp pip3 install -e .
	rmdir temp

	./scripts/install_smrender.sh
	./scripts/install_ssdp.sh


.PHONY: init
