PYTHON := python3
MKDOCS := $(PYTHON) -m mkdocs

all: gen build export

gen:
	$(PYTHON) gen_api_docs.py

build:
	$(MKDOCS) build -f ../mkdocs.yml

serve:
	$(MKDOCS) serve -f ../mkdocs.yml

export:
	$(PYTHON) export_site_to_md.py

clean:
	rm -rf ../docs ../site ../documentation