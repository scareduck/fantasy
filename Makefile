BIN    = $(HOME)/bin
SHELLS = scripts/push_to_droplet.sh

.PHONY: install
install:
	@mkdir -p $(BIN)
	@for f in $(SHELLS); do \
	    install -m 755 $$f $(BIN)/$$(basename $$f); \
	    echo "Installed $$f -> $(BIN)/$$(basename $$f)"; \
	done
