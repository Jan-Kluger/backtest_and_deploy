BUILD_DIR := _build

.PHONY: all build configure configure_lsp compilecommands post_build clean

all: build

build: configure
	cmake --build $(BUILD_DIR)
	$(MAKE) post_build

compilecommands: configure_lsp
	cmake --build $(BUILD_DIR)
	$(MAKE) post_build
	ln -sf $(BUILD_DIR)/compile_commands.json compile_commands.json

configure:
	cmake -S . -B $(BUILD_DIR)

configure_lsp:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

post_build:
	cp example.py $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR) compile_commands.json

