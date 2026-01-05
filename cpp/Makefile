BUILD_DIR := _build

.PHONY: all build configure configure_lsp compilecommands post_build clean run

all: build

build: configure
	cmake --build $(BUILD_DIR)

compilecommands: configure_lsp
	cmake --build $(BUILD_DIR)
	ln -sf $(BUILD_DIR)/compile_commands.json compile_commands.json

configure:
	cmake -S . -B $(BUILD_DIR)

configure_lsp:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

run:
	PYTHONPATH=$(BUILD_DIR) python3 example.py

clean:
	rm -rf $(BUILD_DIR) compile_commands.json

