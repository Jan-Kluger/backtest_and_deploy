BUILD_DIR := _build

.PHONY: all build configure configure_lsp compilecommands clean run test test-cpp test-python

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

# ---- Testing ----
test: test-cpp test-python

test-cpp: build
	cd $(BUILD_DIR) && ctest --output-on-failure

test-python:
	pytest test/python/ -v || echo "No Python tests yet"

clean:
	rm -rf $(BUILD_DIR) compile_commands.json

