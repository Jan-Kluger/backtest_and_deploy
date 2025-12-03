CXX = clang++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
LDFLAGS = -lpq

# Homebrew paths for libpq
CXXFLAGS += -I/opt/homebrew/opt/libpq/include
LDFLAGS += -L/opt/homebrew/opt/libpq/lib

TARGET = price
SRC = main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean



