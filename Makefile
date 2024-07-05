CC = gcc
CFLAGS = -pthread -ansi -pedantic -Wall
TARGET = pagerank
SRC = pagerank.c

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

clean:
	rm -f $(TARGET) pagerank.csv
