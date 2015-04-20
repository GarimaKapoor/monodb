
# compile environment
CC = g++

# compile path
ROOT_PATH = .
BIN_PATH = $(ROOT_PATH)/bin
SRC_PATH = $(ROOT_PATH)/src
TEST_PATH = $(ROOT_PATH)/test

# compile c++ source file
$(BIN_PATH)/%.o : $(SRC_PATH)/DB/%.cpp
	$(CC) -c $< -o $@

$(BIN_PATH)/%.o : $(SRC_PATH)/%.cpp
	$(CC) -c $< -o $@

BIN = \
	$(BIN_PATH)/Data.o \
	$(BIN_PATH)/Btree.o

all : $(PROTOBUF) $(PACKET) $(BIN)
#	$(CC) Source/Boot.cpp -lpthread -lprotobuf -o $(BIN_PATH)/monodb $(BIN)

clean :
	rm -f $(ROOT_PATH)/analysis
	rm -f $(ROOT_PATH)/gmon.out
	rm -f $(ROOT_PATH)/default.conf
	rm -f $(ROOT_PATH)/SYSTEM
	rm -f $(BIN_PATH)/monodb
	rm -f $(BIN_PATH)/*.o
	rm -f $(TEST_PATH)/log/*
	rm -f $(TEST_PATH)/temp/Data/*
	rm -f $(TEST_PATH)/temp/Btree/*
	rm -f $(TEST_PATH)/smarttest

memtest : $(PROTOBUF) $(PACKET) $(BIN)
	$(CC) test/Test.cpp -lpthread -o $(TEST_PATH)/smarttest $(BIN)
	valgrind test/smarttest

smarttest : $(PROTOBUF) $(PACKET) $(BIN)
	$(CC) test/Test.cpp -pg -lpthread -o $(TEST_PATH)/smarttest $(BIN)
	$(TEST_PATH)/smarttest
	gprof -p $(TEST_PATH)/smarttest gmon.out > analysis
