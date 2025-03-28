ifeq ($(mode),debug)
	CFLAGS := -g -std=c++11
	BUILD_DIR = ./build/debug
	TARGET = nitcbase-debug
else
	CFLAGS:= -std=c++11
	TARGET = nitcbase
	BUILD_DIR = ./build
endif

SUBDIR =FrontendInterface Frontend Algebra Schema BlockAccess BPlusTree Cache Buffer Disk_Class

HEADERS = $(wildcard define/*.h $(foreach fd, $(SUBDIR), $(fd)/*.h))
SRCS = $(wildcard main.cpp $(foreach fd, $(SUBDIR), $(fd)/*.cpp))
OBJS = $(addprefix $(BUILD_DIR)/, $(SRCS:cpp=o))

$(TARGET): $(OBJS)
	g++ $(CFLAGS) -o $@ $(OBJS) -lreadline

$(BUILD_DIR)/%.o: %.cpp $(HEADERS)
	mkdir -p $(@D)
	g++ $(CFLAGS) -o $@ -c $<

clean:
	rm -rf $(BUILD_DIR)/*