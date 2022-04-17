#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#


#---------------------build config-------------------------

DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?= -I/home/hank/.local/include/rocksdb
EXTRA_LDFLAGS ?= -L/home/hank/.local/lib -lsnappy -lz -llz4 -lbz2 -lzstd -ldl

BIND_ROCKSDB ?= 1

#----------------------------------------------------------

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_ROCKSDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard rocksdb/*.cc)
endif

CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
DEPS += $(SOURCES:.cc=.d)
EXEC = ycsb

all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d : %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete
	$(RM) $(EXEC)

.PHONY: clean
