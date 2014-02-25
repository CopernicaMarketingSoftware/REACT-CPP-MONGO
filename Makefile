PREFIX                  = /usr
INCLUDE_DIR             = ${PREFIX}/include
LIBRARY_DIR             = ${PREFIX}/lib

all:
		$(MAKE) -C src all

static:
		$(MAKE) -C src static

shared:
		$(MAKE) -C src shared

clean:
		$(MAKE) -C src clean

install:
		mkdir -p ${INCLUDE_DIR}/mongocpp
		mkdir -p ${LIBRARY_DIR}
		cp -f mongocpp.h ${INCLUDE_DIR}
		cp -fr include/* ${INCLUDE_DIR}/mongocpp
		cp -f src/libmongocpp.so ${LIBRARY_DIR}
		cp -f src/libmongocpp.a ${LIBRARY_DIR}
