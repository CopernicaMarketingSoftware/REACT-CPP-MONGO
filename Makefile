PREFIX                  = /usr
INCLUDE_DIR             = ${PREFIX}/include/reactcpp
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
		mkdir -p ${INCLUDE_DIR}/mongo
		mkdir -p ${LIBRARY_DIR}
		cp -f mongo.h ${INCLUDE_DIR}
		cp -fr include/* ${INCLUDE_DIR}/mongo
		cp -f src/libreactcpp-mongo.so ${LIBRARY_DIR}
		cp -f src/libreactcpp-mongo.a ${LIBRARY_DIR}
