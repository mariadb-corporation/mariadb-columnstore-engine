#******************************************************************************************
# $Id: Makefile 3465 2013-12-04 17:17:23Z dhall $
#
# Copyright (C) 2009-2013 Calpont Corporation
# All rights reserved
#*****************************************************************************************/
include ../../rules.mak
CPPFLAGS=-DTEST2
CXXFLAGS=-g -O0
LDFLAGS=

LINCLUDES=winport.h

install: bootstrap

bootstrap:
	$(INSTALL) $(LINCLUDES) $(INSTALL_ROOT_INCLUDE)

docs:

clean:
	rm -f idb_getopt *.o core

test: idbgetopt
	./idbgetopt


