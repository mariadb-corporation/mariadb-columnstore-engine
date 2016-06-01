include ../rules.mak
#******************************************************************************************
# $Id: Makefile 3121 2013-06-26 15:31:13Z rdempsey $
#
# Copyright (C) 2016 MariaDB, Corporation
# All rights reserved
#*****************************************************************************************/
TARGETS = bootstrap clean test docs coverage leakcheck install

.PHONY: all $(TARGETS)

all:
	$(MAKE) -C mcsadmin
	$(MAKE) -C traphandler
	$(MAKE) -C serverMonitor
	$(MAKE) -C postConfigure
	$(MAKE) -C columnstoreDB
	$(MAKE) -C sendtrap
	$(MAKE) -C columnstoreSupport
$(TARGETS):
	$(MAKE) -C mcsadmin $@
	$(MAKE) -C traphandler $@
	$(MAKE) -C serverMonitor $@
	$(MAKE) -C postConfigure $@
	$(MAKE) -C columnstoreDB $@
	$(MAKE) -C sendtrap $@
	$(MAKE) -C columnstoreSupport $@
