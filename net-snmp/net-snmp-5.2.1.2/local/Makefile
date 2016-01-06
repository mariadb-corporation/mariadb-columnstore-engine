#
# Minimum environment and virtual path setup
#
SHELL		= /bin/sh
srcdir		= .
top_srcdir	= ..
VERSION		= 5.2.1.2


#
# Paths
#
prefix		= /home/builder/infinidb
exec_prefix	= /home/builder/infinidb
bindir		= ${exec_prefix}/bin
sbindir		= ${exec_prefix}/sbin
libdir		= ${exec_prefix}/lib
datadir		= ${prefix}/share
includedir	= ${prefix}/include/net-snmp
ucdincludedir	= ${prefix}/include/ucd-snmp
mandir		= ${prefix}/man
man1dir		= $(mandir)/man1
man3dir		= $(mandir)/man3
man5dir		= $(mandir)/man5
man8dir		= $(mandir)/man8
snmplibdir	= $(datadir)/snmp
mibdir		= $(snmplibdir)/mibs
persistentdir	= /var/net-snmp
INSTALL_PREFIX  = 

#
# Programs
#
INSTALL		= $(LIBTOOL) --mode=install /usr/bin/install -c
UNINSTALL	= $(LIBTOOL) --mode=uninstall rm -f
INSTALL_DATA    = ${INSTALL} -m 644
SED		= /bin/sed
LN_S		= ln -s
AUTOCONF	= :
AUTOHEADER	= :

#
# Compiler arguments
#
CFLAGS		= -g -O2 -Dlinux 
EXTRACPPFLAGS	= -x c
LDFLAGS		= -Wl,-rpath -Wl,/home/builder/infinidb/lib 
LIBTOOL		= $(SHELL) $(top_builddir)/libtool 
EXEEXT		= 

# Misc Compiling Stuff
CC	        = gcc

# ok, use libtool versioning, but not quite the way they recommend.
# The shared library version number will be:
#
#   (CURRENT-AGE).AGE.REVISION
#
# *** THIS MEANS YOU MAY NEED TO DECREMENT CURRENT FOR A NEW RELEASE ***
#
# examples:
#   C.A.R   library name
#  -------  ---------------
#   5.0.9   libxxx.so.5.0.9
#   6.1.2   libxxx.so.5.1.2
#   7.2.8   libxxx.so.5.2.8
#   6.0.0   libxxx.so.6.0.0
# 
LIBCURRENT  = 7
LIBAGE      = 2
LIBREVISION = 1

LIB_LD_CMD      = $(LIBTOOL) --mode=link $(CC) $(CFLAGS) -rpath $(libdir) -version-info $(LIBCURRENT):$(LIBREVISION):$(LIBAGE) -o
LIB_EXTENSION   = la
LIB_VERSION     =
LIB_LDCONFIG_CMD = $(LIBTOOL) --mode=finish $(libdir)
LINK		= $(LIBTOOL) --mode=link $(CC)
# RANLIB 	= ranlib
RANLIB		= :

# libtool definitions
.SUFFIXES: .c .o .lo .rc
.c.lo:
	$(LIBTOOL) --mode=compile $(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ $<
.rc.lo:
	$(LIBTOOL) --mode=compile windres -o $@ $<

# include paths
#
SRC_TOP_INCLUDES            = -I$(top_srcdir)/include
SRC_SNMPLIB_INCLUDES        = -I$(top_srcdir)/snmplib
SRC_AGENT_INCLUDES          = -I$(top_srcdir)/agent
SRC_HELPER_INCLUDES         = -I$(top_srcdir)/agent/helpers
SRC_MIBGROUP_INCLUDES       = -I$(top_srcdir)/agent/mibgroup

BLD_TOP_INCLUDES            = -I$(top_builddir)/include $(SRC_TOP_INCLUDES)
BLD_SNMPLIB_INCLUDES        = -I$(top_builddir)/snmplib $(SRC_SNMPLIB_INCLUDES)
BLD_AGENT_INCLUDES          = -I$(top_builddir)/agent $(SRC_AGENT_INCLUDES)
BLD_HELPER_INCLUDES         = -I$(top_builddir)/agent/helpers $(SRC_HELPER_INCLUDES)
BLD_MIBGROUP_INCLUDES       = -I$(top_builddir)/agent/mibgroup $(SRC_MIBGROUP_INCLUDES)

TOP_INCLUDES            = $(SRC_TOP_INCLUDES)
SNMPLIB_INCLUDES        = $(SRC_SNMPLIB_INCLUDES)
AGENT_INCLUDES          = $(SRC_AGENT_INCLUDES)
HELPER_INCLUDES         = $(SRC_HELPER_INCLUDES)
MIBGROUP_INCLUDES       = $(SRC_MIBGROUP_INCLUDES)

#
# local (scripts) directory Makefile
#
top_builddir=..



#
# stuff to install
#
OTHERINSTALL=localinstall
OTHERUNINSTALL=localuninstall

#
# local info
#
SNMPCONFPATH=/home/builder/infinidb/etc/snmp
PERLSCRIPTS=snmpcheck tkmib mib2c fixproc ipf-mod.pl snmpconf traptoemail
SHELLSCRIPTS=mib2c-update
SCRIPTSMADEFORPERL=snmpcheck.made tkmib.made mib2c.made fixproc.made \
	ipf-mod.pl.made snmpconf.made traptoemail.made
PERLPROG=/usr/bin/perl
DATASRCS=mib2c.conf mib2c.iterate.conf mib2c.iterate_access.conf \
	mib2c.create-dataset.conf mib2c.mfd.conf \
	mib2c.array-user.conf mib2c.column_enums.conf \
	mib2c.column_defines.conf mib2c.column_storage.conf \
	mib2c.old-api.conf mib2c.scalar.conf \
	mib2c.check_values.conf mib2c.check_values_local.conf \
	mib2c.access_functions.conf mib2c.notify.conf \
	mib2c.int_watch.conf mib2c.genhtml.conf
MIB2CINSTALLDIR=$(snmplibdir)/mib2c-data
MIB2CDATASRC=mib2c-conf.d
MIB2CFILES=default-mfd-top.m2c details-enums.m2i details-node.m2i \
	details-table.m2i generic-ctx-copy.m2i generic-ctx-get.m2i \
	generic-ctx-set.m2i generic-data-allocate.m2i generic-data-context.m2i \
	generic-get-char.m2i generic-get-decl-bot.m2i generic-get-decl.m2i \
	generic-get-long.m2i generic-get-oid.m2i generic-header-bottom.m2i \
	generic-header-top.m2i generic-source-includes.m2i \
	generic-table-constants.m2c generic-table-enums.m2c \
	generic-table-indexes-from-oid.m2i generic-table-indexes-set.m2i \
	generic-table-indexes-to-oid.m2i \
	generic-table-indexes-varbind-setup.m2i generic-table-indexes.m2i \
	generic-table-oids.m2c generic-value-map-func.m2i \
	generic-value-map-reverse.m2i generic-value-map.m2i \
	m2c-internal-warning.m2i \
	m2c_setup_enum.m2i m2c_setup_node.m2i m2c_setup_table.m2i \
	mfd-access-container-cached-defines.m2i \
	mfd-access-unsorted-external-defines.m2i \
	mfd-data-access.m2c mfd-data-get.m2c mfd-data-set.m2c \
	mfd-doxygen.m2c mfd-interactive-setup.m2c mfd-interface.m2c \
	mfd-makefile.m2m mfd-readme.m2c mfd-top.m2c \
	node-get.m2i node-set.m2i node-storage.m2i node-varbind-validate.m2i \
	parent-dependencies.m2i parent-set.m2i \
	subagent.m2c \
	syntax-COUNTER64-get.m2i syntax-DateAndTime-get.m2d \
	syntax-DateAndTime-get.m2i syntax-DateAndTime-readme.m2i \
	syntax-InetAddress-get.m2i syntax-InetAddress-set.m2i \
	syntax-InetAddressType-get.m2i syntax-InetAddressType-set.m2i \
	syntax-RowStatus-dependencies.m2i syntax-RowStatus-get.m2i \
	syntax-RowStatus-varbind-validate.m2i \
	syntax-StorageType-dependencies.m2i \
	syntax-TestAndIncr-get.m2i

CONFINSTALLDIR=$(snmplibdir)/snmpconf-data
CONFDATASRC=snmpconf.dir
CONFDIRS=snmp-data snmpd-data snmptrapd-data
CONFFILES=snmpd-data/system snmpd-data/acl snmpd-data/trapsinks \
	snmpd-data/monitor snmpd-data/extending snmpd-data/operation \
	snmpd-data/basic_setup snmpd-data/snmpconf-config \
	snmp-data/authopts snmp-data/debugging snmp-data/output snmp-data/mibs \
	snmp-data/snmpconf-config \
	snmptrapd-data/formatting snmptrapd-data/traphandle \
	snmptrapd-data/authentication snmptrapd-data/logging snmptrapd-data/runtime \
	snmptrapd-data/snmpconf-config

OTHERCLEANTARGETS=snmpcheck $(SCRIPTSMADEFORPERL)

all: $(SCRIPTSMADEFORPERL) standardall

snmpcheck: $(srcdir)/snmpcheck.def ../sedscript
	$(SED) -f ../sedscript $(srcdir)/snmpcheck.def > snmpcheck

snmpcheck.made: snmpcheck
	if test "x$(PERLPROG)" != "x" ; then \
	  $(PERLPROG) -p -e 's%^#!.*/perl.*%#!$(PERLPROG)%' snmpcheck > snmpcheck.made ; \
	else \
	  touch snmpcheck.made ; \
        fi

tkmib.made: $(srcdir)/tkmib
	if test "x$(PERLPROG)" != "x" ; then \
	  $(PERLPROG) -p -e 's%^#!.*/perl.*%#!$(PERLPROG)%' ${srcdir}/tkmib > tkmib.made; \
	else \
	  touch tkmib.made; \
        fi

mib2c.made: $(srcdir)/mib2c
	if test "x$(PERLPROG)" != "x" ; then \
	  $(PERLPROG) -p -e 's%^#!.*/perl.*%#!$(PERLPROG)%;s#/usr/local/share/snmp#$(snmplibdir)#;' ${srcdir}/mib2c > mib2c.made; \
	else \
	  touch mib2c.made; \
        fi


ipf-mod.pl.made: $(srcdir)/ipf-mod.pl
	if test "x$(PERLPROG)" != "x" ; then \
	  $(PERLPROG) -p -e 's%^#!.*/perl.*%#!$(PERLPROG)%' ${srcdir}/ipf-mod.pl > ipf-mod.pl.made; \
	else \
	  touch ipf-mod.pl.made; \
        fi

fixproc.made: $(srcdir)/fixproc
	if test "x$(PERLPROG)" != "x" ; then \
	  $(PERLPROG) -p -e 's%^#!.*/perl.*%#!$(PERLPROG)%' ${srcdir}/fixproc > fixproc.made; \
	else \
	  touch fixproc.made; \
        fi

snmpconf.made: $(srcdir)/snmpconf
	if test "x$(PERLPROG)" != "x" ; then \
	  $(PERLPROG) -p -e 's%^#!.*/perl.*%#!$(PERLPROG)%; s#/usr/local/share#$(datadir)#g; s#/usr/local/etc/snmp#$(SNMPCONFPATH)#g' ${srcdir}/snmpconf > snmpconf.made; \
	else \
	  touch snmpconf.made; \
        fi

traptoemail.made: $(srcdir)/traptoemail
	if test "x$(PERLPROG)" != "x" ; then \
	  $(PERLPROG) -p -e 's%^#!.*/perl.*%#!$(PERLPROG)%; s#/usr/local/share#$(datadir)#g; s#/usr/local/etc/snmp#$(TRAPTOEMAILPATH)#g' ${srcdir}/traptoemail > traptoemail.made; \
	else \
	  touch traptoemail.made; \
        fi

localinstall: $(SCRIPTSMADEFORPERL)
	@if test "x$(PERLPROG)" != "x" ; then \
	  for i in $(PERLSCRIPTS) ; do $(INSTALL) $$i.made $(INSTALL_PREFIX)$(bindir)/$$i ; echo "install:  installed $$i in $(INSTALL_PREFIX)$(bindir)" ; done ; \
	  for i in $(SHELLSCRIPTS) ; do $(INSTALL) $$i $(INSTALL_PREFIX)$(bindir)/$$i ; echo "install:  installed $$i in $(INSTALL_PREFIX)$(bindir)" ; done ; \
	  $(SHELL) $(srcdir)/../mkinstalldirs $(INSTALL_PREFIX)$(snmplibdir) ; \
	  for i in $(DATASRCS) ; do $(INSTALL_DATA) $(srcdir)/$$i $(INSTALL_PREFIX)$(snmplibdir)/$$i ; echo "install:  installed $$i in $(INSTALL_PREFIX)$(snmplibdir)" ; done ; \
	  for i in $(CONFDIRS); do $(SHELL) $(srcdir)/../mkinstalldirs $(INSTALL_PREFIX)$(CONFINSTALLDIR)/$$i ; done ; \
	  for i in $(CONFFILES); do $(INSTALL_DATA) $(srcdir)/$(CONFDATASRC)/$$i $(INSTALL_PREFIX)$(CONFINSTALLDIR)/$$i; echo "install:  installed $$i in $(INSTALL_PREFIX)$(CONFINSTALLDIR)"; done ; \
	  $(SHELL) $(srcdir)/../mkinstalldirs $(INSTALL_PREFIX)$(MIB2CINSTALLDIR) ; \
	  for i in $(MIB2CFILES); do $(INSTALL_DATA) $(srcdir)/$(MIB2CDATASRC)/$$i $(INSTALL_PREFIX)$(MIB2CINSTALLDIR)/$$i; echo "install:  installed $$i in $(INSTALL_PREFIX)$(MIB2CINSTALLDIR)"; done \
	fi

localuninstall:
	@if test "x$(PERLPROG)" != "x" ; then \
	  for i in $(PERLSCRIPTS) ; do rm -f $(INSTALL_PREFIX)$(bindir)/$$i ; echo "removed $$i from $(INSTALL_PREFIX)$(bindir)" ; done ; \
	  for i in $(SHELLSCRIPTS) ; do rm -f $(INSTALL_PREFIX)$(bindir)/$$i ; echo "removed $$i from $(INSTALL_PREFIX)$(bindir)" ; done ; \
	  for i in $(DATASRCS) ; do rm -f $(INSTALL_PREFIX)$(snmplibdir)/$$i ; echo "removed $$i from $(INSTALL_PREFIX)$(snmplibdir)" ; done ; \
	  for i in $(CONFFILES); do rm -f $(INSTALL_PREFIX)$(CONFINSTALLDIR)/$$i; echo "removed $$i from $(INSTALL_PREFIX)$(CONFINSTALLDIR)"; done ; \
	  for i in $(MIB2CFILES); do rm -f $(INSTALL_PREFIX)$(MIB2CINSTALLDIR)/$$i; echo "removed $$i from $(INSTALL_PREFIX)$(MIB2CINSTALLDIR)"; done \
	fi

#
# standard target definitions.  Set appropriate variables to make use of them.
#
# note: the strange use of the "it" variable is for shell parsing when
# there is no targets to install for that rule.
#

# the standard items to build: libraries, bins, and sbins
STANDARDTARGETS     =$(INSTALLLIBS) $(INSTALLBINPROGS) $(INSTALLSBINPROGS)
STANDARDCLEANTARGETS=$(INSTALLLIBS) $(INSTALLBINPROGS) $(INSTALLSBINPROGS) $(INSTALLUCDLIBS)

standardall: subdirs $(STANDARDTARGETS)

objs: ${OBJS} ${LOBJS}

subdirs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making all in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) ) ; \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

# installlibs handles local, ucd and subdir libs. need to do subdir libs
# before bins, sinze those libs may be needed for successful linking
install: installlocalheaders  \
         installlibs \
         installlocalbin      installlocalsbin   \
         installsubdirs      $(OTHERINSTALL)

uninstall: uninstalllibs uninstallbin uninstallsbin uninstallheaders \
           uninstallsubdirs $(OTHERUNINSTALL)

installprogs: installbin installsbin

#
# headers
#
# set INSTALLHEADERS to a list of things to install in each makefile.
# set INSTALLBUILTHEADERS to a list of things to install from builddir
# set INSTALLSUBDIRHEADERS and INSTALLSUBDIR to subdirectory headers
# set INSTALLSUBDIRHEADERS2 and INSTALLSUBDIR2 to more subdirectory headers
# set INSTALLBUILTSUBDIRHEADERS and INSTALLBUILTSUBDIR to a list from builddir
#
installheaders: installlocalheaders  installsubdirheaders

installlocalheaders:
	@if test "$(INSTALLHEADERS)" != "" ; then \
		echo creating directory $(INSTALL_PREFIX)$(includedir) ; \
		it="$(INSTALLHEADERS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(includedir) ; \
		for i in $$it ; do \
			$(INSTALL_DATA) $(top_srcdir)/include/net-snmp/$$i $(INSTALL_PREFIX)$(includedir) ; \
			echo "installing $$i in $(INSTALL_PREFIX)$(includedir)" ; \
		done \
	fi
	@if test "$(INSTALLBUILTHEADERS)" != "" ; then \
		echo creating directory $(INSTALL_PREFIX)$(includedir) ; \
		it="$(INSTALLBUILTHEADERS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(includedir) ; \
		for i in $$it ; do \
			$(INSTALL_DATA) $$i $(INSTALL_PREFIX)$(includedir) ; \
			echo "installing $$i in $(INSTALL_PREFIX)$(includedir)" ; \
		done \
	fi
	@if test "$(INCLUDESUBDIRHEADERS)" != "" ; then \
		echo creating directory $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR) ; \
		it="$(INCLUDESUBDIRHEADERS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR) ; \
		for i in $$it ; do \
			$(INSTALL_DATA) $(top_srcdir)/include/net-snmp/$(INCLUDESUBDIR)/$$i $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR) ; \
			echo "installing $$i in $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR)" ; \
		done \
	fi
	@if test "$(INCLUDESUBDIRHEADERS2)" != "" ; then \
		echo creating directory $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR2) ; \
		it="$(INCLUDESUBDIRHEADERS2)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR2) ; \
		for i in $$it ; do \
			$(INSTALL_DATA) $(top_srcdir)/include/net-snmp/$(INCLUDESUBDIR2)/$$i $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR2) ; \
			echo "installing $$i in $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR2)" ; \
		done \
	fi
	@if test "$(INSTALLBUILTSUBDIRHEADERS)" != "" ; then \
		echo creating directory $(INSTALL_PREFIX)$(includedir)/$(INSTALLBUILTSUBDIR) ; \
		it="$(INSTALLBUILTSUBDIRHEADERS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(includedir)/$(INSTALLBUILTSUBDIR) ; \
		for i in $$it ; do \
			$(INSTALL_DATA) $$i $(INSTALL_PREFIX)$(includedir)/$(INSTALLBUILTSUBDIR) ; \
			echo "installing $$i in $(INSTALL_PREFIX)$(includedir)/$(INSTALLBUILTSUBDIR)" ; \
		done \
	fi

installucdheaders:
	@if test "$(INSTALLUCDHEADERS)" != "" ; then \
		echo creating directory $(INSTALL_PREFIX)$(ucdincludedir) ; \
		it="$(INSTALLUCDHEADERS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(ucdincludedir) ; \
		for i in $$it ; do \
			$(INSTALL_DATA) $(top_srcdir)/include/ucd-snmp/$$i $(INSTALL_PREFIX)$(ucdincludedir) ; \
			echo "installing $$i in $(INSTALL_PREFIX)$(ucdincludedir)" ; \
		done \
	fi

installsubdirheaders:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making installheaders in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) installheaders) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

uninstallheaders:
	@if test "$(INSTALLHEADERS)" != "" ; then \
		it="$(INSTALLHEADERS)" ; \
		for i in $$it ; do \
			rm -f $(INSTALL_PREFIX)$(includedir)/$$i ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(includedir)" ; \
		done \
	fi
	@if test "$(INSTALLBUILTHEADERS)" != "" ; then \
		it="$(INSTALLBUILTHEADERS)" ; \
		for i in $$it ; do \
			rm -f $(INSTALL_PREFIX)$(includedir)/`basename $$i` ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(includedir)" ; \
		done \
	fi
	@if test "$(INCLUDESUBDIRHEADERS)" != "" ; then \
		it="$(INCLUDESUBDIRHEADERS)" ; \
		for i in $$it ; do \
			rm -f $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR)/$$i ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR)" ; \
		done \
	fi
	@if test "$(INCLUDESUBDIRHEADERS2)" != "" ; then \
		it="$(INCLUDESUBDIRHEADERS2)" ; \
		for i in $$it ; do \
			rm -f $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR2)/$$i ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(includedir)/$(INCLUDESUBDIR2)" ; \
		done \
	fi
	@if test "$(INSTALLBUILTSUBDIRHEADERS)" != "" ; then \
		it="$(INSTALLBUILTSUBDIRHEADERS)" ; \
		for i in $$it ; do \
			rm -f $(INSTALL_PREFIX)$(includedir)/$(INSTALLBUILTSUBDIR)/`basename $$i` ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(includedir)/$(INSTALLBUILTSUBDIR)" ; \
		done \
	fi

#
# libraries
#
# set INSTALLLIBS to a list of things to install in each makefile.
#
installlibs: installlocallibs  installsubdirlibs

installlocallibs: $(INSTALLLIBS)
	@if test "$(INSTALLLIBS)" != ""; then \
		it="$(INSTALLLIBS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(libdir) ; \
		$(INSTALL) $(INSTALLLIBS) $(INSTALL_PREFIX)$(libdir) ; \
		for i in $$it ; do \
			echo "installing $$i in $(INSTALL_PREFIX)$(libdir)"; \
			$(RANLIB) $(INSTALL_PREFIX)$(libdir)/$$i ; \
		done ; \
		$(LIB_LDCONFIG_CMD) ; \
	fi

installucdlibs: $(INSTALLUCDLIBS)
	@if test "$(INSTALLUCDLIBS)" != ""; then \
		it="$(INSTALLUCDLIBS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(libdir) ; \
		$(INSTALL) $(INSTALLUCDLIBS) $(INSTALL_PREFIX)$(libdir) ; \
		for i in $$it ; do \
			echo "installing $$i in $(INSTALL_PREFIX)$(libdir)"; \
			$(RANLIB) $(INSTALL_PREFIX)$(libdir)/$$i ; \
		done ; \
		$(LIB_LDCONFIG_CMD) ; \
	fi

installsubdirlibs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making installlibs in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) installlibs) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

uninstalllibs:
	@if test "$(INSTALLLIBS)" != ""; then \
		it="$(INSTALLLIBS)" ; \
		for i in $$it ; do   \
			$(UNINSTALL) $(INSTALL_PREFIX)$(libdir)/$$i ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(libdir)"; \
		done \
	fi

#
# normal bin binaries
#
# set INSTALLBINPROGS to a list of things to install in each makefile.
#
installbin: installlocalbin installsubdirbin

installlocalbin: $(INSTALLBINPROGS)
	@if test "$(INSTALLBINPROGS) $(INSTALLBINSCRIPTS)" != " "; then \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(bindir) ; \
		it="$(INSTALLBINPROGS) $(INSTALLBINSCRIPTS)" ; \
		$(INSTALL) $(INSTALLBINPROGS) $(INSTALLBINSCRIPTS) $(INSTALL_PREFIX)$(bindir) ; \
		for i in $$it ; do   \
			echo "installing $$i in $(INSTALL_PREFIX)$(bindir)"; \
		done \
	fi

installsubdirbin:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making installbin in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) installbin) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

uninstallbin:
	@if test "$(INSTALLBINPROGS) $(INSTALLBINSCRIPTS)" != " "; then \
		it="$(INSTALLBINPROGS) $(INSTALLBINSCRIPTS)" ; \
		for i in $$it ; do   \
			$(UNINSTALL) $(INSTALL_PREFIX)$(bindir)/$$i ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(bindir)"; \
		done \
	fi

#
# sbin binaries
#
# set INSTALLSBINPROGS to a list of things to install in each makefile.
#
installsbin: installlocalsbin installsubdirsbin

installlocalsbin: $(INSTALLSBINPROGS)
	@if test "$(INSTALLSBINPROGS)" != ""; then \
		it="$(INSTALLSBINPROGS)" ; \
		$(SHELL) $(top_srcdir)/mkinstalldirs $(INSTALL_PREFIX)$(sbindir) ; \
		$(INSTALL) $(INSTALLSBINPROGS) $(INSTALL_PREFIX)$(sbindir) ;  \
		for i in $$it ; do   \
			echo "installing $$i in $(INSTALL_PREFIX)$(sbindir)"; \
		done \
	fi

installsubdirsbin:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making installsbin in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) installsbin) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

uninstallsbin:
	@if test "$(INSTALLSBINPROGS)" != ""; then \
		it="$(INSTALLSBINPROGS)" ; \
		for i in $$it ; do   \
			$(UNINSTALL) $(INSTALL_PREFIX)$(sbindir)/$$i ; \
			echo "removing $$i from $(INSTALL_PREFIX)$(sbindir)"; \
		done \
	fi

#
# general make install target for subdirs
#
installsubdirs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making install in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) install) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

uninstallsubdirs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making uninstall in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) uninstall) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

#
# cleaning targets
#
clean: cleansubdirs $(OTHERCLEANTODOS)
	rm -f core ${OBJS} ${LOBJS} $(STANDARDCLEANTARGETS) $(OTHERCLEANTARGETS)
	rm -rf .libs

cleansubdirs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making clean in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) clean) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

lint:
	lint -nhx $(CSRCS)

#
# wacky dependency building.
#
depend: dependdirs
	@if test -f Makefile.depend ; then \
		makedepend `echo $(CPPFLAGS) | sed 's/-f[-a-z]*//g'` -o .lo $(srcdir)/*.c $(srcdir)/*/*.c ; \
	fi


nosysdepend: nosysdependdirs
	@if test -f Makefile.depend ; then \
		makedepend `echo $(CPPFLAGS) | sed 's/-f[-a-z]*//g'` -o .lo $(srcdir)/*.c $(srcdir)/*/*.c ; \
		perl -n -i.bak $(top_srcdir)/makenosysdepend.pl Makefile ; \
	fi

distdepend: nosysdepend distdependdirs
	@if test -f Makefile.depend ; then \
		perl $(top_srcdir)/makefileindepend.pl ; \
	fi

dependdirs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making depend in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) depend) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

nosysdependdirs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making nosysdepend in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) nosysdepend) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

distdependdirs:
	@if test "$(SUBDIRS)" != ""; then \
		it="$(SUBDIRS)" ; \
		for i in $$it ; do \
			echo "making distdepend in `pwd`/$$i"; \
			( cd $$i ; $(MAKE) distdepend) ;   \
			if test $$? != 0 ; then \
				exit 1 ; \
			fi  \
		done \
	fi

# These aren't real targets, let gnu's make know that.
.PHONY: clean cleansubdirs lint \
	install installprogs installheaders installlibs \
	installbin installsbin installsubdirs \
	all subdirs standardall objs \
	depend nosysdepend distdepend dependdirs nosysdependdirs distdependdirs
