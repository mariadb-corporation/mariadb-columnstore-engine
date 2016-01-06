#
# Minimum environment and virtual path setup
#
SHELL		= /bin/sh
srcdir		= .
top_srcdir	= ../..
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
# snmpnetstat Makefile
#

top_builddir=../..
INSTALLBINPROGS=snmpnetstat$(EXEEXT)

# use GNU vpath, if available, to only set a path for source and headers
# VPATH will pick up objects too, which is bad if you are sharing a
# source dir...
#vpath %.h $(srcdir)
#vpath %.c $(srcdir)
# fallback to regular VPATH for non-gnu...
VPATH = $(srcdir)

#
# build info
#
SRCS=	inet.c inet6.c if.c main.c route.c winstub.c
OBJS=	inet.o inet6.o if.o main.o route.o winstub.o
LOBJS=	inet.lo inet6.lo if.lo main.lo route.lo winstub.lo
TARG= snmpnetstat$(EXEEXT)

CPPFLAGS= $(TOP_INCLUDES)  

USELIBS= ../../snmplib/libnetsnmp.$(LIB_EXTENSION)$(LIB_VERSION)
LIBS=	 ../../snmplib/libnetsnmp.$(LIB_EXTENSION)$(LIB_VERSION) -lm 

all: standardall

snmpnetstat$(EXEEXT): ${LOBJS} ${USELIBS}
	${LINK} -o $@ ${LOBJS} ${LOCAL_LIBS} ${LDFLAGS} ${LIBS}

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
# DO NOT DELETE THIS LINE -- make depend depends on it.

ipe./if.lo: ../../include/net-snmp/net-snmp-config.h
ipe./if.lo: ../../include/net-snmp/system/linux.h
ipe./if.lo: ../../include/net-snmp/system/sysv.h
ipe./if.lo: ../../include/net-snmp/system/generic.h
ipe./if.lo: ../../include/net-snmp/machine/generic.h 
ipe./if.lo:  main.h
ipe./if.lo: ../../include/net-snmp/net-snmp-includes.h
ipe./if.lo: ../../include/net-snmp/definitions.h
ipe./if.lo: ../../include/net-snmp/types.h 
ipe./if.lo: ../../include/net-snmp/library/snmp_api.h
ipe./if.lo: ../../include/net-snmp/library/asn1.h
ipe./if.lo: ../../include/net-snmp/library/snmp_impl.h
ipe./if.lo: ../../include/net-snmp/library/snmp.h
ipe./if.lo: ../../include/net-snmp/library/snmp-tc.h
ipe./if.lo: ../../include/net-snmp/utilities.h
ipe./if.lo: ../../include/net-snmp/library/snmp_client.h
ipe./if.lo: ../../include/net-snmp/library/system.h
ipe./if.lo: ../../include/net-snmp/library/tools.h
ipe./if.lo: ../../include/net-snmp/library/int64.h
ipe./if.lo: ../../include/net-snmp/library/mt_support.h
ipe./if.lo: ../../include/net-snmp/library/snmp_alarm.h
ipe./if.lo: ../../include/net-snmp/library/callback.h
ipe./if.lo: ../../include/net-snmp/library/data_list.h
ipe./if.lo: ../../include/net-snmp/library/oid_stash.h
ipe./if.lo: ../../include/net-snmp/library/check_varbind.h
ipe./if.lo: ../../include/net-snmp/library/container.h
ipe./if.lo: ../../include/net-snmp/library/factory.h
ipe./if.lo: ../../include/net-snmp/library/snmp_logging.h
ipe./if.lo: ../../include/net-snmp/library/container_binary_array.h
ipe./if.lo: ../../include/net-snmp/library/container_list_ssll.h
ipe./if.lo: ../../include/net-snmp/library/container_iterator.h
ipe./if.lo: ../../include/net-snmp/library/container.h
ipe./if.lo: ../../include/net-snmp/library/snmp_assert.h
ipe./if.lo: ../../include/net-snmp/version.h
ipe./if.lo: ../../include/net-snmp/session_api.h
ipe./if.lo: ../../include/net-snmp/library/snmp_transport.h
ipe./if.lo: ../../include/net-snmp/library/snmpCallbackDomain.h
ipe./if.lo: ../../include/net-snmp/library/snmpUnixDomain.h
ipe./if.lo: ../../include/net-snmp/library/snmpUDPDomain.h
ipe./if.lo: ../../include/net-snmp/library/snmpTCPDomain.h
ipe./if.lo: ../../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./if.lo: ../../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./if.lo: ../../include/net-snmp/library/snmpIPXDomain.h
ipe./if.lo: ../../include/net-snmp/library/ucd_compat.h
ipe./if.lo: ../../include/net-snmp/pdu_api.h ../../include/net-snmp/mib_api.h
ipe./if.lo: ../../include/net-snmp/library/mib.h
ipe./if.lo: ../../include/net-snmp/library/parse.h
ipe./if.lo: ../../include/net-snmp/varbind_api.h
ipe./if.lo: ../../include/net-snmp/config_api.h
ipe./if.lo: ../../include/net-snmp/library/read_config.h
ipe./if.lo: ../../include/net-snmp/library/default_store.h
ipe./if.lo: ../../include/net-snmp/library/snmp_parse_args.h
ipe./if.lo: ../../include/net-snmp/library/snmp_enum.h
ipe./if.lo: ../../include/net-snmp/library/vacm.h
ipe./if.lo: ../../include/net-snmp/output_api.h
ipe./if.lo: ../../include/net-snmp/library/snmp_debug.h
ipe./if.lo: ../../include/net-snmp/snmpv3_api.h
ipe./if.lo: ../../include/net-snmp/library/snmpv3.h
ipe./if.lo: ../../include/net-snmp/library/transform_oids.h
ipe./if.lo: ../../include/net-snmp/library/keytools.h
ipe./if.lo: ../../include/net-snmp/library/scapi.h
ipe./if.lo: ../../include/net-snmp/library/lcd_time.h
ipe./if.lo: ../../include/net-snmp/library/snmp_secmod.h
ipe./if.lo: ../../include/net-snmp/library/snmpv3-security-includes.h
ipe./if.lo: ../../include/net-snmp/library/snmpusm.h netstat.h
ipe./inet6.lo: ../../include/net-snmp/net-snmp-config.h
ipe./inet6.lo: ../../include/net-snmp/system/linux.h
ipe./inet6.lo: ../../include/net-snmp/system/sysv.h
ipe./inet6.lo: ../../include/net-snmp/system/generic.h
ipe./inet6.lo: ../../include/net-snmp/machine/generic.h
ipe./inet.lo: ../../include/net-snmp/net-snmp-config.h
ipe./inet.lo: ../../include/net-snmp/system/linux.h
ipe./inet.lo: ../../include/net-snmp/system/sysv.h
ipe./inet.lo: ../../include/net-snmp/system/generic.h
ipe./inet.lo: ../../include/net-snmp/machine/generic.h 
ipe./inet.lo:  main.h
ipe./inet.lo: ../../include/net-snmp/net-snmp-includes.h
ipe./inet.lo:  ../../include/net-snmp/definitions.h
ipe./inet.lo: ../../include/net-snmp/types.h 
ipe./inet.lo: ../../include/net-snmp/library/snmp_api.h
ipe./inet.lo: ../../include/net-snmp/library/asn1.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_impl.h
ipe./inet.lo: ../../include/net-snmp/library/snmp.h
ipe./inet.lo: ../../include/net-snmp/library/snmp-tc.h
ipe./inet.lo: ../../include/net-snmp/utilities.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_client.h
ipe./inet.lo: ../../include/net-snmp/library/system.h
ipe./inet.lo: ../../include/net-snmp/library/tools.h
ipe./inet.lo: ../../include/net-snmp/library/int64.h
ipe./inet.lo: ../../include/net-snmp/library/mt_support.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_alarm.h
ipe./inet.lo: ../../include/net-snmp/library/callback.h
ipe./inet.lo: ../../include/net-snmp/library/data_list.h
ipe./inet.lo: ../../include/net-snmp/library/oid_stash.h
ipe./inet.lo: ../../include/net-snmp/library/check_varbind.h
ipe./inet.lo: ../../include/net-snmp/library/container.h
ipe./inet.lo: ../../include/net-snmp/library/factory.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_logging.h
ipe./inet.lo: ../../include/net-snmp/library/container_binary_array.h
ipe./inet.lo: ../../include/net-snmp/library/container_list_ssll.h
ipe./inet.lo: ../../include/net-snmp/library/container_iterator.h
ipe./inet.lo: ../../include/net-snmp/library/container.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_assert.h
ipe./inet.lo: ../../include/net-snmp/version.h
ipe./inet.lo: ../../include/net-snmp/session_api.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_transport.h
ipe./inet.lo: ../../include/net-snmp/library/snmpCallbackDomain.h
ipe./inet.lo: ../../include/net-snmp/library/snmpUnixDomain.h
ipe./inet.lo: ../../include/net-snmp/library/snmpUDPDomain.h
ipe./inet.lo: ../../include/net-snmp/library/snmpTCPDomain.h
ipe./inet.lo: ../../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./inet.lo: ../../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./inet.lo: ../../include/net-snmp/library/snmpIPXDomain.h
ipe./inet.lo: ../../include/net-snmp/library/ucd_compat.h
ipe./inet.lo: ../../include/net-snmp/pdu_api.h
ipe./inet.lo: ../../include/net-snmp/mib_api.h
ipe./inet.lo: ../../include/net-snmp/library/mib.h
ipe./inet.lo: ../../include/net-snmp/library/parse.h
ipe./inet.lo: ../../include/net-snmp/varbind_api.h
ipe./inet.lo: ../../include/net-snmp/config_api.h
ipe./inet.lo: ../../include/net-snmp/library/read_config.h
ipe./inet.lo: ../../include/net-snmp/library/default_store.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_parse_args.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_enum.h
ipe./inet.lo: ../../include/net-snmp/library/vacm.h
ipe./inet.lo: ../../include/net-snmp/output_api.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_debug.h
ipe./inet.lo: ../../include/net-snmp/snmpv3_api.h
ipe./inet.lo: ../../include/net-snmp/library/snmpv3.h
ipe./inet.lo: ../../include/net-snmp/library/transform_oids.h
ipe./inet.lo: ../../include/net-snmp/library/keytools.h
ipe./inet.lo: ../../include/net-snmp/library/scapi.h
ipe./inet.lo: ../../include/net-snmp/library/lcd_time.h
ipe./inet.lo: ../../include/net-snmp/library/snmp_secmod.h
ipe./inet.lo: ../../include/net-snmp/library/snmpv3-security-includes.h
ipe./inet.lo: ../../include/net-snmp/library/snmpusm.h netstat.h
ipe./main.lo: ../../include/net-snmp/net-snmp-config.h
ipe./main.lo: ../../include/net-snmp/system/linux.h
ipe./main.lo: ../../include/net-snmp/system/sysv.h
ipe./main.lo: ../../include/net-snmp/system/generic.h
ipe./main.lo: ../../include/net-snmp/machine/generic.h 
ipe./main.lo: ../../include/net-snmp/utilities.h
ipe./main.lo: ../../include/net-snmp/types.h 
ipe./main.lo: ../../include/net-snmp/definitions.h
ipe./main.lo: ../../include/net-snmp/library/snmp_api.h
ipe./main.lo: ../../include/net-snmp/library/asn1.h
ipe./main.lo: ../../include/net-snmp/library/snmp_impl.h
ipe./main.lo: ../../include/net-snmp/library/snmp.h
ipe./main.lo: ../../include/net-snmp/library/snmp-tc.h
ipe./main.lo: ../../include/net-snmp/library/snmp_client.h
ipe./main.lo: ../../include/net-snmp/library/system.h
ipe./main.lo: ../../include/net-snmp/library/tools.h
ipe./main.lo: ../../include/net-snmp/library/int64.h
ipe./main.lo: ../../include/net-snmp/library/mt_support.h
ipe./main.lo: ../../include/net-snmp/library/snmp_alarm.h
ipe./main.lo: ../../include/net-snmp/library/callback.h
ipe./main.lo: ../../include/net-snmp/library/data_list.h
ipe./main.lo: ../../include/net-snmp/library/oid_stash.h
ipe./main.lo: ../../include/net-snmp/library/check_varbind.h
ipe./main.lo: ../../include/net-snmp/library/container.h
ipe./main.lo: ../../include/net-snmp/library/factory.h
ipe./main.lo: ../../include/net-snmp/library/snmp_logging.h
ipe./main.lo: ../../include/net-snmp/library/container_binary_array.h
ipe./main.lo: ../../include/net-snmp/library/container_list_ssll.h
ipe./main.lo: ../../include/net-snmp/library/container_iterator.h
ipe./main.lo: ../../include/net-snmp/library/container.h
ipe./main.lo: ../../include/net-snmp/library/snmp_assert.h
ipe./main.lo: ../../include/net-snmp/version.h 
ipe./main.lo: ../../include/net-snmp/net-snmp-includes.h
ipe./main.lo: ../../include/net-snmp/session_api.h
ipe./main.lo: ../../include/net-snmp/library/snmp_transport.h
ipe./main.lo: ../../include/net-snmp/library/snmpCallbackDomain.h
ipe./main.lo: ../../include/net-snmp/library/snmpUnixDomain.h
ipe./main.lo: ../../include/net-snmp/library/snmpUDPDomain.h
ipe./main.lo: ../../include/net-snmp/library/snmpTCPDomain.h
ipe./main.lo: ../../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./main.lo: ../../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./main.lo: ../../include/net-snmp/library/snmpIPXDomain.h
ipe./main.lo: ../../include/net-snmp/library/ucd_compat.h
ipe./main.lo: ../../include/net-snmp/pdu_api.h
ipe./main.lo: ../../include/net-snmp/mib_api.h
ipe./main.lo: ../../include/net-snmp/library/mib.h
ipe./main.lo: ../../include/net-snmp/library/parse.h
ipe./main.lo: ../../include/net-snmp/varbind_api.h
ipe./main.lo: ../../include/net-snmp/config_api.h
ipe./main.lo: ../../include/net-snmp/library/read_config.h
ipe./main.lo: ../../include/net-snmp/library/default_store.h
ipe./main.lo: ../../include/net-snmp/library/snmp_parse_args.h
ipe./main.lo: ../../include/net-snmp/library/snmp_enum.h
ipe./main.lo: ../../include/net-snmp/library/vacm.h
ipe./main.lo: ../../include/net-snmp/output_api.h
ipe./main.lo: ../../include/net-snmp/library/snmp_debug.h
ipe./main.lo: ../../include/net-snmp/snmpv3_api.h
ipe./main.lo: ../../include/net-snmp/library/snmpv3.h
ipe./main.lo: ../../include/net-snmp/library/transform_oids.h
ipe./main.lo: ../../include/net-snmp/library/keytools.h
ipe./main.lo: ../../include/net-snmp/library/scapi.h
ipe./main.lo: ../../include/net-snmp/library/lcd_time.h
ipe./main.lo: ../../include/net-snmp/library/snmp_secmod.h
ipe./main.lo: ../../include/net-snmp/library/snmpv3-security-includes.h
ipe./main.lo: ../../include/net-snmp/library/snmpusm.h netstat.h
ipe./route.lo: ../../include/net-snmp/net-snmp-config.h
ipe./route.lo: ../../include/net-snmp/system/linux.h
ipe./route.lo: ../../include/net-snmp/system/sysv.h
ipe./route.lo: ../../include/net-snmp/system/generic.h
ipe./route.lo: ../../include/net-snmp/machine/generic.h 
ipe./route.lo:  main.h
ipe./route.lo: ../../include/net-snmp/net-snmp-includes.h
ipe./route.lo:  ../../include/net-snmp/definitions.h
ipe./route.lo: ../../include/net-snmp/types.h 
ipe./route.lo: ../../include/net-snmp/library/snmp_api.h
ipe./route.lo: ../../include/net-snmp/library/asn1.h
ipe./route.lo: ../../include/net-snmp/library/snmp_impl.h
ipe./route.lo: ../../include/net-snmp/library/snmp.h
ipe./route.lo: ../../include/net-snmp/library/snmp-tc.h
ipe./route.lo: ../../include/net-snmp/utilities.h
ipe./route.lo: ../../include/net-snmp/library/snmp_client.h
ipe./route.lo: ../../include/net-snmp/library/system.h
ipe./route.lo: ../../include/net-snmp/library/tools.h
ipe./route.lo: ../../include/net-snmp/library/int64.h
ipe./route.lo: ../../include/net-snmp/library/mt_support.h
ipe./route.lo: ../../include/net-snmp/library/snmp_alarm.h
ipe./route.lo: ../../include/net-snmp/library/callback.h
ipe./route.lo: ../../include/net-snmp/library/data_list.h
ipe./route.lo: ../../include/net-snmp/library/oid_stash.h
ipe./route.lo: ../../include/net-snmp/library/check_varbind.h
ipe./route.lo: ../../include/net-snmp/library/container.h
ipe./route.lo: ../../include/net-snmp/library/factory.h
ipe./route.lo: ../../include/net-snmp/library/snmp_logging.h
ipe./route.lo: ../../include/net-snmp/library/container_binary_array.h
ipe./route.lo: ../../include/net-snmp/library/container_list_ssll.h
ipe./route.lo: ../../include/net-snmp/library/container_iterator.h
ipe./route.lo: ../../include/net-snmp/library/container.h
ipe./route.lo: ../../include/net-snmp/library/snmp_assert.h
ipe./route.lo: ../../include/net-snmp/version.h
ipe./route.lo: ../../include/net-snmp/session_api.h
ipe./route.lo: ../../include/net-snmp/library/snmp_transport.h
ipe./route.lo: ../../include/net-snmp/library/snmpCallbackDomain.h
ipe./route.lo: ../../include/net-snmp/library/snmpUnixDomain.h
ipe./route.lo: ../../include/net-snmp/library/snmpUDPDomain.h
ipe./route.lo: ../../include/net-snmp/library/snmpTCPDomain.h
ipe./route.lo: ../../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./route.lo: ../../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./route.lo: ../../include/net-snmp/library/snmpIPXDomain.h
ipe./route.lo: ../../include/net-snmp/library/ucd_compat.h
ipe./route.lo: ../../include/net-snmp/pdu_api.h
ipe./route.lo: ../../include/net-snmp/mib_api.h
ipe./route.lo: ../../include/net-snmp/library/mib.h
ipe./route.lo: ../../include/net-snmp/library/parse.h
ipe./route.lo: ../../include/net-snmp/varbind_api.h
ipe./route.lo: ../../include/net-snmp/config_api.h
ipe./route.lo: ../../include/net-snmp/library/read_config.h
ipe./route.lo: ../../include/net-snmp/library/default_store.h
ipe./route.lo: ../../include/net-snmp/library/snmp_parse_args.h
ipe./route.lo: ../../include/net-snmp/library/snmp_enum.h
ipe./route.lo: ../../include/net-snmp/library/vacm.h
ipe./route.lo: ../../include/net-snmp/output_api.h
ipe./route.lo: ../../include/net-snmp/library/snmp_debug.h
ipe./route.lo: ../../include/net-snmp/snmpv3_api.h
ipe./route.lo: ../../include/net-snmp/library/snmpv3.h
ipe./route.lo: ../../include/net-snmp/library/transform_oids.h
ipe./route.lo: ../../include/net-snmp/library/keytools.h
ipe./route.lo: ../../include/net-snmp/library/scapi.h
ipe./route.lo: ../../include/net-snmp/library/lcd_time.h
ipe./route.lo: ../../include/net-snmp/library/snmp_secmod.h
ipe./route.lo: ../../include/net-snmp/library/snmpv3-security-includes.h
ipe./route.lo: ../../include/net-snmp/library/snmpusm.h netstat.h
ipe./winstub.lo: ../../include/net-snmp/net-snmp-config.h
ipe./winstub.lo: ../../include/net-snmp/system/linux.h
ipe./winstub.lo: ../../include/net-snmp/system/sysv.h
ipe./winstub.lo: ../../include/net-snmp/system/generic.h
ipe./winstub.lo: ../../include/net-snmp/machine/generic.h
ipe./winstub.lo: ../../include/net-snmp/net-snmp-includes.h
ipe./winstub.lo:  ../../include/net-snmp/definitions.h
ipe./winstub.lo: ../../include/net-snmp/types.h 
ipe./winstub.lo: ../../include/net-snmp/library/snmp_api.h
ipe./winstub.lo: ../../include/net-snmp/library/asn1.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_impl.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp-tc.h
ipe./winstub.lo: ../../include/net-snmp/utilities.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_client.h
ipe./winstub.lo: ../../include/net-snmp/library/system.h
ipe./winstub.lo: ../../include/net-snmp/library/tools.h
ipe./winstub.lo: ../../include/net-snmp/library/int64.h
ipe./winstub.lo: ../../include/net-snmp/library/mt_support.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_alarm.h
ipe./winstub.lo: ../../include/net-snmp/library/callback.h
ipe./winstub.lo: ../../include/net-snmp/library/data_list.h
ipe./winstub.lo: ../../include/net-snmp/library/oid_stash.h
ipe./winstub.lo: ../../include/net-snmp/library/check_varbind.h
ipe./winstub.lo: ../../include/net-snmp/library/container.h
ipe./winstub.lo: ../../include/net-snmp/library/factory.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_logging.h
ipe./winstub.lo: ../../include/net-snmp/library/container_binary_array.h
ipe./winstub.lo: ../../include/net-snmp/library/container_list_ssll.h
ipe./winstub.lo: ../../include/net-snmp/library/container_iterator.h
ipe./winstub.lo: ../../include/net-snmp/library/container.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_assert.h
ipe./winstub.lo: ../../include/net-snmp/version.h
ipe./winstub.lo: ../../include/net-snmp/session_api.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_transport.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpCallbackDomain.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpUnixDomain.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpUDPDomain.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpTCPDomain.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpIPXDomain.h
ipe./winstub.lo: ../../include/net-snmp/library/ucd_compat.h
ipe./winstub.lo: ../../include/net-snmp/pdu_api.h
ipe./winstub.lo: ../../include/net-snmp/mib_api.h
ipe./winstub.lo: ../../include/net-snmp/library/mib.h
ipe./winstub.lo: ../../include/net-snmp/library/parse.h
ipe./winstub.lo: ../../include/net-snmp/varbind_api.h
ipe./winstub.lo: ../../include/net-snmp/config_api.h
ipe./winstub.lo: ../../include/net-snmp/library/read_config.h
ipe./winstub.lo: ../../include/net-snmp/library/default_store.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_parse_args.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_enum.h
ipe./winstub.lo: ../../include/net-snmp/library/vacm.h
ipe./winstub.lo: ../../include/net-snmp/output_api.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_debug.h
ipe./winstub.lo: ../../include/net-snmp/snmpv3_api.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpv3.h
ipe./winstub.lo: ../../include/net-snmp/library/transform_oids.h
ipe./winstub.lo: ../../include/net-snmp/library/keytools.h
ipe./winstub.lo: ../../include/net-snmp/library/scapi.h
ipe./winstub.lo: ../../include/net-snmp/library/lcd_time.h
ipe./winstub.lo: ../../include/net-snmp/library/snmp_secmod.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpv3-security-includes.h
ipe./winstub.lo: ../../include/net-snmp/library/snmpusm.h
