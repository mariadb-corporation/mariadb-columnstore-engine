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
# Makefile for snmpget, snmpwalk, snmpbulkwalk, snmptest, snmptranslate,
# snmptrapd, snmptable, snmpset, snmpgetnext, and other utilities.
#

top_builddir=..

# use GNU vpath, if available, to only set a path for source and headers
# VPATH will pick up objects too, which is bad if you are sharing a
# source dir...
#vpath %.h $(srcdir)
#vpath %.c $(srcdir)
# fallback to regular VPATH for non-gnu...
VPATH = $(srcdir)

#
# Things to install
#

INSTALLBINPROGS	= snmpget$(EXEEXT) 			\
		snmpgetnext$(EXEEXT) 			\
		snmpset$(EXEEXT) 			\
		snmpusm$(EXEEXT)			\
		snmpwalk$(EXEEXT) 			\
		snmpbulkwalk$(EXEEXT) 			\
		snmptable$(EXEEXT)			\
		snmptrap$(EXEEXT) 			\
		snmpbulkget$(EXEEXT)			\
		snmptranslate$(EXEEXT) 			\
		snmpstatus$(EXEEXT) 			\
		snmpdelta$(EXEEXT) 			\
		snmptest$(EXEEXT)			\
		encode_keychange$(EXEEXT) 		\
		snmpdf$(EXEEXT) 			\
		snmpvacm$(EXEEXT)

INSTALLSBINPROGS = snmptrapd$(EXEEXT)

INSTALLLIBS     = libnetsnmptrapd.$(LIB_EXTENSION)$(LIB_VERSION)

SUBDIRS		= snmpnetstat

#
# build variables.
#

# USELIBS/USEAGENTLIBS are for dependencies
USELIBS		= ../snmplib/libnetsnmp.$(LIB_EXTENSION)$(LIB_VERSION) 
USEAGENTLIBS	= ../agent/libnetsnmpagent.$(LIB_EXTENSION)$(LIB_VERSION) ../agent/libnetsnmpmibs.$(LIB_EXTENSION)$(LIB_VERSION) ../agent/helpers/libnetsnmphelpers.$(LIB_EXTENSION)$(LIB_VERSION) $(USELIBS)

#
# link path in src dir
LOCAL_LIBS	= -L../snmplib -L../agent -L../agent/helpers
LIBS		= $(USELIBS) -lm 

#
# hack for compiling trapd when agent is disabled
TRAPDWITHAGENT  = $(USETRAPLIBS) -ldl -lm 
TRAPDWITHOUTAGENT = $(LIBS)

# these will be set by configure to one of the above 2 lines
TRAPLIBS	= $(TRAPDWITHAGENT) 
USETRAPLIBS	= $(USEAGENTLIBS)

CPPFLAGS	= $(TOP_INCLUDES) -I. $(AGENT_INCLUDES) $(HELPER_INCLUDES) \
		  $(MIBGROUP_INCLUDES)  $(SNMPLIB_INCLUDES) 

OSUFFIX		= lo
TRAPD_OBJECTS   = snmptrapd.$(OSUFFIX) 
LIBTRAPD_OBJS   = snmptrapd_handlers.o  snmptrapd_log.o  notification_log.o
LLIBTRAPD_OBJS  = snmptrapd_handlers.lo snmptrapd_log.lo notification_log.lo
OBJS  = *.o
LOBJS = *.lo

all: standardall

OTHERINSTALL=snmpinforminstall snmptrapdperlinstall
OTHERUNINSTALL=snmpinformuninstall snmptrapdperluninstall

#
# build rules
#
snmpwalk$(EXEEXT):    snmpwalk.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpwalk.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpbulkwalk$(EXEEXT):    snmpbulkwalk.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpbulkwalk.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpbulkget$(EXEEXT):    snmpbulkget.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpbulkget.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmptranslate$(EXEEXT):    snmptranslate.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmptranslate.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpstatus$(EXEEXT):    snmpstatus.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpstatus.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpget$(EXEEXT):    snmpget.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpget.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpdelta$(EXEEXT):    snmpdelta.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpdelta.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmptable$(EXEEXT):    snmptable.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmptable.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmptest$(EXEEXT):    snmptest.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmptest.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmptrapd$(EXEEXT):    $(TRAPD_OBJECTS) $(USETRAPLIBS) $(INSTALLLIBS)
	$(LINK) -o $@ $(TRAPD_OBJECTS) $(INSTALLLIBS) $(LOCAL_LIBS) ${LDFLAGS} ${TRAPLIBS}

snmptrap$(EXEEXT):    snmptrap.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmptrap.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpinform$(EXEEXT): snmptrap$(EXEEXT)
	rm -f snmpinform
	$(LN_S) snmptrap$(EXEEXT) snmpinform$(EXEEXT)

snmpset$(EXEEXT):    snmpset.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpset.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpusm$(EXEEXT):    snmpusm.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpusm.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpvacm$(EXEEXT):    snmpvacm.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpvacm.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpgetnext$(EXEEXT):    snmpgetnext.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpgetnext.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

encode_keychange$(EXEEXT):    encode_keychange.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ encode_keychange.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

snmpdf$(EXEEXT):    snmpdf.$(OSUFFIX) $(USELIBS)
	$(LINK) -o $@ snmpdf.$(OSUFFIX) $(LOCAL_LIBS) ${LDFLAGS} ${LIBS} 

libnetsnmptrapd.$(LIB_EXTENSION)$(LIB_VERSION): $(LLIBTRAPD_OBJS)
	$(LIB_LD_CMD) libnetsnmptrapd.$(LIB_EXTENSION)$(LIB_VERSION) ${LLIBTRAPD_OBJS}  $(LIB_LD_LIBS)
	$(RANLIB) libnetsnmptrapd.$(LIB_EXTENSION)$(LIB_VERSION)

snmpinforminstall:
	rm -f $(INSTALL_PREFIX)$(bindir)/snmpinform$(EXEEXT)
	$(LN_S) snmptrap$(EXEEXT) $(INSTALL_PREFIX)$(bindir)/snmpinform$(EXEEXT)

snmpinformuninstall:
	rm -f $(INSTALL_PREFIX)$(bindir)/snmpinform$(EXEEXT)

snmptrapdperlinstall: installdirs
	  @$(INSTALL) $(srcdir)/snmp_perl_trapd.pl $(INSTALL_PREFIX)$(snmplibdir)/snmp_perl_trapd.pl
	  @echo "install:  installed snmp_perl_trapd.pl in $(INSTALL_PREFIX)$(snmplibdir)"

snmptrapdperluninstall: installdirs
	  @rm -f $(INSTALL_PREFIX)$(snmplibdir)/snmp_perl_trapd.pl
	  @echo "removed snmp_perl_trapd.pl from $(INSTALL_PREFIX)$(snmplibdir)"

installdirs:
	@$(SHELL) $(srcdir)/../mkinstalldirs $(INSTALL_PREFIX)$(snmplibdir)

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

ipe./encode_keychange.lo: ../include/net-snmp/net-snmp-config.h
ipe./encode_keychange.lo: ../include/net-snmp/system/linux.h
ipe./encode_keychange.lo: ../include/net-snmp/system/sysv.h
ipe./encode_keychange.lo: ../include/net-snmp/system/generic.h
ipe./encode_keychange.lo: ../include/net-snmp/machine/generic.h
ipe./encode_keychange.lo: ../include/net-snmp/net-snmp-includes.h
ipe./encode_keychange.lo: ../include/net-snmp/definitions.h
ipe./encode_keychange.lo: ../include/net-snmp/types.h 
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_api.h
ipe./encode_keychange.lo: ../include/net-snmp/library/asn1.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_impl.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp-tc.h
ipe./encode_keychange.lo: ../include/net-snmp/utilities.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_client.h
ipe./encode_keychange.lo: ../include/net-snmp/library/system.h
ipe./encode_keychange.lo: ../include/net-snmp/library/tools.h
ipe./encode_keychange.lo: ../include/net-snmp/library/int64.h
ipe./encode_keychange.lo: ../include/net-snmp/library/mt_support.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./encode_keychange.lo: ../include/net-snmp/library/callback.h
ipe./encode_keychange.lo: ../include/net-snmp/library/data_list.h
ipe./encode_keychange.lo: ../include/net-snmp/library/oid_stash.h
ipe./encode_keychange.lo: ../include/net-snmp/library/check_varbind.h
ipe./encode_keychange.lo: ../include/net-snmp/library/container.h
ipe./encode_keychange.lo: ../include/net-snmp/library/factory.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_logging.h
ipe./encode_keychange.lo: ../include/net-snmp/library/container_binary_array.h
ipe./encode_keychange.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./encode_keychange.lo: ../include/net-snmp/library/container_iterator.h
ipe./encode_keychange.lo: ../include/net-snmp/library/container.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_assert.h
ipe./encode_keychange.lo: ../include/net-snmp/version.h
ipe./encode_keychange.lo: ../include/net-snmp/session_api.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_transport.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./encode_keychange.lo: ../include/net-snmp/library/ucd_compat.h
ipe./encode_keychange.lo: ../include/net-snmp/pdu_api.h
ipe./encode_keychange.lo: ../include/net-snmp/mib_api.h
ipe./encode_keychange.lo: ../include/net-snmp/library/mib.h
ipe./encode_keychange.lo: ../include/net-snmp/library/parse.h
ipe./encode_keychange.lo: ../include/net-snmp/varbind_api.h
ipe./encode_keychange.lo: ../include/net-snmp/config_api.h
ipe./encode_keychange.lo: ../include/net-snmp/library/read_config.h
ipe./encode_keychange.lo: ../include/net-snmp/library/default_store.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_enum.h
ipe./encode_keychange.lo: ../include/net-snmp/library/vacm.h
ipe./encode_keychange.lo: ../include/net-snmp/output_api.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_debug.h
ipe./encode_keychange.lo: ../include/net-snmp/snmpv3_api.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpv3.h
ipe./encode_keychange.lo: ../include/net-snmp/library/transform_oids.h
ipe./encode_keychange.lo: ../include/net-snmp/library/keytools.h
ipe./encode_keychange.lo: ../include/net-snmp/library/scapi.h
ipe./encode_keychange.lo: ../include/net-snmp/library/lcd_time.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./encode_keychange.lo: ../include/net-snmp/library/snmpusm.h
ipe./notification_log.lo: ../include/net-snmp/net-snmp-config.h
ipe./notification_log.lo: ../include/net-snmp/system/linux.h
ipe./notification_log.lo: ../include/net-snmp/system/sysv.h
ipe./notification_log.lo: ../include/net-snmp/system/generic.h
ipe./notification_log.lo: ../include/net-snmp/machine/generic.h
ipe./notification_log.lo: ../include/net-snmp/net-snmp-includes.h
ipe./notification_log.lo: ../include/net-snmp/definitions.h
ipe./notification_log.lo: ../include/net-snmp/types.h 
ipe./notification_log.lo: ../include/net-snmp/library/snmp_api.h
ipe./notification_log.lo: ../include/net-snmp/library/asn1.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_impl.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp-tc.h
ipe./notification_log.lo: ../include/net-snmp/utilities.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_client.h
ipe./notification_log.lo: ../include/net-snmp/library/system.h
ipe./notification_log.lo: ../include/net-snmp/library/tools.h
ipe./notification_log.lo: ../include/net-snmp/library/int64.h
ipe./notification_log.lo: ../include/net-snmp/library/mt_support.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./notification_log.lo: ../include/net-snmp/library/callback.h
ipe./notification_log.lo: ../include/net-snmp/library/data_list.h
ipe./notification_log.lo: ../include/net-snmp/library/oid_stash.h
ipe./notification_log.lo: ../include/net-snmp/library/check_varbind.h
ipe./notification_log.lo: ../include/net-snmp/library/container.h
ipe./notification_log.lo: ../include/net-snmp/library/factory.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_logging.h
ipe./notification_log.lo: ../include/net-snmp/library/container_binary_array.h
ipe./notification_log.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./notification_log.lo: ../include/net-snmp/library/container_iterator.h
ipe./notification_log.lo: ../include/net-snmp/library/container.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_assert.h
ipe./notification_log.lo: ../include/net-snmp/version.h
ipe./notification_log.lo: ../include/net-snmp/session_api.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_transport.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./notification_log.lo: ../include/net-snmp/library/ucd_compat.h
ipe./notification_log.lo: ../include/net-snmp/pdu_api.h
ipe./notification_log.lo: ../include/net-snmp/mib_api.h
ipe./notification_log.lo: ../include/net-snmp/library/mib.h
ipe./notification_log.lo: ../include/net-snmp/library/parse.h
ipe./notification_log.lo: ../include/net-snmp/varbind_api.h
ipe./notification_log.lo: ../include/net-snmp/config_api.h
ipe./notification_log.lo: ../include/net-snmp/library/read_config.h
ipe./notification_log.lo: ../include/net-snmp/library/default_store.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_enum.h
ipe./notification_log.lo: ../include/net-snmp/library/vacm.h
ipe./notification_log.lo: ../include/net-snmp/output_api.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_debug.h
ipe./notification_log.lo: ../include/net-snmp/snmpv3_api.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpv3.h
ipe./notification_log.lo: ../include/net-snmp/library/transform_oids.h
ipe./notification_log.lo: ../include/net-snmp/library/keytools.h
ipe./notification_log.lo: ../include/net-snmp/library/scapi.h
ipe./notification_log.lo: ../include/net-snmp/library/lcd_time.h
ipe./notification_log.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./notification_log.lo: ../include/net-snmp/library/snmpusm.h
ipe./notification_log.lo: ../include/net-snmp/agent/net-snmp-agent-includes.h
ipe./notification_log.lo: ../include/net-snmp/agent/snmp_agent.h
ipe./notification_log.lo: ../include/net-snmp/agent/snmp_vars.h
ipe./notification_log.lo: ../include/net-snmp/agent/agent_handler.h
ipe./notification_log.lo: ../include/net-snmp/agent/var_struct.h
ipe./notification_log.lo: ../include/net-snmp/agent/agent_registry.h
ipe./notification_log.lo: ../include/net-snmp/agent/ds_agent.h
ipe./notification_log.lo: ../include/net-snmp/agent/agent_read_config.h
ipe./notification_log.lo: ../include/net-snmp/agent/agent_trap.h
ipe./notification_log.lo: ../include/net-snmp/agent/all_helpers.h
ipe./notification_log.lo: ../include/net-snmp/agent/instance.h
ipe./notification_log.lo: ../include/net-snmp/agent/baby_steps.h
ipe./notification_log.lo: ../include/net-snmp/agent/scalar.h
ipe./notification_log.lo: ../include/net-snmp/agent/scalar_group.h
ipe./notification_log.lo: ../include/net-snmp/agent/watcher.h
ipe./notification_log.lo: ../include/net-snmp/agent/multiplexer.h
ipe./notification_log.lo: ../include/net-snmp/agent/null.h
ipe./notification_log.lo: ../include/net-snmp/agent/debug_handler.h
ipe./notification_log.lo: ../include/net-snmp/agent/cache_handler.h
ipe./notification_log.lo: ../include/net-snmp/agent/old_api.h
ipe./notification_log.lo: ../include/net-snmp/agent/read_only.h
ipe./notification_log.lo: ../include/net-snmp/agent/row_merge.h
ipe./notification_log.lo: ../include/net-snmp/agent/serialize.h
ipe./notification_log.lo: ../include/net-snmp/agent/bulk_to_next.h
ipe./notification_log.lo: ../include/net-snmp/agent/mode_end_call.h
ipe./notification_log.lo: ../include/net-snmp/agent/table.h
ipe./notification_log.lo: ../include/net-snmp/agent/table_data.h
ipe./notification_log.lo: ../include/net-snmp/agent/table_dataset.h
ipe./notification_log.lo: ../include/net-snmp/agent/table_iterator.h
ipe./notification_log.lo: ../include/net-snmp/agent/table_container.h
ipe./notification_log.lo: ../include/net-snmp/agent/mfd.h
ipe./notification_log.lo: ../include/net-snmp/agent/mib_module_config.h
ipe./notification_log.lo: ../include/net-snmp/agent/agent_module_config.h
ipe./notification_log.lo: snmptrapd_handlers.h snmptrapd_log.h
ipe./notification_log.lo: notification_log.h
ipe./snmpbulkget.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpbulkget.lo: ../include/net-snmp/system/linux.h
ipe./snmpbulkget.lo: ../include/net-snmp/system/sysv.h
ipe./snmpbulkget.lo: ../include/net-snmp/system/generic.h
ipe./snmpbulkget.lo: ../include/net-snmp/machine/generic.h
ipe./snmpbulkget.lo:  ../include/net-snmp/utilities.h
ipe./snmpbulkget.lo: ../include/net-snmp/types.h 
ipe./snmpbulkget.lo: ../include/net-snmp/definitions.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/asn1.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/system.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/tools.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/int64.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/callback.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/data_list.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/container.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/factory.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/container.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpbulkget.lo: ../include/net-snmp/version.h 
ipe./snmpbulkget.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpbulkget.lo: ../include/net-snmp/session_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpbulkget.lo: ../include/net-snmp/pdu_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/mib_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/mib.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/parse.h
ipe./snmpbulkget.lo: ../include/net-snmp/varbind_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/config_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/read_config.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/default_store.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/vacm.h
ipe./snmpbulkget.lo: ../include/net-snmp/output_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpbulkget.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/keytools.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/scapi.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpbulkget.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/system/linux.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/system/sysv.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/system/generic.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/machine/generic.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/definitions.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/types.h 
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/asn1.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/utilities.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/system.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/tools.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/int64.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/callback.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/data_list.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/container.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/factory.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/container.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/version.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/session_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/pdu_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/mib_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/mib.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/parse.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/varbind_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/config_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/read_config.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/default_store.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/vacm.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/output_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/keytools.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/scapi.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpbulkwalk.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpdelta.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpdelta.lo: ../include/net-snmp/system/linux.h
ipe./snmpdelta.lo: ../include/net-snmp/system/sysv.h
ipe./snmpdelta.lo: ../include/net-snmp/system/generic.h
ipe./snmpdelta.lo: ../include/net-snmp/machine/generic.h
ipe./snmpdelta.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpdelta.lo: ../include/net-snmp/definitions.h
ipe./snmpdelta.lo: ../include/net-snmp/types.h 
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpdelta.lo: ../include/net-snmp/library/asn1.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpdelta.lo: ../include/net-snmp/utilities.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpdelta.lo: ../include/net-snmp/library/system.h
ipe./snmpdelta.lo: ../include/net-snmp/library/tools.h
ipe./snmpdelta.lo: ../include/net-snmp/library/int64.h
ipe./snmpdelta.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpdelta.lo: ../include/net-snmp/library/callback.h
ipe./snmpdelta.lo: ../include/net-snmp/library/data_list.h
ipe./snmpdelta.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpdelta.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpdelta.lo: ../include/net-snmp/library/container.h
ipe./snmpdelta.lo: ../include/net-snmp/library/factory.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpdelta.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpdelta.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpdelta.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpdelta.lo: ../include/net-snmp/library/container.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpdelta.lo: ../include/net-snmp/version.h
ipe./snmpdelta.lo: ../include/net-snmp/session_api.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpdelta.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpdelta.lo: ../include/net-snmp/pdu_api.h
ipe./snmpdelta.lo: ../include/net-snmp/mib_api.h
ipe./snmpdelta.lo: ../include/net-snmp/library/mib.h
ipe./snmpdelta.lo: ../include/net-snmp/library/parse.h
ipe./snmpdelta.lo: ../include/net-snmp/varbind_api.h
ipe./snmpdelta.lo: ../include/net-snmp/config_api.h
ipe./snmpdelta.lo: ../include/net-snmp/library/read_config.h
ipe./snmpdelta.lo: ../include/net-snmp/library/default_store.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpdelta.lo: ../include/net-snmp/library/vacm.h
ipe./snmpdelta.lo: ../include/net-snmp/output_api.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpdelta.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpdelta.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpdelta.lo: ../include/net-snmp/library/keytools.h
ipe./snmpdelta.lo: ../include/net-snmp/library/scapi.h
ipe./snmpdelta.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpdelta.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpdf.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpdf.lo: ../include/net-snmp/system/linux.h
ipe./snmpdf.lo: ../include/net-snmp/system/sysv.h
ipe./snmpdf.lo: ../include/net-snmp/system/generic.h
ipe./snmpdf.lo: ../include/net-snmp/machine/generic.h 
ipe./snmpdf.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpdf.lo: ../include/net-snmp/definitions.h ../include/net-snmp/types.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpdf.lo: ../include/net-snmp/library/asn1.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpdf.lo: ../include/net-snmp/utilities.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpdf.lo: ../include/net-snmp/library/system.h
ipe./snmpdf.lo: ../include/net-snmp/library/tools.h
ipe./snmpdf.lo: ../include/net-snmp/library/int64.h
ipe./snmpdf.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpdf.lo: ../include/net-snmp/library/callback.h
ipe./snmpdf.lo: ../include/net-snmp/library/data_list.h
ipe./snmpdf.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpdf.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpdf.lo: ../include/net-snmp/library/container.h
ipe./snmpdf.lo: ../include/net-snmp/library/factory.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpdf.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpdf.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpdf.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpdf.lo: ../include/net-snmp/library/container.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpdf.lo: ../include/net-snmp/version.h
ipe./snmpdf.lo: ../include/net-snmp/session_api.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpdf.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpdf.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmpdf.lo: ../include/net-snmp/library/mib.h
ipe./snmpdf.lo: ../include/net-snmp/library/parse.h
ipe./snmpdf.lo: ../include/net-snmp/varbind_api.h
ipe./snmpdf.lo: ../include/net-snmp/config_api.h
ipe./snmpdf.lo: ../include/net-snmp/library/read_config.h
ipe./snmpdf.lo: ../include/net-snmp/library/default_store.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpdf.lo: ../include/net-snmp/library/vacm.h
ipe./snmpdf.lo: ../include/net-snmp/output_api.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpdf.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpdf.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpdf.lo: ../include/net-snmp/library/keytools.h
ipe./snmpdf.lo: ../include/net-snmp/library/scapi.h
ipe./snmpdf.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpdf.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpget.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpget.lo: ../include/net-snmp/system/linux.h
ipe./snmpget.lo: ../include/net-snmp/system/sysv.h
ipe./snmpget.lo: ../include/net-snmp/system/generic.h
ipe./snmpget.lo: ../include/net-snmp/machine/generic.h 
ipe./snmpget.lo: ../include/net-snmp/utilities.h ../include/net-snmp/types.h
ipe./snmpget.lo:  ../include/net-snmp/definitions.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpget.lo: ../include/net-snmp/library/asn1.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpget.lo: ../include/net-snmp/library/system.h
ipe./snmpget.lo: ../include/net-snmp/library/tools.h
ipe./snmpget.lo: ../include/net-snmp/library/int64.h
ipe./snmpget.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpget.lo: ../include/net-snmp/library/callback.h
ipe./snmpget.lo: ../include/net-snmp/library/data_list.h
ipe./snmpget.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpget.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpget.lo: ../include/net-snmp/library/container.h
ipe./snmpget.lo: ../include/net-snmp/library/factory.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpget.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpget.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpget.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpget.lo: ../include/net-snmp/library/container.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpget.lo: ../include/net-snmp/version.h
ipe./snmpget.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpget.lo: ../include/net-snmp/session_api.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpget.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpget.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmpget.lo: ../include/net-snmp/library/mib.h
ipe./snmpget.lo: ../include/net-snmp/library/parse.h
ipe./snmpget.lo: ../include/net-snmp/varbind_api.h
ipe./snmpget.lo: ../include/net-snmp/config_api.h
ipe./snmpget.lo: ../include/net-snmp/library/read_config.h
ipe./snmpget.lo: ../include/net-snmp/library/default_store.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpget.lo: ../include/net-snmp/library/vacm.h
ipe./snmpget.lo: ../include/net-snmp/output_api.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpget.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpget.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpget.lo: ../include/net-snmp/library/keytools.h
ipe./snmpget.lo: ../include/net-snmp/library/scapi.h
ipe./snmpget.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpget.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpget.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpgetnext.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpgetnext.lo: ../include/net-snmp/system/linux.h
ipe./snmpgetnext.lo: ../include/net-snmp/system/sysv.h
ipe./snmpgetnext.lo: ../include/net-snmp/system/generic.h
ipe./snmpgetnext.lo: ../include/net-snmp/machine/generic.h
ipe./snmpgetnext.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpgetnext.lo: ../include/net-snmp/definitions.h
ipe./snmpgetnext.lo: ../include/net-snmp/types.h 
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/asn1.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpgetnext.lo: ../include/net-snmp/utilities.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/system.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/tools.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/int64.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/callback.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/data_list.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/container.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/factory.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/container.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpgetnext.lo: ../include/net-snmp/version.h
ipe./snmpgetnext.lo: ../include/net-snmp/session_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpgetnext.lo: ../include/net-snmp/pdu_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/mib_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/mib.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/parse.h
ipe./snmpgetnext.lo: ../include/net-snmp/varbind_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/config_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/read_config.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/default_store.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/vacm.h
ipe./snmpgetnext.lo: ../include/net-snmp/output_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpgetnext.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/keytools.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/scapi.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpgetnext.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpset.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpset.lo: ../include/net-snmp/system/linux.h
ipe./snmpset.lo: ../include/net-snmp/system/sysv.h
ipe./snmpset.lo: ../include/net-snmp/system/generic.h
ipe./snmpset.lo: ../include/net-snmp/machine/generic.h 
ipe./snmpset.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpset.lo: ../include/net-snmp/definitions.h
ipe./snmpset.lo: ../include/net-snmp/types.h 
ipe./snmpset.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpset.lo: ../include/net-snmp/library/asn1.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpset.lo: ../include/net-snmp/utilities.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpset.lo: ../include/net-snmp/library/system.h
ipe./snmpset.lo: ../include/net-snmp/library/tools.h
ipe./snmpset.lo: ../include/net-snmp/library/int64.h
ipe./snmpset.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpset.lo: ../include/net-snmp/library/callback.h
ipe./snmpset.lo: ../include/net-snmp/library/data_list.h
ipe./snmpset.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpset.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpset.lo: ../include/net-snmp/library/container.h
ipe./snmpset.lo: ../include/net-snmp/library/factory.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpset.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpset.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpset.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpset.lo: ../include/net-snmp/library/container.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpset.lo: ../include/net-snmp/version.h
ipe./snmpset.lo: ../include/net-snmp/session_api.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpset.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpset.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmpset.lo: ../include/net-snmp/library/mib.h
ipe./snmpset.lo: ../include/net-snmp/library/parse.h
ipe./snmpset.lo: ../include/net-snmp/varbind_api.h
ipe./snmpset.lo: ../include/net-snmp/config_api.h
ipe./snmpset.lo: ../include/net-snmp/library/read_config.h
ipe./snmpset.lo: ../include/net-snmp/library/default_store.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpset.lo: ../include/net-snmp/library/vacm.h
ipe./snmpset.lo: ../include/net-snmp/output_api.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpset.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpset.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpset.lo: ../include/net-snmp/library/keytools.h
ipe./snmpset.lo: ../include/net-snmp/library/scapi.h
ipe./snmpset.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpset.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpset.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpstatus.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpstatus.lo: ../include/net-snmp/system/linux.h
ipe./snmpstatus.lo: ../include/net-snmp/system/sysv.h
ipe./snmpstatus.lo: ../include/net-snmp/system/generic.h
ipe./snmpstatus.lo: ../include/net-snmp/machine/generic.h
ipe./snmpstatus.lo: ../include/net-snmp/utilities.h
ipe./snmpstatus.lo: ../include/net-snmp/types.h 
ipe./snmpstatus.lo: ../include/net-snmp/definitions.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpstatus.lo: ../include/net-snmp/library/asn1.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpstatus.lo: ../include/net-snmp/library/system.h
ipe./snmpstatus.lo: ../include/net-snmp/library/tools.h
ipe./snmpstatus.lo: ../include/net-snmp/library/int64.h
ipe./snmpstatus.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpstatus.lo: ../include/net-snmp/library/callback.h
ipe./snmpstatus.lo: ../include/net-snmp/library/data_list.h
ipe./snmpstatus.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpstatus.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpstatus.lo: ../include/net-snmp/library/container.h
ipe./snmpstatus.lo: ../include/net-snmp/library/factory.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpstatus.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpstatus.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpstatus.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpstatus.lo: ../include/net-snmp/library/container.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpstatus.lo: ../include/net-snmp/version.h
ipe./snmpstatus.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpstatus.lo: ../include/net-snmp/session_api.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpstatus.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpstatus.lo: ../include/net-snmp/pdu_api.h
ipe./snmpstatus.lo: ../include/net-snmp/mib_api.h
ipe./snmpstatus.lo: ../include/net-snmp/library/mib.h
ipe./snmpstatus.lo: ../include/net-snmp/library/parse.h
ipe./snmpstatus.lo: ../include/net-snmp/varbind_api.h
ipe./snmpstatus.lo: ../include/net-snmp/config_api.h
ipe./snmpstatus.lo: ../include/net-snmp/library/read_config.h
ipe./snmpstatus.lo: ../include/net-snmp/library/default_store.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpstatus.lo: ../include/net-snmp/library/vacm.h
ipe./snmpstatus.lo: ../include/net-snmp/output_api.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpstatus.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpstatus.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpstatus.lo: ../include/net-snmp/library/keytools.h
ipe./snmpstatus.lo: ../include/net-snmp/library/scapi.h
ipe./snmpstatus.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpstatus.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmptable.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmptable.lo: ../include/net-snmp/system/linux.h
ipe./snmptable.lo: ../include/net-snmp/system/sysv.h
ipe./snmptable.lo: ../include/net-snmp/system/generic.h
ipe./snmptable.lo: ../include/net-snmp/machine/generic.h
ipe./snmptable.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmptable.lo: ../include/net-snmp/definitions.h
ipe./snmptable.lo: ../include/net-snmp/types.h 
ipe./snmptable.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmptable.lo: ../include/net-snmp/library/asn1.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmptable.lo: ../include/net-snmp/utilities.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmptable.lo: ../include/net-snmp/library/system.h
ipe./snmptable.lo: ../include/net-snmp/library/tools.h
ipe./snmptable.lo: ../include/net-snmp/library/int64.h
ipe./snmptable.lo: ../include/net-snmp/library/mt_support.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmptable.lo: ../include/net-snmp/library/callback.h
ipe./snmptable.lo: ../include/net-snmp/library/data_list.h
ipe./snmptable.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmptable.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmptable.lo: ../include/net-snmp/library/container.h
ipe./snmptable.lo: ../include/net-snmp/library/factory.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmptable.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmptable.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmptable.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmptable.lo: ../include/net-snmp/library/container.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmptable.lo: ../include/net-snmp/version.h
ipe./snmptable.lo: ../include/net-snmp/session_api.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmptable.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmptable.lo: ../include/net-snmp/pdu_api.h
ipe./snmptable.lo: ../include/net-snmp/mib_api.h
ipe./snmptable.lo: ../include/net-snmp/library/mib.h
ipe./snmptable.lo: ../include/net-snmp/library/parse.h
ipe./snmptable.lo: ../include/net-snmp/varbind_api.h
ipe./snmptable.lo: ../include/net-snmp/config_api.h
ipe./snmptable.lo: ../include/net-snmp/library/read_config.h
ipe./snmptable.lo: ../include/net-snmp/library/default_store.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmptable.lo: ../include/net-snmp/library/vacm.h
ipe./snmptable.lo: ../include/net-snmp/output_api.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmptable.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmptable.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmptable.lo: ../include/net-snmp/library/keytools.h
ipe./snmptable.lo: ../include/net-snmp/library/scapi.h
ipe./snmptable.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmptable.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmptable.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmptest.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmptest.lo: ../include/net-snmp/system/linux.h
ipe./snmptest.lo: ../include/net-snmp/system/sysv.h
ipe./snmptest.lo: ../include/net-snmp/system/generic.h
ipe./snmptest.lo: ../include/net-snmp/machine/generic.h 
ipe./snmptest.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmptest.lo: ../include/net-snmp/definitions.h
ipe./snmptest.lo: ../include/net-snmp/types.h 
ipe./snmptest.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmptest.lo: ../include/net-snmp/library/asn1.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmptest.lo: ../include/net-snmp/utilities.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmptest.lo: ../include/net-snmp/library/system.h
ipe./snmptest.lo: ../include/net-snmp/library/tools.h
ipe./snmptest.lo: ../include/net-snmp/library/int64.h
ipe./snmptest.lo: ../include/net-snmp/library/mt_support.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmptest.lo: ../include/net-snmp/library/callback.h
ipe./snmptest.lo: ../include/net-snmp/library/data_list.h
ipe./snmptest.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmptest.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmptest.lo: ../include/net-snmp/library/container.h
ipe./snmptest.lo: ../include/net-snmp/library/factory.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmptest.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmptest.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmptest.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmptest.lo: ../include/net-snmp/library/container.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmptest.lo: ../include/net-snmp/version.h
ipe./snmptest.lo: ../include/net-snmp/session_api.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmptest.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmptest.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmptest.lo: ../include/net-snmp/library/mib.h
ipe./snmptest.lo: ../include/net-snmp/library/parse.h
ipe./snmptest.lo: ../include/net-snmp/varbind_api.h
ipe./snmptest.lo: ../include/net-snmp/config_api.h
ipe./snmptest.lo: ../include/net-snmp/library/read_config.h
ipe./snmptest.lo: ../include/net-snmp/library/default_store.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmptest.lo: ../include/net-snmp/library/vacm.h
ipe./snmptest.lo: ../include/net-snmp/output_api.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmptest.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmptest.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmptest.lo: ../include/net-snmp/library/keytools.h
ipe./snmptest.lo: ../include/net-snmp/library/scapi.h
ipe./snmptest.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmptest.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmptest.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmptranslate.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmptranslate.lo: ../include/net-snmp/system/linux.h
ipe./snmptranslate.lo: ../include/net-snmp/system/sysv.h
ipe./snmptranslate.lo: ../include/net-snmp/system/generic.h
ipe./snmptranslate.lo: ../include/net-snmp/machine/generic.h
ipe./snmptranslate.lo: ../include/net-snmp/utilities.h
ipe./snmptranslate.lo: ../include/net-snmp/types.h 
ipe./snmptranslate.lo: ../include/net-snmp/definitions.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmptranslate.lo: ../include/net-snmp/library/asn1.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmptranslate.lo: ../include/net-snmp/library/system.h
ipe./snmptranslate.lo: ../include/net-snmp/library/tools.h
ipe./snmptranslate.lo: ../include/net-snmp/library/int64.h
ipe./snmptranslate.lo: ../include/net-snmp/library/mt_support.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmptranslate.lo: ../include/net-snmp/library/callback.h
ipe./snmptranslate.lo: ../include/net-snmp/library/data_list.h
ipe./snmptranslate.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmptranslate.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmptranslate.lo: ../include/net-snmp/library/container.h
ipe./snmptranslate.lo: ../include/net-snmp/library/factory.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmptranslate.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmptranslate.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmptranslate.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmptranslate.lo: ../include/net-snmp/library/container.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmptranslate.lo: ../include/net-snmp/version.h
ipe./snmptranslate.lo: ../include/net-snmp/config_api.h
ipe./snmptranslate.lo: ../include/net-snmp/library/read_config.h
ipe./snmptranslate.lo: ../include/net-snmp/library/default_store.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmptranslate.lo: ../include/net-snmp/library/vacm.h
ipe./snmptranslate.lo: ../include/net-snmp/output_api.h
ipe./snmptranslate.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmptranslate.lo: ../include/net-snmp/mib_api.h
ipe./snmptranslate.lo: ../include/net-snmp/library/mib.h
ipe./snmptranslate.lo: ../include/net-snmp/library/parse.h
ipe./snmptranslate.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmptrap.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmptrap.lo: ../include/net-snmp/system/linux.h
ipe./snmptrap.lo: ../include/net-snmp/system/sysv.h
ipe./snmptrap.lo: ../include/net-snmp/system/generic.h
ipe./snmptrap.lo: ../include/net-snmp/machine/generic.h 
ipe./snmptrap.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmptrap.lo: ../include/net-snmp/definitions.h
ipe./snmptrap.lo: ../include/net-snmp/types.h 
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmptrap.lo: ../include/net-snmp/library/asn1.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmptrap.lo: ../include/net-snmp/utilities.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmptrap.lo: ../include/net-snmp/library/system.h
ipe./snmptrap.lo: ../include/net-snmp/library/tools.h
ipe./snmptrap.lo: ../include/net-snmp/library/int64.h
ipe./snmptrap.lo: ../include/net-snmp/library/mt_support.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmptrap.lo: ../include/net-snmp/library/callback.h
ipe./snmptrap.lo: ../include/net-snmp/library/data_list.h
ipe./snmptrap.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmptrap.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmptrap.lo: ../include/net-snmp/library/container.h
ipe./snmptrap.lo: ../include/net-snmp/library/factory.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmptrap.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmptrap.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmptrap.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmptrap.lo: ../include/net-snmp/library/container.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmptrap.lo: ../include/net-snmp/version.h
ipe./snmptrap.lo: ../include/net-snmp/session_api.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmptrap.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmptrap.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmptrap.lo: ../include/net-snmp/library/mib.h
ipe./snmptrap.lo: ../include/net-snmp/library/parse.h
ipe./snmptrap.lo: ../include/net-snmp/varbind_api.h
ipe./snmptrap.lo: ../include/net-snmp/config_api.h
ipe./snmptrap.lo: ../include/net-snmp/library/read_config.h
ipe./snmptrap.lo: ../include/net-snmp/library/default_store.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmptrap.lo: ../include/net-snmp/library/vacm.h
ipe./snmptrap.lo: ../include/net-snmp/output_api.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmptrap.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmptrap.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmptrap.lo: ../include/net-snmp/library/keytools.h
ipe./snmptrap.lo: ../include/net-snmp/library/scapi.h
ipe./snmptrap.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmptrap.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmptrapd.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmptrapd.lo: ../include/net-snmp/system/linux.h
ipe./snmptrapd.lo: ../include/net-snmp/system/sysv.h
ipe./snmptrapd.lo: ../include/net-snmp/system/generic.h
ipe./snmptrapd.lo: ../include/net-snmp/machine/generic.h
ipe./snmptrapd.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmptrapd.lo: ../include/net-snmp/definitions.h
ipe./snmptrapd.lo: ../include/net-snmp/types.h 
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmptrapd.lo: ../include/net-snmp/library/asn1.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmptrapd.lo: ../include/net-snmp/utilities.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmptrapd.lo: ../include/net-snmp/library/system.h
ipe./snmptrapd.lo: ../include/net-snmp/library/tools.h
ipe./snmptrapd.lo: ../include/net-snmp/library/int64.h
ipe./snmptrapd.lo: ../include/net-snmp/library/mt_support.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmptrapd.lo: ../include/net-snmp/library/callback.h
ipe./snmptrapd.lo: ../include/net-snmp/library/data_list.h
ipe./snmptrapd.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmptrapd.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmptrapd.lo: ../include/net-snmp/library/container.h
ipe./snmptrapd.lo: ../include/net-snmp/library/factory.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmptrapd.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmptrapd.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmptrapd.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmptrapd.lo: ../include/net-snmp/library/container.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmptrapd.lo: ../include/net-snmp/version.h
ipe./snmptrapd.lo: ../include/net-snmp/session_api.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmptrapd.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmptrapd.lo: ../include/net-snmp/pdu_api.h
ipe./snmptrapd.lo: ../include/net-snmp/mib_api.h
ipe./snmptrapd.lo: ../include/net-snmp/library/mib.h
ipe./snmptrapd.lo: ../include/net-snmp/library/parse.h
ipe./snmptrapd.lo: ../include/net-snmp/varbind_api.h
ipe./snmptrapd.lo: ../include/net-snmp/config_api.h
ipe./snmptrapd.lo: ../include/net-snmp/library/read_config.h
ipe./snmptrapd.lo: ../include/net-snmp/library/default_store.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmptrapd.lo: ../include/net-snmp/library/vacm.h
ipe./snmptrapd.lo: ../include/net-snmp/output_api.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmptrapd.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmptrapd.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmptrapd.lo: ../include/net-snmp/library/keytools.h
ipe./snmptrapd.lo: ../include/net-snmp/library/scapi.h
ipe./snmptrapd.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmptrapd.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/net-snmp-agent-includes.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/snmp_agent.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/snmp_vars.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/agent_handler.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/var_struct.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/agent_registry.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/ds_agent.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/agent_read_config.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/agent_trap.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/all_helpers.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/instance.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/baby_steps.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/scalar.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/scalar_group.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/watcher.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/multiplexer.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/null.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/debug_handler.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/cache_handler.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/old_api.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/read_only.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/row_merge.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/serialize.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/bulk_to_next.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/mode_end_call.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/table.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/table_data.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/table_dataset.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/table_iterator.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/table_container.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/mfd.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/mib_module_config.h
ipe./snmptrapd.lo: ../include/net-snmp/agent/agent_module_config.h
ipe./snmptrapd.lo: snmptrapd_handlers.h snmptrapd_log.h notification_log.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/system/linux.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/system/sysv.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/system/generic.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/machine/generic.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/config_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/types.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/definitions.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/asn1.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/read_config.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/default_store.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/vacm.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/output_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/mib_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/mib.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/parse.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/callback.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/utilities.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/system.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/tools.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/int64.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/mt_support.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/data_list.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/container.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/factory.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/container.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/version.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/session_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/pdu_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/varbind_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/keytools.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/scapi.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/net-snmp-agent-includes.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/snmp_agent.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/snmp_vars.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/agent_handler.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/var_struct.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/agent_registry.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/ds_agent.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/agent_read_config.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/agent_trap.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/all_helpers.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/instance.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/baby_steps.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/scalar.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/scalar_group.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/watcher.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/multiplexer.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/null.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/debug_handler.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/cache_handler.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/old_api.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/read_only.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/row_merge.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/serialize.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/bulk_to_next.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/mode_end_call.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/table.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/table_data.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/table_dataset.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/table_iterator.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/table_container.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/mfd.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/mib_module_config.h
ipe./snmptrapd_handlers.lo: ../include/net-snmp/agent/agent_module_config.h
ipe./snmptrapd_handlers.lo: ../agent/mibgroup/utilities/execute.h
ipe./snmptrapd_handlers.lo: snmptrapd_handlers.h snmptrapd_log.h
ipe./snmptrapd_log.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmptrapd_log.lo: ../include/net-snmp/system/linux.h
ipe./snmptrapd_log.lo: ../include/net-snmp/system/sysv.h
ipe./snmptrapd_log.lo: ../include/net-snmp/system/generic.h
ipe./snmptrapd_log.lo: ../include/net-snmp/machine/generic.h
ipe./snmptrapd_log.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmptrapd_log.lo: ../include/net-snmp/definitions.h
ipe./snmptrapd_log.lo: ../include/net-snmp/types.h 
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/asn1.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmptrapd_log.lo: ../include/net-snmp/utilities.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/system.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/tools.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/int64.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/mt_support.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/callback.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/data_list.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/container.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/factory.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/container.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmptrapd_log.lo: ../include/net-snmp/version.h
ipe./snmptrapd_log.lo: ../include/net-snmp/session_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmptrapd_log.lo: ../include/net-snmp/pdu_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/mib_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/mib.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/parse.h
ipe./snmptrapd_log.lo: ../include/net-snmp/varbind_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/config_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/read_config.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/default_store.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/vacm.h
ipe./snmptrapd_log.lo: ../include/net-snmp/output_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmptrapd_log.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/keytools.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/scapi.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmptrapd_log.lo: ../include/net-snmp/library/snmpusm.h snmptrapd_log.h
ipe./snmpusm.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpusm.lo: ../include/net-snmp/system/linux.h
ipe./snmpusm.lo: ../include/net-snmp/system/sysv.h
ipe./snmpusm.lo: ../include/net-snmp/system/generic.h
ipe./snmpusm.lo: ../include/net-snmp/machine/generic.h 
ipe./snmpusm.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpusm.lo: ../include/net-snmp/definitions.h
ipe./snmpusm.lo: ../include/net-snmp/types.h 
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpusm.lo: ../include/net-snmp/library/asn1.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpusm.lo: ../include/net-snmp/utilities.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpusm.lo: ../include/net-snmp/library/system.h
ipe./snmpusm.lo: ../include/net-snmp/library/tools.h
ipe./snmpusm.lo: ../include/net-snmp/library/int64.h
ipe./snmpusm.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpusm.lo: ../include/net-snmp/library/callback.h
ipe./snmpusm.lo: ../include/net-snmp/library/data_list.h
ipe./snmpusm.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpusm.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpusm.lo: ../include/net-snmp/library/container.h
ipe./snmpusm.lo: ../include/net-snmp/library/factory.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpusm.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpusm.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpusm.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpusm.lo: ../include/net-snmp/library/container.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpusm.lo: ../include/net-snmp/version.h
ipe./snmpusm.lo: ../include/net-snmp/session_api.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpusm.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpusm.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmpusm.lo: ../include/net-snmp/library/mib.h
ipe./snmpusm.lo: ../include/net-snmp/library/parse.h
ipe./snmpusm.lo: ../include/net-snmp/varbind_api.h
ipe./snmpusm.lo: ../include/net-snmp/config_api.h
ipe./snmpusm.lo: ../include/net-snmp/library/read_config.h
ipe./snmpusm.lo: ../include/net-snmp/library/default_store.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpusm.lo: ../include/net-snmp/library/vacm.h
ipe./snmpusm.lo: ../include/net-snmp/output_api.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpusm.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpusm.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpusm.lo: ../include/net-snmp/library/keytools.h
ipe./snmpusm.lo: ../include/net-snmp/library/scapi.h
ipe./snmpusm.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpusm.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpvacm.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpvacm.lo: ../include/net-snmp/system/linux.h
ipe./snmpvacm.lo: ../include/net-snmp/system/sysv.h
ipe./snmpvacm.lo: ../include/net-snmp/system/generic.h
ipe./snmpvacm.lo: ../include/net-snmp/machine/generic.h 
ipe./snmpvacm.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpvacm.lo: ../include/net-snmp/definitions.h
ipe./snmpvacm.lo: ../include/net-snmp/types.h 
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpvacm.lo: ../include/net-snmp/library/asn1.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpvacm.lo: ../include/net-snmp/utilities.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpvacm.lo: ../include/net-snmp/library/system.h
ipe./snmpvacm.lo: ../include/net-snmp/library/tools.h
ipe./snmpvacm.lo: ../include/net-snmp/library/int64.h
ipe./snmpvacm.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpvacm.lo: ../include/net-snmp/library/callback.h
ipe./snmpvacm.lo: ../include/net-snmp/library/data_list.h
ipe./snmpvacm.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpvacm.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpvacm.lo: ../include/net-snmp/library/container.h
ipe./snmpvacm.lo: ../include/net-snmp/library/factory.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpvacm.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpvacm.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpvacm.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpvacm.lo: ../include/net-snmp/library/container.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpvacm.lo: ../include/net-snmp/version.h
ipe./snmpvacm.lo: ../include/net-snmp/session_api.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpvacm.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpvacm.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmpvacm.lo: ../include/net-snmp/library/mib.h
ipe./snmpvacm.lo: ../include/net-snmp/library/parse.h
ipe./snmpvacm.lo: ../include/net-snmp/varbind_api.h
ipe./snmpvacm.lo: ../include/net-snmp/config_api.h
ipe./snmpvacm.lo: ../include/net-snmp/library/read_config.h
ipe./snmpvacm.lo: ../include/net-snmp/library/default_store.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpvacm.lo: ../include/net-snmp/library/vacm.h
ipe./snmpvacm.lo: ../include/net-snmp/output_api.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpvacm.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpvacm.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpvacm.lo: ../include/net-snmp/library/keytools.h
ipe./snmpvacm.lo: ../include/net-snmp/library/scapi.h
ipe./snmpvacm.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpvacm.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpwalk.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpwalk.lo: ../include/net-snmp/system/linux.h
ipe./snmpwalk.lo: ../include/net-snmp/system/sysv.h
ipe./snmpwalk.lo: ../include/net-snmp/system/generic.h
ipe./snmpwalk.lo: ../include/net-snmp/machine/generic.h 
ipe./snmpwalk.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpwalk.lo: ../include/net-snmp/definitions.h
ipe./snmpwalk.lo: ../include/net-snmp/types.h 
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpwalk.lo: ../include/net-snmp/library/asn1.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpwalk.lo: ../include/net-snmp/utilities.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpwalk.lo: ../include/net-snmp/library/system.h
ipe./snmpwalk.lo: ../include/net-snmp/library/tools.h
ipe./snmpwalk.lo: ../include/net-snmp/library/int64.h
ipe./snmpwalk.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpwalk.lo: ../include/net-snmp/library/callback.h
ipe./snmpwalk.lo: ../include/net-snmp/library/data_list.h
ipe./snmpwalk.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpwalk.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpwalk.lo: ../include/net-snmp/library/container.h
ipe./snmpwalk.lo: ../include/net-snmp/library/factory.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpwalk.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpwalk.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpwalk.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpwalk.lo: ../include/net-snmp/library/container.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpwalk.lo: ../include/net-snmp/version.h
ipe./snmpwalk.lo: ../include/net-snmp/session_api.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpwalk.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpwalk.lo: ../include/net-snmp/pdu_api.h ../include/net-snmp/mib_api.h
ipe./snmpwalk.lo: ../include/net-snmp/library/mib.h
ipe./snmpwalk.lo: ../include/net-snmp/library/parse.h
ipe./snmpwalk.lo: ../include/net-snmp/varbind_api.h
ipe./snmpwalk.lo: ../include/net-snmp/config_api.h
ipe./snmpwalk.lo: ../include/net-snmp/library/read_config.h
ipe./snmpwalk.lo: ../include/net-snmp/library/default_store.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpwalk.lo: ../include/net-snmp/library/vacm.h
ipe./snmpwalk.lo: ../include/net-snmp/output_api.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpwalk.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpwalk.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpwalk.lo: ../include/net-snmp/library/keytools.h
ipe./snmpwalk.lo: ../include/net-snmp/library/scapi.h
ipe./snmpwalk.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpwalk.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/system/linux.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/system/sysv.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/system/generic.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/machine/generic.h
ipe./snmpnetstat/if.lo: ./snmpnetstat/main.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/definitions.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/types.h 
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/asn1.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/utilities.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/system.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/tools.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/int64.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/callback.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/data_list.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/factory.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/version.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/session_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/pdu_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/mib_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/mib.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/parse.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/varbind_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/config_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/read_config.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/default_store.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/vacm.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/output_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/keytools.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/scapi.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpnetstat/if.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpnetstat/if.lo: ./snmpnetstat/netstat.h
ipe./snmpnetstat/inet6.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpnetstat/inet6.lo: ../include/net-snmp/system/linux.h
ipe./snmpnetstat/inet6.lo: ../include/net-snmp/system/sysv.h
ipe./snmpnetstat/inet6.lo: ../include/net-snmp/system/generic.h
ipe./snmpnetstat/inet6.lo: ../include/net-snmp/machine/generic.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/system/linux.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/system/sysv.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/system/generic.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/machine/generic.h
ipe./snmpnetstat/inet.lo:  ./snmpnetstat/main.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/definitions.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/types.h 
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/asn1.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/utilities.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/system.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/tools.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/int64.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/callback.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/data_list.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/factory.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/version.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/session_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/pdu_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/mib_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/mib.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/parse.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/varbind_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/config_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/read_config.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/default_store.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/vacm.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/output_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/keytools.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/scapi.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpnetstat/inet.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpnetstat/inet.lo: ./snmpnetstat/netstat.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/system/linux.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/system/sysv.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/system/generic.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/machine/generic.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/utilities.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/types.h 
ipe./snmpnetstat/main.lo: ../include/net-snmp/definitions.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/asn1.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/system.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/tools.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/int64.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/callback.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/data_list.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/factory.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/version.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/session_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/pdu_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/mib_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/mib.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/parse.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/varbind_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/config_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/read_config.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/default_store.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/vacm.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/output_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/keytools.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/scapi.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpnetstat/main.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpnetstat/main.lo: ./snmpnetstat/netstat.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/system/linux.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/system/sysv.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/system/generic.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/machine/generic.h
ipe./snmpnetstat/route.lo:  ./snmpnetstat/main.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/definitions.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/types.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/asn1.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/utilities.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/system.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/tools.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/int64.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/callback.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/data_list.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/factory.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/version.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/session_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/pdu_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/mib_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/mib.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/parse.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/varbind_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/config_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/read_config.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/default_store.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/vacm.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/output_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/keytools.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/scapi.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpnetstat/route.lo: ../include/net-snmp/library/snmpusm.h
ipe./snmpnetstat/route.lo: ./snmpnetstat/netstat.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/net-snmp-config.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/system/linux.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/system/sysv.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/system/generic.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/machine/generic.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/net-snmp-includes.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/definitions.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/types.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/asn1.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_impl.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp-tc.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/utilities.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_client.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/system.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/tools.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/int64.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/mt_support.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_alarm.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/callback.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/data_list.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/oid_stash.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/check_varbind.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/factory.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_logging.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/container_binary_array.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/container_list_ssll.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/container_iterator.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/container.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_assert.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/version.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/session_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_transport.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpCallbackDomain.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpUnixDomain.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpUDPDomain.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpTCPDomain.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpUDPIPv6Domain.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpTCPIPv6Domain.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpIPXDomain.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/ucd_compat.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/pdu_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/mib_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/mib.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/parse.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/varbind_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/config_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/read_config.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/default_store.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_parse_args.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_enum.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/vacm.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/output_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_debug.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/snmpv3_api.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpv3.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/transform_oids.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/keytools.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/scapi.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/lcd_time.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmp_secmod.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpv3-security-includes.h
ipe./snmpnetstat/winstub.lo: ../include/net-snmp/library/snmpusm.h
