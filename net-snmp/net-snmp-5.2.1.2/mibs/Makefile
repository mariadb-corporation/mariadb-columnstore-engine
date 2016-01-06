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
# Makefile to install mib files.
#

top_builddir=..



#
# Stuff here
#
OTHERINSTALL=mibsinstall
OTHERUNINSTALL=mibsuninstall


V1MIBS	= RFC1155-SMI.txt RFC1213-MIB.txt RFC-1215.txt

V2MIBS = SNMPv2-CONF.txt SNMPv2-SMI.txt SNMPv2-TC.txt SNMPv2-TM.txt \
	SNMPv2-MIB.txt

V3MIBS	= SNMP-FRAMEWORK-MIB.txt SNMP-MPD-MIB.txt SNMP-TARGET-MIB.txt \
	SNMP-NOTIFICATION-MIB.txt SNMP-PROXY-MIB.txt \
	SNMP-USER-BASED-SM-MIB.txt SNMP-VIEW-BASED-ACM-MIB.txt \
	SNMP-COMMUNITY-MIB.txt TRANSPORT-ADDRESS-MIB.txt

AGENTMIBS = AGENTX-MIB.txt SMUX-MIB.txt

IANAMIBS = IANAifType-MIB.txt IANA-LANGUAGE-MIB.txt \
	IANA-ADDRESS-FAMILY-NUMBERS-MIB.txt

RFCMIBS	= IF-MIB.txt IF-INVERTED-STACK-MIB.txt \
	EtherLike-MIB.txt \
	IP-MIB.txt IP-FORWARD-MIB.txt IANA-RTPROTO-MIB.txt \
	TCP-MIB.txt UDP-MIB.txt \
	INET-ADDRESS-MIB.txt HCNUM-TC.txt \
	HOST-RESOURCES-MIB.txt HOST-RESOURCES-TYPES.txt \
	RMON-MIB.txt \
	IPV6-TC.txt IPV6-MIB.txt IPV6-ICMP-MIB.txt IPV6-TCP-MIB.txt \
	IPV6-UDP-MIB.txt \
	DISMAN-SCRIPT-MIB.txt DISMAN-SCHEDULE-MIB.txt \
	NOTIFICATION-LOG-MIB.txt SNMP-USM-AES-MIB.txt \
	SNMP-USM-DH-OBJECTS-MIB.txt

NETSNMPMIBS = NET-SNMP-TC.txt NET-SNMP-MIB.txt NET-SNMP-AGENT-MIB.txt \
	NET-SNMP-EXAMPLES-MIB.txt NET-SNMP-EXTEND-MIB.txt

UCDMIBS = UCD-SNMP-MIB.txt UCD-DEMO-MIB.txt UCD-IPFWACC-MIB.txt \
	UCD-DLMOD-MIB.txt UCD-DISKIO-MIB.txt

DEFAULTMIBS =  UCD-SNMP-MIB.txt UCD-DEMO-MIB.txt SNMP-TARGET-MIB.txt NET-SNMP-AGENT-MIB.txt DISMAN-EVENT-MIB.txt SNMP-VIEW-BASED-ACM-MIB.txt SNMP-COMMUNITY-MIB.txt NET-SNMP-EXTEND-MIB.txt UCD-DLMOD-MIB.txt SNMP-FRAMEWORK-MIB.txt SNMP-MPD-MIB.txt SNMP-USER-BASED-SM-MIB.txt SNMP-USER-BASED-SM-MIB.txt SNMP-NOTIFICATION-MIB.txt SNMPv2-TM.txt

MIBS	= $(V1MIBS) $(V2MIBS) $(V3MIBS) $(RFCMIBS) \
	$(AGENTMIBS) $(IANAMIBS) \
	$(NETSNMPMIBS) $(UCDMIBS) $(DEFAULTMIBS)

all: standardall

mibsinstall: installdirs
	@for i in $(MIBS) ; do \
		$(INSTALL_DATA) $(srcdir)/$$i $(INSTALL_PREFIX)$(mibdir) ; \
		echo "install: installed $$i in $(INSTALL_PREFIX)$(mibdir)" ; \
	done

mibsuninstall: installdirs
	@for i in $(MIBS) ; do \
		rm -f $(INSTALL_PREFIX)$(mibdir)/$$i ; \
		echo "removed $$i from $(INSTALL_PREFIX)$(mibdir)" ; \
	done

installdirs:
	@$(SHELL) $(srcdir)/../mkinstalldirs $(INSTALL_PREFIX)$(mibdir)

htmldir:
	@if test ! -d html ; then \
		mkdir html ; \
	fi
	rm -f html/index.html
	perl makehtml.pl $(MIBS) > html/index.html

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
