#
# Minimum environment and virtual path setup
#
SHELL		= /bin/sh
srcdir		= .
top_srcdir	= .
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
# Makefile.in (at the root of net-snmp)
#

top_builddir	= .


SUBDIRS		= snmplib  agent apps man local mibs
TESTDIRS	= testing

CPP		= gcc -E 					        \
		-Iinclude -I$(srcdir)/include -I$(srcdir)/agent/mibgroup -I. -I$(srcdir)	\
		-DDONT_INC_STRUCTS -DBINDIR=$(bindir) 		        \
		$(EXTRACPPFLAGS)

INSTALLHEADERS=version.h
INCLUDESUBDIR=system
INCLUDESUBDIRHEADERS= aix.h bsd.h bsdi3.h bsdi4.h bsdi.h cygwin.h \
	darwin.h darwin7.h freebsd5.h freebsd6.h \
	dynix.h freebsd2.h freebsd3.h freebsd4.h freebsd.h generic.h hpux.h \
	irix.h linux.h mingw32.h mips.h netbsd.h openbsd.h osf5.h \
	solaris2.6.h solaris2.7.h solaris2.8.h solaris2.9.h solaris.h \
	sunos.h svr5.h sysv.h ultrix4.h
INCLUDESUBDIR2=machine
INCLUDESUBDIRHEADERS2=generic.h
INSTALLBUILTHEADERS=include/net-snmp/net-snmp-config.h
INSTALLBINSCRIPTS=net-snmp-config
INSTALLUCDHEADERS=ucd-snmp-config.h version.h mib_module_config.h

#
# other install rules.
#
OTHERINSTALL=copypersistentfiles 
OTHERUNINSTALL=
COPY_PERSISTENT_FILES=no
PERSISTENT_DIRECTORY=/var/net-snmp
UCDPERSISTENT_DIRECTORY=/var/ucd-snmp

#
# perl specific
#
# yes, order matters here.  default_store must occur before anything else
PERLMODULES=default_store SNMP ASN OID agent
PERLARGS=

#
# libtool
#
LIBTOOL_DEPS = ./ltmain.sh

#
# targets
#
all:    sedscript EXAMPLE.conf standardall net-snmp-config-x  

start-flag:
	@touch build-in-progress-flag

end-flag:
	@rm -f build-in-progress-flag > /dev/null 2>&1 

libtool:	$(LIBTOOL_DEPS)
	$(SHELL) ./config.status --recheck

#
# local build rules
#
sedscript: sedscript.in include/net-snmp/net-snmp-config.h $(srcdir)/agent/mibgroup/mibdefs.h
	$(CPP) $(srcdir)/sedscript.in | egrep '^s[/#]' | sed 's/REMOVEME//g;s# */#/#g;s/ *#/#/g;s#/ *#/#g;s/# g/#g/;' > sedscript
	echo 's#DATADIR#$(datadir)#g' >> sedscript
	echo 's#LIBDIR#$(libdir)#g' >> sedscript
	echo 's#BINDIR#$(bindir)#g' >> sedscript
	echo 's#PERSISTENT_DIRECTORY#$(PERSISTENT_DIRECTORY)#g' >> sedscript
	echo 's#SYSCONFDIR#${prefix}/etc#g' >> sedscript

EXAMPLE.conf: sedscript EXAMPLE.conf.def
	$(SED) -f sedscript $(srcdir)/EXAMPLE.conf.def > EXAMPLE.conf

docs: docsdir mancp

docsdir: docsdox docsmunge

docsdox: doxygen.conf
	srcdir=$(srcdir) doxygen $(srcdir)/doxygen.conf
	-rm docs/man/man3/todo.3
	-rm docs/man/man3/deprecated.3

docsmunge:
	find docs/html -name \*.html -exec perl -p -i -e 's/Generated on/<!--#include virtual=\"\/sfbutton.html\" --><br>Generated on\n/; s/<body/<body bgcolor=white/' {} \;


mancp:
	@for i in docs/man/man3/* ; do \
	     cp $$i man/netsnmp_`basename $$i` ; \
	done

net-snmp-config-x: net-snmp-config
	chmod a+x net-snmp-config
	touch net-snmp-config-x

#
# extra install rules
#

copypersistentfiles:
	@if test "$(COPY_PERSISTENT_FILES)" = "yes" -a -d $(UCDPERSISTENT_DIRECTORY) -a ! -d $(PERSISTENT_DIRECTORY) ; then \
		cp -pr $(UCDPERSISTENT_DIRECTORY) $(PERSISTENT_DIRECTORY) ; \
		echo "copying $(UCDPERSISTENT_DIRECTORY) to $(PERSISTENT_DIRECTORY)" ; \
	fi
#
# test targets
#
test: all testdirs
	( cd testing; $(MAKE) test )

testdirs:
	for i in $(TESTDIRS) ; do	\
           ( cd $$i ; $(MAKE) ) ;		\
           if test $$? != 0 ; then \
              exit 1 ; \
           fi  \
	done

distall: ${srcdir}/configure ${srcdir}/include/net-snmp/net-snmp-config.h 

OTHERCLEANTARGETS=EXAMPLE.conf sedscript
OTHERCLEANTODOS=perlclean

#
# perl specific build rules
#
perlmodules: perlmakefiles
	@(cd perl ; $(MAKE)) ; \
        if test $$? != 0 ; then \
           exit 1 ; \
        fi

perlmakefiles:
	@if test ! -f perl/Makefile; then \
	  (dir=`pwd`; \
	   cd perl ; \
	   perl Makefile.PL -NET-SNMP-IN-SOURCE=true -NET-SNMP-CONFIG="sh $$dir/net-snmp-config" $(PERLARGS) ) ; \
        fi

perlinstall:
	@(cd perl ; $(MAKE) install) ; \
        if test $$? != 0 ; then \
           exit 1 ; \
        fi

perluninstall:
	@(cd perl ; $(MAKE) uninstall) ; \
        if test $$? != 0 ; then \
           exit 1 ; \
        fi

perltest:
	@(cd perl ; $(MAKE) test) ; \
	if test $$? != 0 ; then \
	   exit 1 ; \
	fi

perlclean:
	@if test -f perl/Makefile; then \
	   ( cd perl ; $(MAKE) clean ) ; \
	fi
perlrealclean:
	@if test -f perl/Makefile; then \
	   ( cd perl ; $(MAKE) realclean ) ; \
	fi


#
# make distclean completely removes all traces of building including
# any files generated by configure itself.
#
distclean: perlrealclean clean configclean tarclean

makefileclean:
	rm -f Makefile snmplib/Makefile				\
		agent/Makefile agent/mibgroup/Makefile		\
		agent/helpers/Makefile				\
		apps/Makefile  apps/snmpnetstat/Makefile	\
		man/Makefile mibs/Makefile ov/Makefile		\
		local/Makefile testing/Makefile

configclean: makefileclean
	rm -f config.cache config.status config.log \
		libtool include/net-snmp/net-snmp-config.h \
		net-snmp-config net-snmp-config-x configure-summary
	rm -f mibs/.index
	rm -f include/net-snmp/agent/mib_module_config.h		\
		include/net-snmp/agent/agent_module_config.h		\
		include/net-snmp/library/snmpv3-security-includes.h \
		snmplib/snmpsm_init.h                   \
		agent/mibgroup/agent_module_includes.h 	\
		agent/mibgroup/agent_module_inits.h 	\
		agent/mibgroup/agent_module_shutdown.h 	\
		agent/mibgroup/agent_module_dot_conf.h  \
		agent/mibgroup/mib_module_includes.h 	\
		agent/mibgroup/mib_module_inits.h 	\
		agent/mibgroup/mib_module_shutdown.h 	\
		agent/mibgroup/mib_module_dot_conf.h
	rm -f *.core

#
# Configure script related targets
#
touchit:
	touch configure include/net-snmp/net-snmp-config.h.in
	touch config.status
	touch stamp-h stamp-h.in

Makefile: Makefile.in config.status Makefile.rules Makefile.top
	@if test "x$(NOAUTODEPS)" = "x"; then \
	    echo "running config.status because the following file(s) changed:"; \
	    echo "  $?"; \
	    ./config.status; \
	else \
	    echo "WARNING: not running config.status"; \
	fi

$(srcdir)/include/net-snmp/net-snmp-config.h.in: stamp-h.in
$(srcdir)/stamp-h.in: configure.in acconfig.h
	@if test "x$(NOAUTODEPS)" = "x" -a "x$(AUTOHEADER)" != "x:"; then \
	    cd ${srcdir} && LC_COLLATE=C $(AUTOHEADER); \
	    echo timestamp > ${srcdir}/stamp-h.in; \
	else \
	    echo "WARNING: not running autoheader"; \
	fi

include/net-snmp/net-snmp-config.h: stamp-h
stamp-h: include/net-snmp/net-snmp-config.h.in config.status
	@if test "x$(NOAUTODEPS)" = "x"; then \
	    echo "running config.status because the following file(s) changed:"; \
	    echo "  $?"; \
	    ./config.status; \
	    echo timestamp > stamp-h; \
	else \
	    echo "WARNING: not running config.status"; \
	fi

$(srcdir)/configure: configure.in aclocal.m4
	@if test "x$(NOAUTODEPS)" = "x" -a "x$(AUTOCONF)" != "x:"; then \
	    cd ${srcdir} && $(AUTOCONF); \
	    echo "Please run configure now."; \
	    sh -c exit 2; \
	else \
	    echo "WARNING: not running autoconf"; \
	fi

config.status: configure
	@if test "x$(NOAUTODEPS)" = "x"; then \
	    echo "running config.status because $? changed"; \
	    ./config.status --recheck; \
	else \
	    echo "WARNING: not running config.status --recheck"; \
	fi

#
# Emacs TAGS file
#
TAGS:
	find $(srcdir) -path $(srcdir)/dist/rpm -prune -o -name '*.[ch]' -print | etags -

#
# Internal distribution packaging, etc.
#
version:
	@if test "x$(VERSION)" = "x"; then \
	  echo "you need to supply a VERSION string."; \
	  exit 2; \
	fi
	perl local/Version-Munge.pl -T $(VERSION) -M -V -D -C

tag:
	@if test "x$(VERSION)" = "x"; then \
	  echo "you need to supply a VERSION string."; \
	  exit 2; \
	fi
	${srcdir}/agent/mibgroup/versiontag $(VERSION) tag

tar:
	@if test "x$(VERSION)" = "x"; then \
	  echo "you need to supply a VERSION string."; \
	  exit 2; \
	fi
	${srcdir}/agent/mibgroup/versiontag $(VERSION) tar

tarclean:
	@if test -x ${srcdir}/agent/mibgroup/versiontag ; then \
	  ${srcdir}/agent/mibgroup/versiontag Ext clean ; \
	fi

checks:
	$(MAKE) -k makefilecheck commentcheck warningcheck

# always exit 1, since I can't figure out how to invert grep rc
warningcheck:
	@echo "Checking for cpp warnings..."
	@grep -n -E "#warning" `find . -name \*.\[ch\]`
	@exit 1

# always exit 1, since I can't figure out how to invert grep rc
commentcheck:
	@echo "Checking for C++ style comments..."
	@grep -n -E "([^:)]|^)//" `find . -path './win32' -prune -o -name \*.\[ch\]`
	@exit 1

# always exit 1, since I can't figure out how to invert grep rc
makefilecheck:
	@echo "Checking for non-portable Makefile constructs..."
	@grep -n "\.c=" `find . -name "Makefile.*"` && exit 1
	@exit 1

dist: version tag tar

FAQ.html:
	local/FAQ2HTML FAQ

.PHONY: docs docsdir mancp testdirs test TAGS
# note: tags and docs are phony to force rebulding

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
