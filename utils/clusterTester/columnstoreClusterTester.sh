#!/bin/bash

bold=$(tput bold)
normal=$(tput sgr0)

IPADDRESSES=""
OS=""
PASSWORD="ssh"
CHECK=true
REPORTPASS=true
LOGFILE=""

OS_LIST=("centos6" "centos7" "debian8" "debian9" "suse12" "ubuntu16")

NODE_IPADDRESS=""

if [ -z "$COLUMNSTORE_INSTALL_DIR" ]; then
	COLUMNSTORE_INSTALL_DIR=/usr/local/mariadb/columnstore
fi

checkContinue() {

  if [ "$CHECK" = false ]; then
    return 0
  fi
  
  echo ""
  read -p "Failure occurred, do you want to continue? (y,n) > " answer
  case ${answer:0:1} in
    y|Y )
      return 0
    ;;
    * ) 
      exit
    ;;
  esac
}

###
# Print Fucntions
###

helpPrint () {
          ################################################################################
    echo ""
    echo  "This is the MariaDB ColumnStore Cluster System Test Tool." 
    echo ""
    echo  "It will run a set of test to validate the setup of the MariaDB Columnstore system." 
    echo  "This can be run prior to the install of MariaDB ColumnStore to make sure the" 
    echo  "servers/nodes are configured properly. It should be run as the user of the planned"
    echo  "install. Meaning if MariaDB ColumnStore is going to be installed as root user,"
    echo  "then run from root user. Also the assumption is that the servers/node have be"
    echo  "setup based on the Preparing for ColumnStore Installation."
    echo  "It should also be run on the server that is designated as Performance Module #1."
    echo  "This is the same server where the MariaDB ColumnStore package would be installed"
    echo  " and where the install script would be executed from, postConfigure."
    echo ""
    echo "Additional information on Tool is documented at:"
    echo ""
    echo "https://mariadb.com/kb/en/mariadb/*****/"
    echo ""
    echo  "Items that are checked:" 
    echo  "	Node Ping test" 
    echo  "	Node SSH test"
    echo  "	ColumnStore Port test"
    echo  "	OS version" 
    echo  "	Locale settings" 
    echo  "	Firewall settings"
    echo  "     Date/time settings"
    echo  "	Dependent packages installed"
    echo  "     For non-root user install - test permissions on /tmp and /dev/shm"
    echo ""
    echo  "Usage: $0 [options]" 
    echo "OPTIONS:"
    echo  "   -h,--help			Help" 
    echo  "   --ipaddr=[ipaddresses]	Remote Node IP-Addresses/Hostnames, if not provide, will only check local node"
    echo  "                           	examples: 192.168.1.1,192.168.1.2 or serverum1,serverpm2"
    echo  "   --os=[os]			Optional: Set OS Version (centos6, centos7, debian8, debian9, suse12, ubuntu16)" 
    echo  "   --password=[password]	Provide a user password. (Default: ssh-keys setup will be assumed)" 
    echo  "   -c,--continue		Continue on failures"
    echo  "   --logfile=[fileName] 	Output results to a log file"
    echo ""
    echo  "NOTE: Dependent package : 'nmap' and 'expect' packages need to be installed locally" 
    echo ""
}

# Parse command line options.
while getopts hc-: OPT; do
    case "$OPT" in
        h)
            echo $USAGE
            helpPrint
            exit 0
            ;;
        c)
            CHECK=false
            ;;      
        -)  LONG_OPTARG="${OPTARG#*=}"
            ## Parsing hack for the long style of arguments.
            case $OPTARG in
                help )  
                    helpPrint
                    exit 0
                    ;;            
		continue )
		    CHECK=false
		    ;;      
                ipaddr=?* )  
                    IPADDRESSES="$LONG_OPTARG"
                    ;;
                os=?* )
                    OS="$LONG_OPTARG"
    		    match=false
                    for SUPPORTED_OS in "${OS_LIST[@]}"; do
                      if [ $SUPPORTED_OS == "$OS" ]; then
                        match=true
                        break;
                      fi
                    done

                    if [ $match == "false" ] ; then
		      echo ""
                      echo "OS version not-supported, please re-run and provide the OS from list of support OSs "
                      for SUPPORTED_OS in "${OS_LIST[@]}"; do
                        echo "$SUPPORTED_OS"
                      done
                      echo ""
                      exit 1
                    fi

                    ;;
                password=?* )
                    PASSWORD="$LONG_OPTARG"
                    ;;
                logfile=?* )
                    LOGFILE="$LONG_OPTARG"
                    exec 1<>$LOGFILE
                    exec 2>&1
                    ;;
                ipaddr* )  
                    echo "No arg for --$OPTARG option" >&2
                    exit 1
                    ;;
                os* )  
                    echo "No arg for --$OPTARG option" >&2
                    exit 1
                    ;;
                password* )  
                    echo "No arg for --$OPTARG option" >&2
                    exit 1
                    ;;
                continue* )
                    echo "No arg allowed for --$OPTARG option" >&2
                    exit 1 
                    ;;
                logfile* )  
                    echo "No arg for --$OPTARG option" >&2
                    exit 1
                    ;;
                 help* )  
                    helpPrint
                    exit 0
                    ;;                                        
                 '' )
                    break ;; # "--" terminates argument processing
                * )
                    echo "Illegal option --$OPTARG" >&2
                    exit 1
                    ;;
            esac 
            ;;       
        \?)
            # getopts issues an error message
            echo $USAGE >&2
            exit 1
            ;;
    esac
done

# Remove the switches we parsed above.
shift `expr $OPTIND - 1`

if [ "$IPADDRESSES" != "" ]; then
  #parse IP Addresses into an array
  IFS=','
  read -ra NODE_IPADDRESS <<< "$IPADDRESSES"
  
  if ! type expect > /dev/null 2>&1 ; then
      echo "expect is not installed. Please install and rerun."
      exit 1
  fi

  if ! type nmap > /dev/null 2>&1; then
      echo "nmap is not installed. Please install and rerun."
      exit 1
  fi
fi  

checkLocalOS()
{
  echo "** Validate local OS is supported"
  echo ""

  #get local OS
  `$COLUMNSTORE_INSTALL_DIR/bin/os_detect.sh > /tmp/os_detect 2>&1`
  if [ "$?" -eq 0 ]; then
    localOS=`cat /tmp/os_detect | grep "Operating System name" | cut -f2 -d '"'`
    echo "Local Node OS System Name : $localOS"

    if [ "$OS" != "" ] ; then
      echo ""
      echo "Local Node OS Versions doesn't match the command line OS argument"
      echo "Contining using the Detected Local Node OS Version"
      OS=`cat /tmp/os_detect | grep "Operating System tag" | cut -f4 -d " "`
    
      echo "Local Node OS Version : $OS"
    else
      OS=`cat /tmp/os_detect | grep "Operating System tag" | cut -f4 -d " "`
    fi
  else
    localOS=`cat /tmp/os_detect | grep "Operating System name" | cut -f2 -d '"'`
    echo "Local Node OS System Name : $localOS"

    if [ "$OS" == "" ] ; then
      echo ""
      echo "Operating System name doesn't match any of the supported OS's"
      echo ""
      if [ $LOGFILE != ""  ] ; then
        exit 1
      fi

      echo "Please select from this OS list or enter 'exit'"
      for SUPPORTED_OS in "${OS_LIST[@]}"; do
        echo "  $SUPPORTED_OS"
      done

      echo ""
      read -p "Enter OS or 'exit' > " answer
      if [ $answer == 'exit' ] ; then
        exit 1
      fi
      match=false
      for SUPPORTED_OS in "${OS_LIST[@]}"; do
        if [ $SUPPORTED_OS == $answer ] ; then
	  OS=$answer
          match=true
          break;
        fi
      done

      if [ $match == "false" ] ; then 
        echo "OS version unknown, please re-run and provide the OS in the command line --os"
        exit 1
      fi
    else
      echo "${bold}Warning${normal}: Local Node OS version detected is not supported and different than the enter OS Version"
    fi
  fi
}
 
checkLocalDir()
{
    if [ "$USER" != "root" ]; then
      # Non-root User directory permissions check
      #
      echo ""
      echo "** Run Non-root User directory permissions check on Local Node"
      echo ""
      
      #remove any check tmp files from previous runs
      `sudo rm -f /tmp/*_check > /dev/null 2>&1`
      
      #check /tmp and /dev/shm
      pass=true
      `touch /tmp/cs_check > /dev/null 2>&1`
      if [ "$?" -eq 0 ]; then
	echo "Local Node permission test on /tmp : Passed"
	`rm -f /tmp/cs_check`
      else
	echo "Local Node permission test on /tmp : ${bold}Failed${normal}, change permissions to 777 and re-test"
	exit 1
      fi

      `touch /dev/shm/cs_check > /dev/null 2>&1`
      if [ "$?" -eq 0 ]; then
	echo "Local Node permission test on /dev/shm : Passed"
	`rm -f /dev/shm/cs_check`
      else
	echo "Local Node permission test on /dev/shm : ${bold}Failed${normal}, change permissions to 777 and re-test"
	pass=false
	REPORTPASS=false
      fi
    fi
}

checkPing()
{
    # ping test
    #
    echo ""
    echo "** Run Ping access Test to remote nodes"
    echo ""

    for ipadd in "${NODE_IPADDRESS[@]}"; do

      `ping $ipadd -c 1 -w 5 > /dev/null 2>&1`
      if [ "$?" -eq 0 ]; then
	echo $ipadd " Node Passed ping test"
      else
	echo $ipadd " Node ${bold}Failed${normal} ping test, correct and retest"
	exit 1
      fi
    done
}

checkSSH()
{
  # Login test
  #
  echo ""
  echo "** Run SSH Login access Test to remote nodes"
  echo ""

  for ipadd in "${NODE_IPADDRESS[@]}"; do
    `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD ls 1 > /dev/null 2>&1`;
    rc="$?"
    if  [ $rc -eq 0 ] || ( [ $rc -eq 2 ] && [ $OS == "suse12" ] ) ; then
      if [ $PASSWORD == "ssh" ] ; then
	echo $ipadd " Node Passed SSH login test using ssh-keys"
      else
	echo $ipadd " Node Passed SSH login test using user password"
      fi
    else
      if [ $PASSWORD == "ssh" ] ; then
	echo $ipadd " Node ${bold}Failed${normal} SSH login test using ssh-keys"
      else
	echo $ipadd " Node ${bold}Failed${normal} SSH login test using user password"
      fi
      exit 1
    fi
  done
}

checkRemoteDir()
{
  #
  # remove old _check tmp files from remote servers
  
  `sudo rm -f /tmp/*_check > /dev/null 2>&1`
  
  for ipadd in "${NODE_IPADDRESS[@]}"; do
    `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD 'sudo rm -f /tmp/*_check > /dev/null 2>&1' 1 > /tmp/remote_command_check 2>&1`
  done

  if [ "$USER" != "root" ]; then
    # Non-root User directory permissions check
    #
    echo ""
    echo "** Run Non-root User directory permissions check on remote nodes"
    echo ""

    for ipadd in "${NODE_IPADDRESS[@]}"; do
      `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD 'touch /tmp/cs_check' 1 > /tmp/remote_command_check 2>&1`
      rc="$?"
      if  [ $rc -eq 0 ] || ( [ $rc -eq 2 ] && [ $OS == "suse12" ] ) ; then
	`grep "Permission denied" /tmp/remote_command_check  > /dev/null 2>&1`
	if [ "$?" -eq 0 ]; then
	  echo "$ipadd Node permission test on /tmp : ${bold}Failed${normal}, change permissions to 777 and re-test"
	  exit 1
	else
	  echo "$ipadd Node permission test on /tmp : Passed"
	fi
      else
	  echo "Error running remote_command.sh to $ipadd Node, check /tmp/remote_command_check"
	  pass=false
	  REPORTPASS=false
      fi

      `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD 'touch /dev/shm/cs_check' 1 > /tmp/remote_command_check 2>&1`
      rc="$?"
      if  [ $rc -eq 0 ] || ( [ $rc -eq 2 ] && [ $OS == "suse12" ] ) ; then
	`grep "Permission denied" /tmp/remote_command_check  > /dev/null 2>&1`
	if [ "$?" -eq 0 ]; then
	  echo "$ipadd Node permission test on /dev/shm : ${bold}Failed${normal}, change permissions to 777 and re-test"
	  pass=false
	  REPORTPASS=false
	else
	  echo "$ipadd Node permission test on /dev/shm : Passed"
	fi
      else
	echo "Error running remote_command.sh to $ipadd Node, check /tmp/remote_command_check"
	pass=false
	REPORTPASS=false
      fi
    done
    
    if ! $pass; then
      checkContinue
    fi
  fi
}

checkOS()
{
  # Os check
  #
  echo ""
  echo "** Run OS check - OS version needs to be the same on all nodes"
  echo ""

  echo "Local Node OS Version : $localOS"
  echo ""
  
  pass=true
  `/bin/cp -f $COLUMNSTORE_INSTALL_DIR/bin/os_detect.sh /tmp/.`
  for ipadd in "${NODE_IPADDRESS[@]}"; do
    `$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_put.sh $ipadd $PASSWORD /tmp/os_detect.sh 1 > /tmp/remote_scp_put_check 2>&1`
    if [ "$?" -ne 0 ]; then
      echo "Error running remote_scp_put.sh to $ipadd Node, check /tmp/remote_scp_put_check"
      exit 1
    else
      `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD /tmp/os_detect.sh 1 > /tmp/remote_command_check`
      rc="$?"
      if [ "$?" -ne 0 ]; then
	echo "Error running remote_command.sh /tmp/os_detect.sh on $ipadd Node, check /tmp/remote_command_check"
	exit 1
      else
	  remoteOS=`cat /tmp/remote_command_check | grep "Operating System name" | cut -f2 -d '"'`
	  echo "$ipadd Node OS Version : $remoteOS"
	  if [ $localOS != $remoteOS ]; then
	    echo "${bold}Failed${normal}, $ipadd has a different OS than local node"
	    pass=false
	    REPORTPASS=false
	  fi
      fi
    fi
  done

  if ! $pass; then
    checkContinue
  fi
}

checkLocale()
{
  # Locale check
  #
  echo ""
  echo "** Run Locale check - Locale needs to be the same on all nodes"
  echo ""
  
  #get local Locale
  `locale | grep LANG= > /tmp/locale_check 2>&1`
  if [ "$?" -eq 0 ]; then
    echo "Local Node Locale : `cat /tmp/locale_check`"
  else
    echo "Error running 'locale' command on local node"
  fi
  
  pass=true
  for ipadd in "${NODE_IPADDRESS[@]}"; do
    `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD 'locale | grep LANG= > /tmp/locale_check 2>&1' 1 > /tmp/remote_command_check`
    rc="$?"
     if  [ $rc -eq 0 ] || ( [ $rc -eq 2 ] && [ $OS == "suse12" ] ) ; then
      `$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/locale_check > /tmp/remote_scp_get_check 2>&1`
      if [ "$?" -ne 0 ]; then
	echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
	exit 1
      else
	echo "$ipadd Node Locale : `cat locale_check`"
	`diff /tmp/locale_check locale_check > /dev/null 2>&1`
	if [ "$?" -ne 0 ]; then
	  echo "${bold}Failed${normal}, $ipadd has a different Locale setting than local node"
	  pass=false
	  REPORTPASS=false
	fi
	`rm -f locale_check`
      fi
    else
      echo "Error running remote_command.sh to $ipadd Node, check /tmp/remote_command_check"
      exit 1
      pass=false
      REPORTPASS=false
    fi
  done
  
  if ! $pass; then
    checkContinue
  fi
}

checkSELINUX()
{
  # SELINUX check
  #
  echo ""
  echo "** Run SELINUX check"
  echo ""
  
  pass=true
  #check local SELINUX
  if [ -f /etc/selinux/config ]; then
    `cat /etc/selinux/config | grep SELINUX | grep enforcing > /tmp/selinux_check 2>&1`
    if [ "$?" -eq 0 ]; then
      echo "${bold}Warning${normal}, Local Node SELINUX setting is Enabled, check port test results"
      pass=false
    else
      echo "Local Node SELINUX setting is Not Enabled"
    fi
  else
      echo "Local Node SELINUX setting is Not Enabled"
  fi
  
  for ipadd in "${NODE_IPADDRESS[@]}"; do
    `$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /etc/selinux/config > /tmp/remote_scp_get_check 2>&1`
    if [ "$?" -ne 0 ]; then
      echo "$ipadd Node SELINUX setting is Not Enabled"
    else
     `cat config | grep SELINUX | grep enforcing > /tmp/selinux_check 2>&1`
    if [ "$?" -eq 0 ]; then
      echo "${bold}Warning${normal}, $ipadd SELINUX setting is Enabled, check port test results"
      pass=false
    else
      echo "$ipadd Node SELINUX setting is Not Enabled"
    fi
      `rm -f config`
    fi
  done
}

checkFirewalls()
{
  # FIREWALL checks
  #
  echo ""
  echo "** Run Firewall Services check"
  echo ""

  declare -a FIREWALL_LIST=("iptables" "ufw" "firewalld" "firewall")

  #check local FIREWALLS
  for firewall in "${FIREWALL_LIST[@]}"; do
    pass=true
    `service  $firewall status > /tmp/firewall1_check 2>&1`
    if [ "$?" -eq 0 ]; then
      echo "${bold}Warning${normal}, Local Node $firewall service is Active, check port test results"
      pass=false
    else
      `systemctl status $firewall > /tmp/firewall1_check 2>&1`
      if [ "$?" -eq 0 ]; then
        echo "${bold}Warning${normal}, Local Node $firewall service is Active, check port test results"
        pass=false
      fi
    fi

    if $pass ; then
      echo "Local Node $firewall service is Not Active"
    fi
  done

  echo ""
  fpass=true
  for ipadd in "${NODE_IPADDRESS[@]}"; do
      # 'sysconfig not on remote node
      for firewall in "${FIREWALL_LIST[@]}"; do
	pass=true
        `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "service '$firewall' status > /tmp/firewall_check 2>&1" 1 > /tmp/remote_command_check`
        if [ "$?" -eq 0 ]; then
              echo "${bold}Warning${normal}, $ipadd Node $firewall service is Active, check port test results"
              pass=false
        else
	  `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "systemctl status '$firewall' > /tmp/firewall_check 2>&1" 1 > /tmp/remote_command_check`
	  if [ "$?" -eq 0 ]; then
	      echo "${bold}Warning${normal}, $ipadd Node $firewall service is Active, check port test results"
	      pass=false
	  fi
        fi  

	if $pass ; then
	  echo "$ipadd Node $firewall service is Not Enabled"
	fi
      done

  echo ""
  done
  
  if [ $OS == "suse12" ]; then
    # rcSuSEfirewall2 check
    #
    echo ""
    echo "** Run rcSuSEfirewall2 check"
    echo ""
    
    pass=true
    #check local IPTABLES
    `/sbin/rcSuSEfirewall2 status > /tmp/rcSuSEfirewall2_check 2>&1`
    if [ "$?" -eq 0 ]; then
      echo "${bold}Failed${normal}, Local Node rcSuSEfirewall2 service is Enabled, check port test results"
      pass=false
    else
      echo "Local Node rcSuSEfirewall2 service is Not Enabled"
    fi

    for ipadd in "${NODE_IPADDRESS[@]}"; do
      `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD '/sbin/rcSuSEfirewall2 status > /tmp/rcSuSEfirewall2_check 2>&1' 1 > /tmp/remote_command_check`
      rc="$?"
      if  [ $rc -eq 0 ] ; then
	echo "${bold}Failed${normal}, $ipadd Node rcSuSEfirewall2 service is Enabled, check port test results"
	pass=false
      else
       echo "$ipadd Node rcSuSEfirewall2 service is Not Enabled"
      fi
    done
  fi
}

checkPorts()
{
  # port test
  #
  echo ""
  echo "** Run MariaDB ColumnStore Port (8600-8630,8700,8800,3306) availability test"
  echo ""

  pass=true
  for ipadd in "${NODE_IPADDRESS[@]}"; do

    `sudo nmap $ipadd -p 8600-8630,8700,8800,3306 | grep 'filtered' > /tmp/port_test`
    if [ "$?" -ne 0 ]; then
      echo $ipadd " Node Passed port test"
    else
      echo $ipadd " Node ${bold}Failed${normal} port test, check and disable any firewalls or open ports in firewall"
      cat /tmp/port_test
      pass=false
      REPORTPASS=false
    fi
  done

  if ! $pass; then
    checkContinue
  fi
}

checkTime()
{
  # Time check
  #
  echo ""
  echo "** Run Date/Time check - Date/Time should be within 10 seconds on all nodes"
  echo ""
  
  pass=true
  #get local epoch time
  localTime=`date +%s`
  for ipadd in "${NODE_IPADDRESS[@]}"; do
     `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD 'date +%s > /tmp/time_check' > /tmp/time_check`
      rc="$?"
      if  [ $rc -ne 0 ] ; then
	  echo $ipadd " Node ${bold}Failed${normal} date/time check failed, check /tmp/time_check"
	  pass=false
	  REPORTPASS=false
      else
	`$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/time_check > /tmp/remote_scp_get_check 2>&1`
	if [ "$?" -ne 0 ]; then
	  echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
	else
	  remoteTime=`cat time_check`
	  timeDiff=`echo "$(($remoteTime-$localTime))"`
	  range=10
	  if [ $timeDiff -gt $range ] || [ $timeDiff -lt -$range ] ; then
	    echo $ipadd " Node ${bold}Failed${normal}, $ipadd Node date/time is more than 10 seconds away from local node"
	    pass=false
	  else
	    echo "Passed: $ipadd Node date/time is within 10 seconds of local node"
	    fi
	fi  
      fi
  done
  `rm -f time_check`

  if ! $pass; then
    checkContinue
  fi
}

checkMysqlPassword()
{
  # Locale check
  #
  echo ""
  echo "** Run MariaDB Console Password check"
  echo ""
  
  #get MariaDB password
  pass=true
  `$COLUMNSTORE_INSTALL_DIR/mysql/mysql-Columnstore start > /dev/null 2>&1`
  `$COLUMNSTORE_INSTALL_DIR/bin/mariadb-command-line.sh > /dev/null 2>&1`
  if [ "$?" -eq 2 ]; then
      echo "${bold}Failed${normal}, Local Node MariaDB login failed with missing password file, /root/.my.cnf"
  fi
  
  if [ "$IPADDRESSES" != "" ]; then
    `/bin/cp -f $COLUMNSTORE_INSTALL_DIR/bin/mariadb-command-line.sh /tmp/.`

    for ipadd in "${NODE_IPADDRESS[@]}"; do
      `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD $COLUMNSTORE_INSTALL_DIR/mysql/mysql-Columnstore start > /dev/null 2>&1`
      `$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_put.sh $ipadd $PASSWORD /tmp/mariadb-command-line.sh 1 > /tmp/remote_scp_put_check 2>&1`
      if [ "$?" -ne 0 ]; then
	echo "Error running remote_scp_put.sh to $ipadd Node, check /tmp/remote_scp_put_check"
	exit 1
      else
	`$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD /tmp/mariadb-command-line.sh 1 > /tmp/remote_command_check`
	    `cat /tmp/remote_command_check | grep "ERROR - PASSWORD" > /dev/null 2>&1`
	    if [ "$?" -eq 0 ]; then
	      echo "${bold}Failed${normal}, $ipadd Node MariaDB login failed with missing password file, /root/.my.cnf"
	      pass=false
	    fi
      fi 
    done
  fi
  
  if ! $pass; then
    checkContinue
  else
    echo "Passed, no problems detected with a MariaDB password being set without an associated /root/.my.cnf"
  fi
}

checkPackages()
{
  #
  # now check packaging on local and remote nodes
  #

  echo ""
  echo "** Run MariaDB ColumnStore Dependent Package Check"
  echo ""

  declare -a CENTOS_PKG=("expect" "perl" "perl-DBI" "openssl" "zlib" "file" "sudo" "libaio" "rsync" "snappy" "net-tools" "numactl-libs")
  declare -a CENTOS_PKG_NOT=("mariadb-libs")

  if [ "$OS" == "centos6" ] || [ "$OS" == "centos7" ]; then
    if [ ! `which yum 2>/dev/null` ] ; then
      echo "${bold}Failed${normal}, Local Node ${bold}yum${normal} package not installed"
      pass=false
      REPORTPASS=false
    else
      pass=true
      #check centos packages on local node
      for PKG in "${CENTOS_PKG[@]}"; do
	if [ $OS == "centos6" ] && [ "$PKG" == "boost" ]; then
	  `ls /usr/lib/libboost_regex.so > /dev/null 2>&1`
	  if [ "$?" -ne 0 ]; then
	    echo "${bold}Failed${normal}, Local Node ${bold}boost libraries${normal} not installed"
	    pass=false
	    REPORTPASS=false
	  fi
	else
	  `yum list installed "$PKG" > /tmp/pkg_check 2>&1`
	  `cat /tmp/pkg_check | grep Installed > /dev/null 2>&1`
	  if [ "$?" -ne 0 ]; then
	    echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is not installed, please install"
	    pass=false
	    REPORTPASS=false
	  fi
	fi
      done
    fi

    if [ $pass == true ] ; then
      echo "Local Node - Passed, all dependency packages are installed"
    else
      checkContinue
    fi

    #check for package that shouldnt be installed
    pass=true
    for PKG in "${CENTOS_PKG_NOT[@]}"; do
      `yum list installed "$PKG" > /tmp/pkg_check 2>&1`
      `cat /tmp/pkg_check | grep Installed > /dev/null 2>&1`
      if [ "$?" -eq 0 ]; then
	echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is installed, please un-install"
	pass=false
	REPORTPASS=false
      fi
    done

    if [ $pass == true ] ; then
      echo "Local Node - Passed, all packages that should not be installed aren't installed"
    else
      checkContinue
    fi
    
    echo ""
    pass=true
    if [ "$IPADDRESSES" != "" ]; then
      for ipadd in "${NODE_IPADDRESS[@]}"; do
	for PKG in "${CENTOS_PKG[@]}"; do
	  if [ $OS == "centos6" ] && [ $PKG == "boost" ]; then
	    `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD 'ls /usr/lib/libboost_regex.so > /dev/null 2>&1' 1 > /tmp/remote_command_check 2>&1`
	    if  [ $? -ne 0 ] ; then
	      echo "${bold}Failed${normal}, $ipadd Node ${bold}boost libraries${normal} not installed"
	      pass=false
	      REPORTPASS=false
	    fi
	  else
	    `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "yum list installed '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
	    rc="$?"
	    if  [ $rc -eq 2 ] ; then
              echo "${bold}Failed${normal}, $ipadd Node, 'yum' not installed"
              pass=false
              REPORTPASS=false
              break
	    elif [ $rc -eq 1 ] ; then
		echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is not installed, please install"
		pass=false
		REPORTPASS=false
	    fi
	  fi
	done
	
	if $pass; then
	  echo "$ipadd Node - Passed, all dependency packages are installed"
	else
	  checkContinue
	  pass=true
	fi
	echo ""
	
	#check for package that shouldnt be installed
	for PKG in "${CENTOS_PKG_NOT[@]}"; do
	  `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "yum list installed '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
	  rc="$?"
	  if  [ $rc -eq 2 ] ; then
	    echo "${bold}Failed${normal}, $ipadd Node, 'yum' not installed"
	    pass=false
	    REPORTPASS=false
	    break
	  elif [ $rc -ne 1 ] ; then
	      echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is installed, please un-install"
	      pass=false
	      REPORTPASS=false
	  fi
	done
	
	if $pass; then
	  echo "$ipadd Node - Passed, all packages that should not be installed aren't installed"
	else
	  checkContinue
	  pass=true
	fi
	echo ""

      done
    fi
  fi

  declare -a SUSE_PKG=("boost-devel" "expect" "perl" "perl-DBI" "openssl" "file" "sudo" "libaio1" "rsync" "libsnappy1" "net-tools" "libnuma1")
  declare -a SUSE_PKG_NOT=("mariadb" , "libmariadb18")

  if [ "$OS" == "suse12" ]; then
    if [ ! `which rpm 2>/dev/null` ] ; then
      echo "${bold}Failed${normal}, Local Node ${bold}rpm${normal} package not installed"
      pass=false
      REPORTPASS=false
    else
      pass=true
      #check centos packages on local node
      for PKG in "${SUSE_PKG[@]}"; do
	`rpm -qi "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep "not installed" > /dev/null 2>&1`
	if [ "$?" -eq 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is not installed, please install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all dependency packages are installed"
      else
	checkContinue
      fi

      #check for package that shouldnt be installed
      pass=true
      for PKG in "${SUSE_PKG_NOT[@]}"; do
	`rpm -qi "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep "not installed" > /dev/null 2>&1`
	if [ "$?" -ne 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is installed, please un-install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all packages that should not be installed aren't installed"
      else
	checkContinue
      fi
    fi
    
    echo ""
    pass=true
    if [ "$IPADDRESSES" != "" ]; then
      for ipadd in "${NODE_IPADDRESS[@]}"; do
	for PKG in "${SUSE_PKG[@]}"; do
	  `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "rpm -qi '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
	  rc="$?"
	  if  [ $rc -ne 0 ] ; then
	    echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is not installed, please install"
	    pass=false
	    REPORTPASS=false
	  fi
	done
	
	if $pass; then
	  echo "$ipadd Node - Passed, all dependency packages are installed"
	else
	  checkContinue
          pass=true
	fi
	echo ""
	
	#check for package that shouldnt be installed
	for PKG in "${SUSE_PKG_NOT[@]}"; do
	  `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "rpm -qi '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
	  rc="$?"
	  if  [ $rc -eq 0 ] ; then
	    echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is installed, please un-install"
	    pass=false
	    REPORTPASS=false
	  fi
	done
	
	if $pass; then
	  echo "$ipadd Node - Passed, all packages that should not be installed aren't installed"
	else
	  checkContinue
          pass=true
	fi
	echo ""

      done
    fi
  fi  

  declare -a UBUNTU_PKG=("libboost-all-dev" "expect" "libdbi-perl" "perl" "openssl" "file" "sudo" "libreadline-dev" "rsync" "libsnappy1V5" "net-tools" "libnuma1" )
  declare -a UBUNTU_PKG_NOT=("mariadb-server" "libmariadb18")

  if [ "$OS" == "ubuntu16" ] ; then
    if [ ! `which dpkg 2>/dev/null` ] ; then
      echo "${bold}Failed${normal}, Local Node ${bold}rpm${normal} package not installed"
      pass=false
      REPORTPASS=false
    else
      pass=true
      #check centos packages on local node
      for PKG in "${UBUNTU_PKG[@]}"; do
	`dpkg -s "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	if [ "$?" -ne 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is not installed, please install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all dependency packages are installed"
      else
	checkContinue
      fi
      
      #check for package that shouldnt be installed
      pass=true
      for PKG in "${UBUNTU_PKG_NOT[@]}"; do
	`dpkg -s "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	if [ "$?" -eq 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is installed, please un-install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all packages that should not be installed aren't installed"
      else
	checkContinue
      fi
    fi
    
    echo ""
    pass=true
    if [ "$IPADDRESSES" != "" ]; then
     for ipadd in "${NODE_IPADDRESS[@]}"; do
      for PKG in "${UBUNTU_PKG[@]}"; do
        `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "dpkg -s '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
          `$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/pkg_check > /tmp/remote_scp_get_check 2>&1`
            if [ "$?" -ne 0 ]; then
              echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
            else
              `cat /tmp/remote_command_check | grep 'command not found' > /dev/null 2>&1`
              if [ "$?" -eq 0 ]; then
                echo "${bold}Failed${normal}, $ipadd Node ${bold}dpkg${normal} package not installed"
                pass=false
                break
              else
                `cat pkg_check | grep 'install ok installed' > /dev/null 2>&1`
                if [ "$?" -ne 0 ]; then
                    echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is not installed, please install"
                    pass=false
                fi

                `rm -f pkg_check`
              fi
	    fi
      done

      if $pass; then
        echo "$ipadd Node - Passed, all dependency packages are installed"
      else
        checkContinue
   	pass=true
      fi
      echo ""
      
      #check for package that shouldnt be installed
      for PKG in "${UBUNTU_PKG_NOT[@]}"; do
        `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "dpkg -s '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
	`$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/pkg_check > /tmp/remote_scp_get_check 2>&1`
	  if [ "$?" -ne 0 ]; then
	    echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
	  else
	    `cat /tmp/remote_command_check | grep 'command not found' > /dev/null 2>&1`
	    if [ "$?" -eq 0 ]; then
	      echo "${bold}Failed${normal}, $ipadd Node ${bold}dpkg${normal} package not installed"
	      pass=false
	      break
	    else
	      `cat pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	      if [ "$?" -eq 0 ]; then
		  echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is installed, please un-install"
		  pass=false
	      fi

	      `rm -f pkg_check`
	    fi
	  fi
      done

      if $pass; then
        echo "$ipadd Node - Passed, all packages that should not be installed aren't installed"
      else
        checkContinue
   	pass=true
      fi
      echo ""

    done
   fi
  fi

  declare -a DEBIAN_PKG=("libboost-all-dev" "expect" "libdbi-perl" "perl" "openssl" "file" "sudo" "libreadline-dev" "rsync" "libsnappy1" "net-tools" "libnuma1")
  declare -a DEBIAN_PKG_NOT=("libmariadb18" "mariadb-server")

  if [ "$OS" == "debian8" ]; then
    if [ ! `which dpkg 2>/dev/null` ] ; then
      echo "${bold}Failed${normal}, Local Node ${bold}rpm${normal} package not installed"
      pass=false
      REPORTPASS=false
    else    
      pass=true
      #check centos packages on local node
      for PKG in "${DEBIAN_PKG[@]}"; do
	`dpkg -s "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	if [ "$?" -ne 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is not installed, please install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all dependency packages are installed"
      else
	checkContinue
      fi
      
      #check for package that shouldnt be installed
      pass=true
      for PKG in "${DEBIAN_PKG_NOT[@]}"; do
	`dpkg -s "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	if [ "$?" -eq 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is installed, please un-install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all packages that should not be installed aren't installed"
      else
	checkContinue
      fi
    fi
    
    echo ""
    pass=true
    if [ "$IPADDRESSES" != "" ]; then
     for ipadd in "${NODE_IPADDRESS[@]}"; do
      for PKG in "${DEBIAN_PKG[@]}"; do
        `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "dpkg -s '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
          `$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/pkg_check > /tmp/remote_scp_get_check 2>&1`
            if [ "$?" -ne 0 ]; then
              echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
            else
              `cat /tmp/remote_command_check | grep 'command not found' > /dev/null 2>&1`
              if [ "$?" -eq 0 ]; then
                echo "${bold}Failed${normal}, $ipadd Node ${bold}dpkg${normal} package not installed"
                pass=false
                break
              else
                `cat pkg_check | grep 'install ok installed' > /dev/null 2>&1`
                if [ "$?" -ne 0 ]; then
                    echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is not installed, please install"
                    pass=false
                fi

                `rm -f pkg_check`
              fi
            fi
      done

      if $pass; then
        echo "$ipadd Node - Passed, all dependency packages are installed"
      else
        checkContinue
        pass=true
      fi
      echo ""
      
      #check for package that shouldnt be installed
      for PKG in "${DEBIAN_PKG_NOT[@]}"; do
        `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "dpkg -s '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
	`$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/pkg_check > /tmp/remote_scp_get_check 2>&1`
	  if [ "$?" -ne 0 ]; then
	    echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
	  else
	    `cat /tmp/remote_command_check | grep 'command not found' > /dev/null 2>&1`
	    if [ "$?" -eq 0 ]; then
	      echo "${bold}Failed${normal}, $ipadd Node ${bold}dpkg${normal} package not installed"
	      pass=false
	      break
	    else
	      `cat pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	      if [ "$?" -eq 0 ]; then
		  echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is installed, please un-install"
		  pass=false
	      fi

	      `rm -f pkg_check`
	    fi
	  fi
      done

      if $pass; then
        echo "$ipadd Node - Passed, all packages that should not be installed aren't installed"
      else
        checkContinue
   	pass=true
      fi
      echo ""

     done
    fi
  fi
  
  declare -a DEBIAN9_PKG=("libboost-all-dev" "expect" "libdbi-perl" "perl" "openssl" "file" "sudo" "libreadline5" "rsync" "libsnappy1V5" "net-tools" "libaio1" "libnuma1")
  declare -a DEBIAN9_PKG_NOT=("libmariadb18" "mariadb-server")

  if [ "$OS" == "debian9" ]; then
    if [ ! `which dpkg 2>/dev/null` ] ; then
      echo "${bold}Failed${normal}, Local Node ${bold}rpm${normal} package not installed"
      pass=false
      REPORTPASS=false
    else    
      pass=true
      #check centos packages on local node
      for PKG in "${DEBIAN9_PKG[@]}"; do
	`dpkg -s "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	if [ "$?" -ne 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is not installed, please install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all dependency packages are installed"
      else
	checkContinue
      fi
      
      #check for package that shouldnt be installed
      pass=true
      for PKG in "${DEBIAN9_PKG_NOT[@]}"; do
	`dpkg -s "$PKG" > /tmp/pkg_check 2>&1`
	`cat /tmp/pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	if [ "$?" -eq 0 ]; then
	  echo "${bold}Failed${normal}, Local Node package ${bold}${PKG}${normal} is installed, please un-install"
	  pass=false
	  REPORTPASS=false
	fi
      done

      if $pass; then
	echo "Local Node - Passed, all packages that should not be installed aren't installed"
      else
	checkContinue
      fi

    fi
    
    echo ""
    pass=true
    if [ "$IPADDRESSES" != "" ]; then
     for ipadd in "${NODE_IPADDRESS[@]}"; do
      for PKG in "${DEBIAN9_PKG[@]}"; do
        `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "dpkg -s '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
          `$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/pkg_check > /tmp/remote_scp_get_check 2>&1`
            if [ "$?" -ne 0 ]; then
              echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
            else
              `cat /tmp/remote_command_check | grep 'command not found' > /dev/null 2>&1`
              if [ "$?" -eq 0 ]; then
                echo "${bold}Failed${normal}, $ipadd Node ${bold}dpkg${normal} package not installed"
                pass=false
                break
              else
                `cat pkg_check | grep 'install ok installed' > /dev/null 2>&1`
                if [ "$?" -ne 0 ]; then
                    echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is not installed, please install"
                    pass=false
                fi

                `rm -f pkg_check`
              fi
            fi
      done

      if $pass; then
        echo "$ipadd Node - Passed, all dependency packages are installed"
      else
        checkContinue
        pass=true
      fi
      echo ""
      
      #check for package that shouldnt be installed
      for PKG in "${DEBIAN9_PKG_NOT[@]}"; do
        `$COLUMNSTORE_INSTALL_DIR/bin/remote_command.sh $ipadd $PASSWORD "dpkg -s '$PKG' > /tmp/pkg_check 2>&1" 1 > /tmp/remote_command_check 2>&1`
	`$COLUMNSTORE_INSTALL_DIR/bin/remote_scp_get.sh $ipadd $PASSWORD /tmp/pkg_check > /tmp/remote_scp_get_check 2>&1`
	  if [ "$?" -ne 0 ]; then
	    echo "Error running remote_scp_get.sh to $ipadd Node, check /tmp/remote_scp_get_check"
	  else
	    `cat /tmp/remote_command_check | grep 'command not found' > /dev/null 2>&1`
	    if [ "$?" -eq 0 ]; then
	      echo "${bold}Failed${normal}, $ipadd Node ${bold}dpkg${normal} package not installed"
	      pass=false
	      break
	    else
	      `cat pkg_check | grep 'install ok installed' > /dev/null 2>&1`
	      if [ "$?" -eq 0 ]; then
		  echo "${bold}Failed${normal}, $ipadd Node package ${bold}${PKG}${normal} is installed, please un-install"
		  pass=false
	      fi

	      `rm -f pkg_check`
	    fi
	  fi
      done

      if $pass; then
        echo "$ipadd Node - Passed, all packages that should not be installed aren't installed"
      else
        checkContinue
   	pass=true
      fi
      echo ""

     done
    fi
  fi
  

}

echo ""  
echo "*** This is the MariaDB Columnstore Cluster System Test Tool ***"
echo ""

checkLocalOS
checkLocalDir
if [ "$IPADDRESSES" != "" ]; then
  checkPing
  checkSSH
  checkRemoteDir
  checkOS
  checkLocale
  checkSELINUX
  checkFirewalls
  checkPorts
  checkTime
fi

checkMysqlPassword
checkPackages

if [ $REPORTPASS == true ] ; then
  echo ""
  echo "*** Finished Validation of the Cluster, all Tests Passed ***"
  echo ""
  exit 0
else
  echo ""
  echo "*** Finished Validation of the Cluster, Failures occurred. Check for Error/Failed test results ***"
  echo ""
  exit 1
fi

