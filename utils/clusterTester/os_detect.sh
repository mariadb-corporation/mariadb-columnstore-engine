#!/bin/sh
#
detectOS () {
   checkFile1=/etc/os-release
   checkFile2=/etc/centos-release
   if [ -f "$checkFile1" ]
   then
      osPrettyName=`cat $checkFile1 | grep PRETTY_NAME|awk -F"=" '{print $2}'`
      osVersionID=`cat $checkFile1 | grep VERSION_ID | awk -F"=" '{print $2}' | awk -F"." '{print $1}' | sed 's/"//g'`
   else
      osPrettyName=`head -n 1 $checkFile2`
      osVersionID=`echo $osPrettyName | awk -F" " '{print $3}' | awk -F"." '{print $1}'`
   fi
#
   osName=`echo $osPrettyName | awk -F" " '{print $1}' | sed 's/"//g'`
   if [ -z "$osPrettyName" ]
   then
     osPrettyName=`uname -o -s -r -v`
   fi
   if [ -z "$osName" ] || [ -z "$osVersionID" ]
   then
      osTag=""
   else
         osTag=`echo $osName$osVersionID | awk '{print tolower($0)}'`
   fi
}
#
   detectOS
   echo Operating System name: $osPrettyName
   echo Operating System tag:  $osTag
   case "$osTag" in
      centos6|centos7|ubuntu16|debian8|suse12|debian9)
         ;;
      *)
         echo OS not supported
	 exit 1
         ;;
   esac
   
   exit 0
