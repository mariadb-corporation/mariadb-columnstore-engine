#! /bin/sh
# IDBInstanceCmds.sh
# get-local-instance-ID, get-zone, getPrivateIP from a Cloud environment
#
# 1. Amazon EC2

prefix=/usr/local

#check command
if [ "$1" = "" ]; then
	echo "Enter Command Name: {launchInstance|getInstance|getZone|getPrivateIP|getKey|getAMI|getType|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup|getSubnet}
}"
	exit 1
fi

if [ "$1" = "getPrivateIP" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "launchInstance" ]; then
	if [ "$2" = "" ]; then
		IPaddress="unassigned"
	else
		IPaddress="$2"
	fi
	if [ "$3" = "" ]; then
		instanceType="unassigned"
	else
		instanceType="$3"
	fi
	if [ "$4" = "" ]; then
		group="unassigned"
	else
		group="$4"
	fi
fi

if [ "$1" = "terminateInstance" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "stopInstance" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "startInstance" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	fi
	instanceName="$2"
fi

if [ "$1" = "assignElasticIP" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Instance Name"
		exit 1
	else
		instanceName="$2"
	fi
	if [ "$3" = "" ]; then
		echo "Enter Elastic IP Address"
		exit 1
	else
		IPAddress="$3"
	fi
fi

if [ "$1" = "deassignElasticIP" ]; then
	if [ "$2" = "" ]; then
		echo "Enter Elastic IP Address"
		exit 1
	else
		IPAddress="$2"
	fi
fi


test -f /usr/local/mariadb/columnstore/post/functions && . /usr/local/mariadb/columnstore/post/functions

ec2=`$prefix/mariadb/columnstore/bin/getConfig Installation EC2_HOME`

if [ $ec2 == "unassigned" ]; then
	if [ "$1" = "getPrivateIP" ]; then
		echo "stopped"
		exit 1
	else
       echo "unknown"
        exit 1
	fi
fi

java=`$prefix/mariadb/columnstore/bin/getConfig Installation JAVA_HOME`
path=`$prefix/mariadb/columnstore/bin/getConfig Installation EC2_PATH`

export PATH=$path
export EC2_HOME=$ec2
export JAVA_HOME=$java

# get Keys and region
AmazonAccessKeyFile=`$prefix/mariadb/columnstore/bin/getConfig Installation AmazonAccessKey`
if [ $AmazonAccessKeyFile == "unassigned" ]; then
	echo "FAILED: missing Config Setting AmazonAccessKey : $AmazonAccessKeyfile"
	exit 1
fi

AmazonSecretKeyFile=`$prefix/mariadb/columnstore/bin/getConfig Installation AmazonSecretKey`
if [ $AmazonSecretKeyFile == "unassigned" ]; then
	echo "FAILED: missing Config Setting AmazonSecretKeyFile : $AmazonSecretKeyFile"
	exit 1
fi

AmazonAccessKey=`cat $AmazonAccessKeyFile`
AmazonSecretKey=`cat $AmazonSecretKeyFile`

Region=`$prefix/mariadb/columnstore/bin/getConfig Installation AmazonRegion`
subnet=`$prefix/mariadb/columnstore/bin/getConfig Installation AmazonSubNetID`

if test ! -f $AmazonAccessKeyfile ; then
	echo "FAILED: missing AmazonAccessKeyfile : $AmazonAccessKeyfile"
	exit 1
fi

if test ! -f $AmazonSecretKeyfile ; then
	echo "FAILED: missing AmazonSecretKeyfile : $AmazonSecretKeyfile"
	exit 1
fi

#default instance to null
instance=""

describeInstanceFile="/tmp/describeInstance.txt"
touch $describeInstanceFile

describeInstance() {
	ec2-describe-instances -O $AmazonAccessKey -W $AmazonSecretKey --region $Region > $describeInstanceFile 2>&1
}

#call at start up
describeInstance

getInstance() {
	if [ "$instance" != "" ]; then
		echo $instance
		return
	fi
	
	# first get local IP Address
	localIP=`ifconfig eth0 | grep "inet addr:" | awk '{print substr($2,6,20)}'`

	#get local Instance ID
	instance=`cat $describeInstanceFile | grep -m 1 -w $localIP |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
	if [ "$instance" == "" ]; then
		describeInstance
	fi
	instance=`cat $describeInstanceFile | grep -m 1 -w $localIP |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`

	echo $instance
	return
}

getInstancePrivate() {
	if [ "$instance" != "" ]; then
		echo $instance
		return
	fi
	
	# first get local IP Address
	localIP=`ifconfig eth0 | grep "inet addr:" | awk '{print substr($2,6,20)}'`

	#get local Instance ID
	instance=`cat $describeInstanceFile | grep -m 1 -w $localIP |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
	if [ "$instance" == "" ]; then
		describeInstance
	fi
	instance=`cat $describeInstanceFile | grep -m 1 -w $localIP |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`

	return
}


getZone() {
	#get from Columnstore.xml if it's there, if not, get from instance then store
	zone=`$prefix/mariadb/columnstore/bin/getConfig Installation AmazonZone`

	if [ "$zone" = "unassigned" ] || [ "$zone" = "" ]; then
		#get local Instance ID
		getInstancePrivate >/dev/null 2>&1
		#get zone
		if [ "$subnet" == "unassigned" ]; then
			zone=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $11}'`
			if [ "$zone" == "" ]; then
				describeInstance
			fi
			zone=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $11}'`

		else
			zone=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $11}'`
			if [ "$zone" == "" ]; then
				describeInstance
			fi
			zone=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $11}'`
		fi
		$prefix/mariadb/columnstore/bin/setConfig Installation AmazonZone $zone
	fi

	echo $zone
	return
}

getPrivateIP() {
	#get instance info
	grep -B1 -A4 -m 1 $instanceName $describeInstanceFile > /tmp/instanceInfo_$instanceName 2>&1
	if [ `cat /tmp/instanceInfo_$instanceName | wc -c` -eq 0 ]; then
		describeInstance
	fi
	grep -B1 -A4 -m 1 $instanceName $describeInstanceFile > /tmp/instanceInfo_$instanceName 2>&1

	#check if running or terminated
	cat /tmp/instanceInfo_$instanceName | grep running > /tmp/instanceStatus_$instanceName
	if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -eq 0 ]; then
		# not running
		cat /tmp/instanceInfo_$instanceName | grep pending > /tmp/instanceStatus_$instanceName
		if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -ne 0 ]; then
			describeInstance
			echo "stopped"
			exit 1
		else
			cat /tmp/instanceInfo_$instanceName | grep terminated > /tmp/instanceStatus_$instanceName
			if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -ne 0 ]; then
				echo "terminated"
				exit 1
			else
				cat /tmp/instanceInfo_$instanceName | grep shutting-down > /tmp/instanceStatus_$instanceName
				if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -ne 0 ]; then
					echo "terminated"
					exit 1
				else
					echo "stopped"
					exit 1
				fi
			fi
		fi
	fi
	
	#running, get priviate IP Address
	if [ "$subnet" == "unassigned" ]; then
		IpAddr=`head -n 2 /tmp/instanceInfo_$instanceName | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $15}'`
	else
		IpAddr=`head -n 2 /tmp/instanceInfo_$instanceName | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $14}'`
	fi

	echo $IpAddr
	exit 0
}

getType() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get Type
	if [ "$subnet" == "unassigned" ]; then
		instanceType=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $9}'`
		if [ "$instanceType" == "" ]; then
			describeInstance
		fi
		instanceType=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $9}'`

	else
		instanceType=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $9}'`
		if [ "$instanceType" == "" ]; then
			describeInstance
		fi
		instanceType=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $9}'`

	fi

	echo $instanceType
	return
}

getKey() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get Key
	if [ "$subnet" == "unassigned" ]; then
		key=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
		if [ "$key" == "" ]; then
			describeInstance
		fi
		key=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`

	else
		key=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
		if [ "$key" == "" ]; then
			describeInstance
		fi
		key=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`

	fi

	echo $key
	return
}

getAMI() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get AMI
	ami=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $3}'`
	if [ "$ami" == "" ]; then
		describeInstance
	fi
	ami=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $3}'`

	echo $ami
	return
}

getGroup() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get group 
	if [ "$subnet" == "unassigned" ]; then
		group=`grep -B1 -A4 -m 1 $instance $describeInstanceFile |  grep -m 1 RESERVATION | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $4}'`
		if [ "$group" == "" ]; then
			describeInstance
		fi
		group=`grep -B1 -A4 -m 1 $instance $describeInstanceFile |  grep -m 1 RESERVATION | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $4}'`
		if [ "$group" == "" ]; then
			group=`grep -B1 -A4 -m 1 $instance $describeInstanceFile |  grep -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $22}'`
		fi
	else
		group=`grep -B1 -A6 -m 1 $instance $describeInstanceFile |  grep -m 1 GROUP | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
		if [ "$group" == "" ]; then
			describeInstance
		fi
		group=`grep -B1 -A6 -m 1 $instance $describeInstanceFile |  grep -m 1 GROUP | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
		if [ "$group" == "" ]; then
			group=`grep -B1 -A4 -m 1 $instance $describeInstanceFile |  grep -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $21}'`
		fi
	fi

	echo $group
	return
}

getProfile() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get Type
	if [ "$subnet" == "unassigned" ]; then
		instanceProfile=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $23}'`
		if [ "$instanceProfile" == "" ]; then
			describeInstance
		fi
		instanceProfile=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $23}'`

	else
		instanceProfile=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $22}'`
		if [ "$instanceProfile" == "" ]; then
			describeInstance
		fi
		instanceProfile=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $22}'`

	fi

	echo $instanceProfile
	return
}

launchInstance() {
	#get publickey
	getKey >/dev/null 2>&1
	if [ "$group" = "unassigned" ]; then
		#get group
		getGroup >/dev/null 2>&1
	fi
	#get AMI
	getAMI >/dev/null 2>&1
	#get Zone
	getZone >/dev/null 2>&1
	if [ "$instanceType" = "unassigned" ]; then
		#get type
		getType >/dev/null 2>&1
	fi
	#get AMI Profile
	getProfile >/dev/null 2>&1

	if [ "$subnet" == "unassigned" ]; then
		#NOT VPC
		if [ "$instanceProfile" = "" ] || [ "$instanceProfile" = "default" ]; then
			newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -g $group -t $instanceType -z $zone --region $Region $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
		else
			newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -g $group -t $instanceType -z $zone -p $instanceProfile --region $Region $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
		fi
	else	# VPC
		if [ "$instanceProfile" = "" ] || [ "$instanceProfile" = "default" ]; then
			if [ "$group" != "default" ]; then
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -g $group -t $instanceType -z $zone  --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -g $group -t $instanceType -z $zone  --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			else
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -t $instanceType -z $zone --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances --O $AmazonAccessKey -W $AmazonSecretKey -k $key -t $instanceType -z $zone --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			fi
		else
			if [ "$group" != "default" ]; then
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -g $group -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -g $group -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			else
				if [ "$IPaddress" = "autoassign" ]; then
					newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				else
					newInstance=`ec2-run-instances -O $AmazonAccessKey -W $AmazonSecretKey -k $key -t $instanceType -z $zone -p $instanceProfile --region $Region -s $subnet --private-ip-address $IPaddress $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
				fi
			fi
		fi
	fi
	echo $newInstance
	return
}

terminateInstance() {
	#terminate Instance
	ec2-terminate-instances -O $AmazonAccessKey -W $AmazonSecretKey --region $Region $instanceName > /tmp/termInstanceInfo_$instanceName 2>&1
	return
}

stopInstance() {
	#terminate Instance
	ec2-stop-instances -O $AmazonAccessKey -W $AmazonSecretKey --region $Region $instanceName > /tmp/stopInstanceInfo_$instanceName 2>&1
	return
}

startInstance() {
	#terminate Instance
	ec2-start-instances -O $AmazonAccessKey -W $AmazonSecretKey --region $Region $instanceName > /tmp/startInstanceInfo_$instanceName 2>&1

	cat /tmp/startInstanceInfo_$instanceName | grep INSTANCE > /tmp/startInstanceStatus_$instanceName
	if [ `cat /tmp/startInstanceStatus_$instanceName | wc -c` -eq 0 ]; then
		echo "Failed, check /tmp/startInstanceInfo_$instanceName"
		exit 1
	fi
	echo "Success"
	exit 0
}

assignElasticIP() {
	#terminate Instance

        if [ "$subnet" == "unassigned" ]; then
		ec2-associate-address -O $AmazonAccessKey -W $AmazonSecretKey -i $instanceName $IPAddress > /tmp/assignElasticIPInfo_$IPAddress 2>&1
	else
		EIP=`ec2-describe-addresses -O $AmazonAccessKey -W $AmazonSecretKey --region $Region $IPAddress | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $4}'`
                ec2-associate-address -O $AmazonAccessKey -W $AmazonSecretKey --region $Region -i $instanceName -a $EIP > /tmp/assignElasticIPInfo_$IPAddress 2>&1
	fi

	cat /tmp/assignElasticIPInfo_$IPAddress | grep ADDRESS > /tmp/assignElasticIPStatus_$IPAddress
	if [ `cat /tmp/assignElasticIPStatus_$IPAddress | wc -c` -eq 0 ]; then
		echo "Failed, check /tmp/assignElasticIPInfo_$IPAddress"
		exit 1
	fi

	echo "Success"
	exit 0
}

deassignElasticIP() {
	#terminate Instance
	ec2-disassociate-address -O $AmazonAccessKey -W $AmazonSecretKey $IPAddress > /tmp/deassignElasticIPInfo_$IPAddress 2>&1

	cat /tmp/deassignElasticIPInfo_$IPAddress | grep ADDRESS > /tmp/deassignElasticIPStatus_$IPAddress
	if [ `cat /tmp/deassignElasticIPStatus_$IPAddress | wc -c` -eq 0 ]; then
		echo "Failed, check /tmp/deassignElasticIPStatus_$IPAddress"
		exit 1
	fi

	echo "Success"
	exit 0
}

getSubnet() {
        #get local Instance ID
        getInstancePrivate >/dev/null 2>&1
        #get Subnet
        subnet=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $16}'`
        if [ "$subnet" == "" ]; then
                describeInstance
        fi
        subnet=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $16}'`

	if [[ $subnet == *"subnet"* ]]
	then
        	echo $subnet
	else
		echo "failed"
	fi

        return
}


case "$1" in
  getInstance)
  	getInstance
	;;
  getZone)
  	getZone
	;;
  getPrivateIP)
  	getPrivateIP
	;;
  getKey)
  	getKey
	;;
  getAMI)
  	getAMI
	;;
  getType)
  	getType
	;;
  launchInstance)
  	launchInstance
	;;
  terminateInstance)
  	terminateInstance
	;;
  stopInstance)
  	stopInstance
	;;
  startInstance)
  	startInstance
	;;
  assignElasticIP)
  	assignElasticIP
	;;
  deassignElasticIP)
  	deassignElasticIP
	;;
  getProfile)
  	getProfile
	;;
  getGroup)
  	getGroup
	;;
  getSubnet)
  	getSubnet
	;;
  *)
	echo $"Usage: $0 {launchInstance|getInstance|getZone|getPrivateIP|getType|getKey|getAMI|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup|getSubnet}"
	exit 1
esac

exit $?
