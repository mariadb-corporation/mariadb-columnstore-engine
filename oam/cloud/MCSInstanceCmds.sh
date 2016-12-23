#! /bin/sh
# MCSInstanceCmds.sh
# get-local-instance-ID, get-zone, getPrivateIP from a Cloud environment
#
# 1. Amazon EC2

prefix=/home/mariadb-user

#check command
if [ "$1" = "" ]; then
	echo "Enter Command Name: {launchInstance|getInstance|getZone|getPrivateIP|getKey|getAMI|getType|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup|getSubnet|getVpc}
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
		groupid="unassigned"
	else
		groupid="$4"
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

subnet=`$prefix/mariadb/columnstore/bin/getConfig Installation AmazonSubNetID`

#default instance to null
instance=""

describeInstanceFile="/tmp/describeInstance.txt"
touch $describeInstanceFile

AWSCLI="aws ec2 "

describeInstance() {
	$AWSCLI describe-instances   > $describeInstanceFile 2>&1
}

#call at start up
describeInstance

getInstance() {
	if [ "$instance" != "" ]; then
		echo $instance
		return
	fi
	
	instance=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

	echo $instance
	return
}

getInstancePrivate() {
	if [ "$instance" != "" ]; then
		echo $instance
		return
	fi
	
	instance=`curl -s http://169.254.169.254/latest/meta-data/instance-id`
	
	return
}


getZone() {
	zone=`curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone`

	echo $zone
	return
}

getPrivateIP() {
	#get instance info
	grep -B1 -A7 -m 1 $instanceName $describeInstanceFile > /tmp/instanceInfo_$instanceName 2>&1
	if [ `cat /tmp/instanceInfo_$instanceName | wc -c` -eq 0 ]; then
		describeInstance
	fi
	grep -B1 -A7 -m 1 $instanceName $describeInstanceFile > /tmp/instanceInfo_$instanceName 2>&1

	#check if running or terminated
	cat /tmp/instanceInfo_$instanceName | grep STATE > /tmp/instanceStatus_$instanceName
	if [ `cat /tmp/instanceStatus_$instanceName | wc -c` -ne 0 ]; then
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
	IpAddr=`head -n 2 /tmp/instanceInfo_$instanceName | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $13}'`

	echo $IpAddr
	exit 0
}

getType() {
	#get local Instance ID
	instanceType=`curl -s http://169.254.169.254/latest/meta-data/instance-type`

	echo $instanceType
	return
}

getKey() {
	#get local Instance ID
	getInstancePrivate >/dev/null 2>&1
	#get Key
	if [ "$key" == "" ]; then
		describeInstance
	fi
	
	key=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $10}'`

	echo $key
	return
}

getVpc() {
        #get local Instance ID
        getInstancePrivate >/dev/null 2>&1
        #get VCP
        if [ "$vcp" == "" ]; then
                describeInstance
        fi

        vpc=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $21}'`

        echo $vpc
        return
}

getAMI() {
	#get local Instance ID
	ami=`curl -s http://169.254.169.254/latest/meta-data/ami-id`

	echo $ami
	return
}

getGroup() {
	# get vpc
	getVpc  >/dev/null 2>&1

	#get group name 
	group=`curl -s http://169.254.169.254/latest/meta-data/security-groups`

	#get group id
	groupid=`aws ec2 describe-security-groups --group-names | grep -A 1 $group | grep -m 1 $vpc | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $3}'`
	echo $groupid
	return
}

getProfile() {
	# get profile	
	instanceProfile=`curl -s http://169.254.169.254/latest/meta-data/profile`

	echo $instanceProfile
	return
}

launchInstance() {
	#get publickey
	getKey >/dev/null 2>&1
	if [ "$groupid" = "unassigned" ]; then
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

	#get Subnet
	getSubnet >/dev/null 2>&1

		if [ "$instanceProfile" = "" ] || [ "$instanceProfile" = "default-hvm" ]; then
			if [ "$groupid" != "default" ]; then
				if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ] ; then
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone   --subnet-id $subnet --image-id $ami --security-group-ids $groupid | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				else
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone   --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				fi
			else
				if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone  --subnet-id $subnet --image-id $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				else
					newInstance=`$AWSCLI run-instances - --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				fi
			fi
		else
			if [ "$groupid" != "default" ]; then
				if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --image-id $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				else
					newInstance=`$AWSCLI run-instances  --key-name $key  --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				fi
			else
				if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --image-id $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				else
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami | grep  -m 1 INSTANCE | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $7}'`
				fi
			fi
		fi
	
	echo $newInstance
	return
}

terminateInstance() {
	#terminate Instance
	$AWSCLI terminate-instances --instance-ids $instanceName > /tmp/termInstanceInfo_$instanceName 2>&1
	return
}

stopInstance() {
	#terminate Instance
	$AWSCLI stop-instances --instance-ids $instanceName > /tmp/stopInstanceInfo_$instanceName 2>&1
	return
}

startInstance() {
	#terminate Instance
	$AWSCLI start-instances --instance-ids $instanceName > /tmp/startInstanceInfo_$instanceName 2>&1

	cat /tmp/startInstanceInfo_$instanceName | grep INSTANCE > /tmp/startInstanceStatus_$instanceName
	if [ `cat /tmp/startInstanceStatus_$instanceName | wc -c` -eq 0 ]; then
		echo "Failed, check /tmp/startInstanceInfo_$instanceName"
		exit 1
	fi
	echo "Success"
	exit 0
}

assignElasticIP() {
	EIP=`$AWSCLI describe-addresses --public-ips  $IPAddress | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $2}'`
        
	$AWSCLI associate-address --instance-id $instanceName --allocation-id $EIP > /tmp/assignElasticIPInfo_$IPAddress 2>&1

	cat /tmp/assignElasticIPInfo_$IPAddress | grep error > /tmp/assignElasticIPStatus_$IPAddress
	if [ `cat /tmp/assignElasticIPStatus_$IPAddress | wc -c` -ne 0 ]; then
		echo "Failed, check /tmp/assignElasticIPInfo_$IPAddress"
		exit 1
	fi

	echo "Success"
	exit 0
}

deassignElasticIP() {
	EIP=`$AWSCLI describe-addresses --public-ips  $IPAddress | awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $3}'`

	$AWSCLI disassociate-address --association-id $EIP > /tmp/deassignElasticIPInfo_$IPAddress 2>&1
	cat /tmp/deassignElasticIPInfo_$IPAddress | grep error > /tmp/deassignElasticIPStatus_$IPAddress
	if [ `cat /tmp/deassignElasticIPStatus_$IPAddress | wc -c` -ne 0 ]; then
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
        subnet=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $19}'`
        if [ "$subnet" == "" ]; then
                describeInstance
        fi
        subnet=`cat $describeInstanceFile | grep -m 1 $instance |  awk '{gsub(/^[ \t]+|[ \t]+$/,"");print $19}'`

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
  getVpc)
	getVpc
	;;
  *)
	echo $"Usage: $0 {launchInstance|getInstance|getZone|getPrivateIP|getType|getKey|getAMI|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup|getSubnet|getVpc}"
	exit 1
esac

exit $?
