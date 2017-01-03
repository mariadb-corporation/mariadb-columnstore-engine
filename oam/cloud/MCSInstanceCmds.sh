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
	if [ "$instanceiName" != "" ]; then
		echo $instanceName
		return
	fi
	
	instanceName=`curl -s http://169.254.169.254/latest/meta-data/instance-id`

	echo $instanceName
	return
}


getZone() {
	zone=`curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone`

	echo $zone
	return
}

getPrivateIP() {
	#check if running or terminated
	state=`aws ec2 describe-instances --instance-ids $instanceName --output text --query 'Reservations[*].Instances[*].State.Name'`
	if [ "$state" != "running" ]; then
		# not running
		if [ "$state" != "stopped" ]; then
			echo "stopped"
			exit 1
		else
			if [ "$state" != "terminated" ]; then
				echo "terminated"
				exit 1
			else
				if [ "$state" != "shutting-down" ]; then
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
	IpAddr=`aws ec2 describe-instances --instance-ids $instanceName --output text --query 'Reservations[*].Instances[*].PrivateIpAddress'`

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
	getInstance >/dev/null 2>&1
	
	key=`aws ec2 describe-instances --instance-ids $instanceName --output text --query 'Reservations[*].Instances[*].KeyName'`

	echo $key
	return
}

getVpc() {
        #get local Instance ID
        getInstance >/dev/null 2>&1
        #get VCP
        vpc=`aws ec2 describe-instances --instance-ids $instanceName --output text --query 'Reservations[*].Instances[*].VpcId'`

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
	#get group id
	groupid=`aws ec2 describe-instances --instance-ids $instanceName --output text --query 'Reservations[*].Instances[*].SecurityGroups[*].GroupId'`
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
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone   --subnet-id $subnet --image-id $ami --security-group-ids $groupid --query 'Instances[*].InstanceId' --output text`
				else
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone   --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --query 'Instances[*].InstanceId' --output text`
				fi
			else
				if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone  --subnet-id $subnet --image-id $ami --query 'Instances[*].InstanceId' --output text`
				else
					newInstance=`$AWSCLI run-instances - --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --query 'Instances[*].InstanceId' --output text`
				fi
			fi
		else
			if [ "$groupid" != "default" ]; then
				if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --image-id $ami --query 'Instances[*].InstanceId' --output text`
				else
					newInstance=`$AWSCLI run-instances  --key-name $key  --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --query 'Instances[*].InstanceId' --output text`
				fi
			else
				if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --image-id $ami --query 'Instances[*].InstanceId' --output text`
				else
					newInstance=`$AWSCLI run-instances  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile $instanceProfile  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --query 'Instances[*].InstanceId' --output text`
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
        getInstance >/dev/null 2>&1
        #get Subnet
        subnet=`aws ec2 describe-instances --instance-ids $instanceName --output text --query 'Reservations[*].Instances[*].SubnetId'`

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
