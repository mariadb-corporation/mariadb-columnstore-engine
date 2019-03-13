#! /bin/sh
# MCSInstanceCmds.sh
# get-local-instance-ID, get-zone, getPrivateIP from a Cloud environment
#
# 1. Amazon EC2

if [ -z "$COLUMNSTORE_INSTALL_DIR" ]; then
	COLUMNSTORE_INSTALL_DIR=/usr/local/mariadb/columnstore
fi

export COLUMNSTORE_INSTALL_DIR=$COLUMNSTORE_INSTALL_DIR

#get temp directory
tmpdir=`$COLUMNSTORE_INSTALL_DIR/bin/getConfig SystemConfig SystemTempFileDir`

#check command
if [ "$1" = "" ]; then
	echo "Enter Command Name: {launchInstance|getInstance|getZone|getPrivateIP|getKey|getAMI|getType|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup|getSubnet|getVpc|getRegion|getRole}"
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


$COLUMNSTORE_INSTALL_DIR/bin/MCSgetCredentials.sh >/dev/null 2>&1

test -f $COLUMNSTORE_INSTALL_DIR/post/functions && . $COLUMNSTORE_INSTALL_DIR/post/functions

#default instance to null
instance=""

AWSCLI="aws ec2 "

getRegion() {
        Region=`curl --fail --silent /dev/null http://169.254.169.254/latest/dynamic/instance-identity/document | grep region | cut -d':' -f2 | sed 's/\"//g'  | sed 's/\,//g' | sed -e 's/^[ \t]*//'`

        echo $Region
        return
}

getRole() {
	#check for iam folder
	iam=`curl -s http://169.254.169.254/latest/meta-data/ | grep iam`

	if [ -z "$iam" ]; then
        	return "";
	fi

	Role=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/`

	if [ -z "$Role" ]; then
		return "";
	fi

        echo $Role
        return
}

getInstance() {
	if [ "$instanceName" != "" ]; then
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
        #get region
        getRegion >/dev/null 2>&1

	#check if running or terminated
	state=`aws ec2 describe-instances --instance-ids $instanceName --region $Region --output text --query 'Reservations[*].Instances[*].State.Name'`
	if [ "$state" != "running" ]; then
		# not running
		if [ "$state" == "stopped" ]; then
			echo "stopped"
			exit 1
		else
			if [ "$state" == "terminated" ]; then
				echo "terminated"
				exit 1
			else
				if [ "$state" == "shutting-down" ]; then
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
	IpAddr=`aws ec2 describe-instances --instance-ids $instanceName --region $Region --output text --query 'Reservations[*].Instances[*].PrivateIpAddress'`

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
	#get region
        getRegion >/dev/null 2>&1

	#get local Instance ID
	getInstance >/dev/null 2>&1
	
	key=`aws ec2 describe-instances --instance-ids $instanceName --region $Region --output text --query 'Reservations[*].Instances[*].KeyName'`

	echo $key
	return
}

getVpc() {
        #get region
        getRegion >/dev/null 2>&1

        #get local Instance ID
        getInstance >/dev/null 2>&1

        #get VCP
        vpc=`aws ec2 describe-instances --instance-ids $instanceName --output text --region $Region --query 'Reservations[*].Instances[*].VpcId'`

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
        #get region
        getRegion >/dev/null 2>&1

	#get group id
	groupid=`aws ec2 describe-instances --instance-ids $instanceName --region $Region --output text --query 'Reservations[*].Instances[*].SecurityGroups[*].GroupId' | grep -m 1 sg`
	echo $groupid
	return
}

getProfile() {
        #get region
        getRegion >/dev/null 2>&1

	# get profile	
	instanceProfile=`curl -s http://169.254.169.254/latest/meta-data/profile`

	echo $instanceProfile
	return
}

launchInstance() {
        #get region
        getRegion >/dev/null 2>&1

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

	#get Subnet
	getSubnet >/dev/null 2>&1

	#get IAM Role
	getRole >/dev/null 2>&1

	if [ "$Role" = "" ] || [ "$Role" = "default" ]; then
		if [ "$groupid" != "default" ]; then
			if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ] ; then
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone   --subnet-id $subnet --image-id $ami --security-group-ids $groupid --query 'Instances[*].InstanceId' --output text`
			else
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone   --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --query 'Instances[*].InstanceId' --output text`
			fi
		else
			if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone  --subnet-id $subnet --image-id $ami --query 'Instances[*].InstanceId' --output text`
			else
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --query 'Instances[*].InstanceId' --output text`
			fi
		fi
	else
		if [ "$groupid" != "default" ]; then
			if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile "Name=$Role"  --subnet-id $subnet --image-id $ami --security-group-ids $groupid --query 'Instances[*].InstanceId' --output text`
			else
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key  --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile "Name=$Role"  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --security-group-ids $groupid --query 'Instances[*].InstanceId' --output text`
			fi
		else
			if [ "$IPaddress" = "autoassign" ] || [ "$IPaddress" = "unassigned" ]; then
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile "Name=$Role"  --subnet-id $subnet --image-id $ami --query 'Instances[*].InstanceId' --output text`
			else
				newInstance=`$AWSCLI run-instances --region $Region  --key-name $key --instance-type $instanceType --placement AvailabilityZone=$zone --iam-instance-profile "Name=$Role"  --subnet-id $subnet --private-ip-address $IPaddress --image-id $ami --query 'Instances[*].InstanceId' --output text`
			fi
		fi
	fi
	
	echo $newInstance
	return
}

terminateInstance() {
        #get region
        getRegion >/dev/null 2>&1

	#terminate Instance
	$AWSCLI terminate-instances --instance-ids $instanceName --region $Region > ${tmpdir}/termInstanceInfo_$instanceName 2>&1
	return
}

stopInstance() {
        #get region
        getRegion >/dev/null 2>&1

	#terminate Instance
	$AWSCLI stop-instances --instance-ids $instanceName --region $Region > ${tmpdir}/stopInstanceInfo_$instanceName 2>&1
	return
}

startInstance() {
        #get region
        getRegion >/dev/null 2>&1

	#terminate Instance
	$AWSCLI start-instances --instance-ids $instanceName --region $Region > ${tmpdir}/startInstanceInfo_$instanceName 2>&1

	cat ${tmpdir}/startInstanceInfo_$instanceName | grep InstanceId > ${tmpdir}/startInstanceStatus_$instanceName
	if [ `cat ${tmpdir}/startInstanceStatus_$instanceName | wc -c` -eq 0 ]; then
		echo "Failed, check ${tmpdir}/startInstanceInfo_$instanceName"
		exit 1
	fi
	echo "Success"
	exit 0
}

assignElasticIP() {
        #get region
        getRegion >/dev/null 2>&1

	EIP=`$AWSCLI describe-addresses --region $Region --public-ips  $IPAddress --query 'Addresses[*].AllocationId' --output text`
        
	$AWSCLI associate-address --region $Region  --instance-id $instanceName --allocation-id $EIP > ${tmpdir}/assignElasticIPInfo_$IPAddress 2>&1

	cat ${tmpdir}/assignElasticIPInfo_$IPAddress | grep error > ${tmpdir}/assignElasticIPStatus_$IPAddress
	if [ `cat ${tmpdir}/assignElasticIPStatus_$IPAddress | wc -c` -ne 0 ]; then
		echo "Failed, check ${tmpdir}/assignElasticIPInfo_$IPAddress"
		exit 1
	fi

	echo "Success"
	exit 0
}

deassignElasticIP() {
        #get region
        getRegion >/dev/null 2>&1

	EIP=`$AWSCLI describe-addresses --region $Region --public-ips  $IPAddress --query 'Addresses[*].AssociationId' --output text`

	$AWSCLI disassociate-address --region $Region --association-id $EIP > ${tmpdir}/deassignElasticIPInfo_$IPAddress 2>&1
	cat ${tmpdir}/deassignElasticIPInfo_$IPAddress | grep error > ${tmpdir}/deassignElasticIPStatus_$IPAddress
	if [ `cat ${tmpdir}/deassignElasticIPStatus_$IPAddress | wc -c` -ne 0 ]; then
		echo "Failed, check ${tmpdir}/deassignElasticIPStatus_$IPAddress"
		exit 1
	fi

	echo "Success"
	exit 0
}

getSubnet() {
        #get region
        getRegion >/dev/null 2>&1

        #get local Instance ID
        getInstance >/dev/null 2>&1
        #get Subnet
        subnet=`aws ec2 describe-instances --instance-ids $instanceName --region $Region --output text --query 'Reservations[*].Instances[*].SubnetId'`

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
  getRegion)
        getRegion
        ;;
  getRole)
        getRole
        ;;
  *)
	echo $"Usage: $0 {launchInstance|getInstance|getZone|getPrivateIP|getType|getKey|getAMI|terminateInstance|startInstance|assignElasticIP|deassignElasticIP|getProfile|stopInstance|getGroup|getSubnet|getVpc|getRegion|getRole}"
	exit 1
esac

exit $?
