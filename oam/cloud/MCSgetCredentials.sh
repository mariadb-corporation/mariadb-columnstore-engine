#! /bin/sh
# Get Amazon EC2 security-credentials, access and secret access keys
#
#first check for local versions, then meta-data versions
if [ -f $HOME/.aws/credentials ]; then
	echo "$HOME/.aws/credentials found, use local credentials"
	exit 0
fi

#get IAM Role
#check for iam folder
iam=`curl -s http://169.254.169.254/latest/meta-data/ | grep iam`

if [ -z "$iam" ]; then
	echo "No IAM in meta-data"
	exit 1;
fi

Role=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/`

if [ -z "$Role" ]; then
	echo "No Role in IAM meta-data"
	exit 1;
fi

aws_access_key_id=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/${Role} | grep AccessKeyId | cut -d':' -f2 | sed 's/[^0-9A-Z]*//g'`

if [ -z "$aws_access_key_id" ]; then
	echo "No Access-Key is blank in IAM meta-data"	
        exit 1;
fi

aws_secret_access_key=`curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/${Role} | grep SecretAccessKey | cut -d':' -f2 | sed 's/[^0-9A-Za-z/+=]*//g'`

if [ -z "$aws_secret_access_key" ]; then
	echo "No Secret-Key is blank in IAM meta-data"
        exit 1;
fi


echo $aws_access_key_id $aws_secret_access_key
#
export AWS_ACCESS_KEY_ID=${aws_access_key_id}
export AWS_SECRET_ACCESS_KEY=${aws_secret_access_key}

exit 0
