#!/bin/bash

#-------------------------------------------------------------------------------
# Compare casual partition min/max ranges for the given schema (using editem)
# $1 - schema to validate
# $2 - name of the log file where the editem output will be logged
# returns 0 if casual partition min/max limits match the expected results
# returns 1 if casual partition min/max limits do not match the expected results
#-------------------------------------------------------------------------------
validateExtentMap()
{
	local schema=$1
	local cpLogFile=$2

	rm -f $cpLogFile
	rm -f temp_oid.lis
	/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root \
	calpontsys -e "select objectid from syscolumn where \`schema\`='$schema';" | grep -v objectid > temp_oid.lis

	# Extract columns 6 and 8 from editem output; these are min/max limits
	for oid in $(cat temp_oid.lis);
	do
		/usr/local/Calpont/bin/editem -m -o $oid | grep min | cut -d' ' -f6,8 >> $cpLogFile
	done

	diff editem_cp.log_ref $cpLogFile > /dev/null
	local editem_status=$?
	rm -f temp_oid.lis

	return $editem_status
}

#-------------------------------------------------------------------------------
# Clear casual partition min/max ranges for tpch100 schema by using "editem -c"
#-------------------------------------------------------------------------------
clearExtentMapMinMax()
{
	if [ $1 ] ; then
		schema=$1
	else
		echo "Must pass schema to clear_editem."
		exit 1
	fi
	rm -f temp_oid.lis
	/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root \
		calpontsys -e "select objectid from syscolumn where \`schema\`='$schema';" | grep -v objectid > temp_oid.lis

	for oid in $(cat temp_oid.lis);
	do
		/usr/local/Calpont/bin/editem -c $oid
	done
	rm -f temp_oid.lis
}
