# -------------------------------------------------------------- #
# Enable cross engine join
# Configure user and password in Columnstore.xml file
# -------------------------------------------------------------- #

--disable_query_log
if (!$MASTER_MYPORT)
{
  # Running with --extern
  let $MASTER_MYPORT=`SELECT @@port`;
}

--exec $MCS_MCSSETCONFIG CrossEngineSupport User 'cejuser'
--exec $MCS_MCSSETCONFIG CrossEngineSupport Password 'Vagrant1|0000001'
--exec $MCS_MCSSETCONFIG CrossEngineSupport Port $MASTER_MYPORT

# -------------------------------------------------------------- #
# Create corresponding in the server
# -------------------------------------------------------------- #

--disable_warnings
CREATE USER IF NOT EXISTS'cejuser'@'localhost' IDENTIFIED BY 'Vagrant1|0000001';
--enable_warnings

GRANT ALL PRIVILEGES ON *.* TO 'cejuser'@'localhost';
FLUSH PRIVILEGES;
--enable_query_log

