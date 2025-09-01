# -------------------------------------------------------------- #
# Enable cross engine join
# Configure user and password in Columnstore.xml file
# -------------------------------------------------------------- #
--exec /usr/bin/mcsSetConfig CrossEngineSupport User 'cejuser'
--exec /usr/bin/mcsSetConfig CrossEngineSupport Password 'Vagrant1|0000001'
--exec $MCS_MCSSETCONFIG CrossEngineSupport Port $MASTER_MYPORT

# -------------------------------------------------------------- #
# Create corresponding in the server
# -------------------------------------------------------------- #

--disable_warnings
CREATE USER IF NOT EXISTS'cejuser'@'localhost' IDENTIFIED BY 'Vagrant1|0000001';
--enable_warnings

GRANT ALL PRIVILEGES ON *.* TO 'cejuser'@'localhost';
FLUSH PRIVILEGES;

