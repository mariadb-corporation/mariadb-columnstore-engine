DROP USER IF EXISTS 'youruser'@'localhost';
CREATE USER 'youruser'@'localhost' IDENTIFIED BY 'yourpassword';
GRANT ALL PRIVILEGES ON *.* TO 'youruser'@'localhost' WITH GRANT OPTION;

