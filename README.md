#MariaDB ColumnStore Storage/Execution engine 1.0.6
MariaDB ColumnStore 1.0.6 is the development version of MariaDB ColumnStore. 
It is built by porting InfiniDB 4.6.2 on MariaDB 10.1.19 and adding entirely 
new features not found anywhere else.

#MariaDB ColumnStore 1.0.6 is an GA release. 
This is the first MariaDB ColumnStore release, not all features planned for the MariaDB ColumnStore 1.0 
series are included in this release. 

Additional features will be pushed in future releases. 
A few things to notice:
- Do not use alpha releases on production systems.

#Building
This repository is not meant to be built independently outside of the server.  This repository is integrated into http://mariadb-corporation/mariadb-columnstore-server (ie, the *server*) as a git submodule.  As such, you can find complete build instructions on *the server* page.

  https://github.com/mariadb-corporation/mariadb-columnstore-server

#Issue tracking
Issue tracking of MariaDB ColumnStore happens in JIRA, https://jira.mariadb.org/browse/MCOL
