@echo off
pushd .
set help=
if "%1" == "-h" (set help=true)
if "%1" == "/h" (set help=true)
if "%1" == "--help" (set help=true)
if "%help%"=="true" (
  echo The Columnstore Support Report creates a report that can be sent to Columnstore
  echo for help with field support. There are no options to this command.
  set help=
  exit /B 0 
)

echo.  
echo Running the Columnstore Support Report, outputting to ColumnstoreSupportReport.txt

call :func > ColumnstoreSupportReport.txt 2>&1

echo.  
echo Report finished

popd
exit /B 0

:ErrorExit

echo.  
echo Error - Failed to find Columnstore Install Directory in Windows Registry, Exiting
popd
exit /B 1

:func

  setlocal
  set key="HKLM\SOFTWARE\Columnstore\Columnstore"
  set homeValue=ColumnstoreHome
  set configValue=ConfigFile

  for /f "tokens=3,*" %%a in ('reg query %key% /ve 2^>NUL ^| findstr REG_SZ') do (
    set ColumnstoreInstall=%%b
   )

  if "%ColumnstoreInstall%" == "" (
  	for /f "tokens=2,*" %%a in ('reg query %key% /ve 2^>NUL ^| findstr REG_SZ') do (
    	set ColumnstoreInstall=%%b
   	)
  )

  ::error out if can't locate Install Directory
  if "%ColumnstoreInstall%" == "" GOTO ErrorExit

echo #######################################################################
echo #                                                                     #
echo #     Columnstore Support Report - %date% %time%
echo #                                                                     #
echo #######################################################################
echo.
echo.
echo =======================================================================
echo =                    Software/Version Report                          =
echo =======================================================================
echo. 
echo.
echo -- Columnstore Software Version --
type %ColumnstoreInstall%\etc\ColumnstoreVersion.txt
echo. 
echo -- mysql Software Version --
mysql --user=root -e status
echo. 
echo -- Windows Version --
ver
echo.
echo.
echo =======================================================================
echo =                    Status Report                                    =
echo =======================================================================
echo.  
echo. 
echo -- Columnstore Process Status -- 
echo. 

tasklist /FI "Imagename eq mysqld.exe"
tasklist /FI "Imagename eq controllernode.exe"   
tasklist /FI "Imagename eq workernode.exe"
tasklist /FI "Imagename eq PrimProc.exe"
tasklist /FI "Imagename eq ExeMgr.exe"      
tasklist /FI "Imagename eq DDLProc.exe"           
tasklist /FI "Imagename eq DMLProc.exe"         
tasklist /FI "Imagename eq WriteEngineServer.exe"

echo. 
echo. 
echo =======================================================================
echo =                    Configuration Report                             =
echo =======================================================================
echo.
echo -- Windows Columnstore Registry Values --
echo.

  echo ColumnstoreInstall = %ColumnstoreInstall%

  for /f "tokens=2,*" %%a in ('reg query %key% /v %homeValue% 2^>NUL ^| findstr %homeValue%') do (
    set ColumnstoreHome=%%b
  )
  echo ColumnstoreHome    = %ColumnstoreHome%

  for /f "tokens=2,*" %%a in ('reg query %key% /v %configValue% 2^>NUL ^| findstr %configValue%') do (
    set ConfigFile=%%b
  )
  echo ConfigFile     = %ConfigFile%
echo.
echo.
echo -- Columnstore System Configuration Information -- 
echo.
cd %ColumnstoreInstall%\bin
for /f "delims=" %%a in ('getConfig.exe DBBC NumBlocksPct') do @echo NumBlocksPct = %%a
for /f "delims=" %%a in ('getConfig.exe HashJoin TotalUmMemory') do @echo TotalUmMemory = %%a
for /f "delims=" %%a in ('getConfig.exe VersionBuffer VersionBufferFileSize') do @echo VersionBufferFileSize = %%a
for /f "delims=" %%a in ('getConfig.exe ExtentMap FilesPerColumnPartition') do @echo FilesPerColumnPartition = %%a
for /f "delims=" %%a in ('getConfig.exe ExtentMap ExtentsPerSegmentFile') do @echo ExtentsPerSegmentFile = %%a
echo.
echo.
echo -- Columnstore System Configuration File --
echo.
type "%ConfigFile%"
echo.  
echo.  
echo -- System Process Status -- 
echo. 
tasklist /v
echo.
echo =======================================================================
echo =                   Resource Usage Report                             =
echo =======================================================================
echo. 
echo -- System Information--
echo. 
systeminfo
echo. 
echo -- IP Configuration Information --
echo. 
ipconfig
echo.
echo  -- Disk BRM Data files --
echo.   
dir "%ColumnstoreInstall%\dbrm\"
echo.   
echo  -- View Table Locks --
echo.   
cd %ColumnstoreInstall%\bin\
viewtablelock.exe
echo.   
echo.    
echo   -- BRM Extent Map  --
echo.    
cd %ColumnstoreInstall%\bin\
editem.exe -i
echo.
echo.
echo =======================================================================
echo =                   Log Report                                        =
echo =======================================================================
echo. 
echo -- Columnstore Platform Logs --
echo. 
type "%ColumnstoreInstall%\log\ColumnstoreLog.txt"
echo. 
echo. 
echo -- Columnstore MySQl log --
echo.
type "%ColumnstoreInstall%\mysqldb\*.err" 
echo.
echo. 
echo -- Columnstore Bulk Load Logs --
echo. 
dir "%ColumnstoreInstall%\bulk\data"
echo. 
dir "%ColumnstoreInstall%\bulk\log"
echo. 
dir "%ColumnstoreInstall%\bulk\job"
echo.
echo -- Check for Errors in Bulk Logs --
echo.
cd "%ColumnstoreInstall%\bulk\log"
findstr /spin /c:"error" *
findstr /spin /c:"failed" *
cd "%ColumnstoreInstall%\bulk\job"
findstr /spin /c:"error" *
findstr /spin /c:"failed" *
echo.
echo =======================================================================
echo =                    DBMS Report                                      =
echo =======================================================================
echo.
echo -- DBMS Columnstore Mysql Version -- 
echo.
mysql --user=root -e status
echo. 
echo -- DBMS Mysql Columnstore System Column  -- 
echo. 
mysql --user=root -e "desc columnstoresys.syscolumn"
echo. 
echo -- DBMS Mysql Columnstore System Table  -- 
echo. 
mysql --user=root -e "desc columnstoresys.systable"
echo. 
echo -- DBMS Mysql Columnstore System Table Data -- 
echo.
mysql --user=root -e "select * from columnstoresys.systable"
echo. 
echo -- DBMS Mysql Columnstore Databases -- 
echo.
mysql --user=root -e "show databases"
echo.
echo -- DBMS Mysql Columnstore variables -- 
echo. 
mysql --user=root -e "show variables"
echo. 
echo -- DBMS Mysql Columnstore config file -- 
echo.
type "%ColumnstoreInstall%\my.ini"
echo.
echo -- Active Queries -- 

::cd \Columnstore\genii\oamapps\columnstoreSupport


