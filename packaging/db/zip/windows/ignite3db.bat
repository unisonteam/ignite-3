@rem
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements. See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License. You may obtain a copy of the License at
@rem
@rem      http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

@if "%DEBUG%"=="" @echo off
@rem ##########################################################################
@rem
@rem  ignite3 startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%"=="" set DIRNAME=.
set APP_HOME=%DIRNAME%..

@rem Resolve any "." and ".." in APP_HOME to make it shorter.
for %%i in ("%APP_HOME%") do set APP_HOME=%%~fi

@rem Add default JVM options here. You can also use JAVA_OPTS and IGNITE3_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if %ERRORLEVEL% equ 0 goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH. 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto execute

echo. 1>&2
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME% 1>&2
echo. 1>&2
echo Please set the JAVA_HOME variable in your environment to match the 1>&2
echo location of your Java installation. 1>&2

goto fail

:execute

@rem if IGNITE_HOME is not set than it will be parent directory for bin
if "%IGNITE_HOME%"=="" set IGNITE_HOME=%APP_HOME%
cd "%IGNITE_HOME%"

set DEFAULT_NODE_NAME=defaultNode

@rem Set default values based on IGNITE_HOME
set DEFAULT_WORK_DIR=@WORK_DIR@
set DEFAULT_LOG_DIR=@LOG_DIR@
set DEFAULT_LIBS_DIR=@LIB_DIR@
set DEFAULT_CONFIG_DIR=@CONF_DIR@
set DEFAULT_CONFIG_NAME=@CONF_NAME@
set DEFAULT_EXTRA_JVM_ARGS=

@rem Check if environment variables are set and prioritize them over command-line options
if "%IGNITE_NODE_NAME%"=="" set IGNITE_NODE_NAME=%DEFAULT_NODE_NAME%
if "%IGNITE_WORK_DIR%"=="" set IGNITE_WORK_DIR=%DEFAULT_WORK_DIR%
@rem TODO https://issues.apache.org/jira/browse/IGNITE-24812
@rem if "%IGNITE_LOG_DIR%"=="" set IGNITE_LOG_DIR=%DEFAULT_LOG_DIR%
set IGNITE_LOG_DIR=%DEFAULT_LOG_DIR%
if "%IGNITE_LIBS_DIR%"=="" set IGNITE_LIBS_DIR=%DEFAULT_LIBS_DIR%
if "%IGNITE_CONFIG_DIR%"=="" set IGNITE_CONFIG_DIR=%DEFAULT_CONFIG_DIR%
if "%IGNITE_CONFIG_FILE%"=="" set IGNITE_CONFIG_FILE=%IGNITE_CONFIG_DIR%\%DEFAULT_CONFIG_NAME%
if "%IGNITE_EXTRA_JVM_ARGS%"=="" set IGNITE_EXTRA_JVM_ARGS=%DEFAULT_EXTRA_JVM_ARGS%

call "%IGNITE_CONFIG_DIR%\@VARS_FILE_NAME@"
call "%IGNITE_LIBS_DIR%\@BOOTSTRAP_FILE_NAME@"

@rem Execute ignite3
%JAVA_CMD_WITH_ARGS% %APPLICATION_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if %ERRORLEVEL% equ 0 goto mainEnd

:fail
rem Set variable IGNITE3_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
set EXIT_CODE=%ERRORLEVEL%
if %EXIT_CODE% equ 0 set EXIT_CODE=1
if not ""=="%IGNITE3_EXIT_CONSOLE%" exit %EXIT_CODE%
exit /b %EXIT_CODE%

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
