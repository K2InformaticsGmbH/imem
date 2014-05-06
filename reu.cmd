@echo off
if "%1%"=="" goto verbose
if "%1%"=="silent" goto silent
cls
rebar skip_deps=true eunit suites=%1%
goto end

:verbose
cls
rebar skip_deps=true eunit 
goto end

:silent
echo running all tests for project, reporting only failures
rebar skip_deps=true eunit | grep assertion_failed

:end