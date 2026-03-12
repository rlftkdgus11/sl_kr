@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
set "TMP_DIR=%USERPROFILE%\TempBuild"
set "OUT_DLL=%TMP_DIR%\dsound.dll"
set "GAME_DLL=%ROOT_DIR%\dsound.dll"
set "RELEASE_DLL=%ROOT_DIR%\Release\dsound.dll"
set "PATCH_DLL=%SCRIPT_DIR%dsound.dll"
if not exist "%TMP_DIR%" mkdir "%TMP_DIR%"
set "TEMP=%TMP_DIR%"
set "TMP=%TMP_DIR%"
pushd "%USERPROFILE%"
call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x86 >nul
popd
cl /nologo /EHsc /O2 /MT /LD "%SCRIPT_DIR%proxy_dsound.cpp" /link /DEF:"%SCRIPT_DIR%dsound.def" /OUT:"%OUT_DLL%" user32.lib gdi32.lib dxguid.lib
if not exist "%OUT_DLL%" goto :done
echo Built: %OUT_DLL%
copy /Y "%OUT_DLL%" "%GAME_DLL%" >nul
if errorlevel 1 echo [ERROR] Copy failed: %GAME_DLL% & exit /b 1
if not exist "%ROOT_DIR%\Release" mkdir "%ROOT_DIR%\Release"
copy /Y "%OUT_DLL%" "%RELEASE_DLL%" >nul
if errorlevel 1 echo [ERROR] Copy failed: %RELEASE_DLL% & exit /b 1
copy /Y "%OUT_DLL%" "%PATCH_DLL%" >nul
if errorlevel 1 echo [ERROR] Copy failed: %PATCH_DLL% & exit /b 1
echo Deployed: %GAME_DLL%
echo Deployed: %RELEASE_DLL%
echo Deployed: %PATCH_DLL%
echo Build + deploy completed.
:done
endlocal
