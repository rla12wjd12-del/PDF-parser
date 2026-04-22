@echo off
setlocal

REM 더블클릭: 파일 선택창이 뜹니다.
REM PDF를 이 파일 위로 드래그앤드롭: 해당 PDF(여러 개면 순차) 처리합니다.

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

REM 가능한 경우 Python Launcher(py) 사용, 없으면 python 사용
where py >nul 2>nul
if %errorlevel%==0 (
  py -3 "%SCRIPT_DIR%main.py" %*
  exit /b %errorlevel%
)

python "%SCRIPT_DIR%main.py" %*
exit /b %errorlevel%

