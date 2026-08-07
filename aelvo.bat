@echo off
rem AELVO terminal CLI launcher — `aelvo` is short for `python -m cli`.
rem Passes through every argument (prompt, flags, workspace, …).
setlocal
where python >nul 2>nul
if errorlevel 1 (
    py -3 -m cli %*
) else (
    python -m cli %*
)
endlocal
