@echo off
REM Lancer l'ETL jeux vidéo (Windows)
SETLOCAL ENABLEDELAYEDEXPANSION

REM --- Chemins à adapter ---
set PROJECT_DIR=%~dp0
cd /d "%PROJECT_DIR%"

REM Activer venv si présent
IF EXIST ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
)

python etl_games.py --limit 50
exit /b %ERRORLEVEL%
