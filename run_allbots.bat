@echo off
TITLE kryptoco AI Trading Firm - Launcher
cd /d C:\Users\Suraj\Dropbox\PC\Desktop\kryptoco

echo [1/2] Launching 7-Bot Orchestrator (main.py)...
start "Saviour AI Bots" cmd /k "call .venv\Scripts\activate && python main.py"

echo [2/2] Launching Streamlit Dashboard (streamlit run)...
start "Krypto Dashboard" cmd /k "call .venv\Scripts\activate && streamlit run app.py"

echo ========================================================
echo Both windows have been launched successfully!
echo Close this launcher window whenever you like.
echo ========================================================
timeout /t 5 >nul