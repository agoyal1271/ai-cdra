"""
Unified launcher for Cloudera AI Agents.
On Cloudera AI (CML): serves FastAPI + prebuilt React on CDSW_APP_PORT.
Local dev: runs FastAPI (port 8000) + Vite (port 5173) in parallel threads.
"""
import os
import subprocess
import sys
import time
import threading

IS_CLOUDERA = bool(os.getenv("CDSW_APP_PORT") or os.getenv("CDSW_PROJECT_ID"))
APP_PORT = int(os.getenv("CDSW_APP_PORT", "8000"))

# On CML the project always lives at /home/cdsw.
# Locally, derive from __file__ (defined when run as a real script).
if IS_CLOUDERA:
    BASE_DIR = "/home/cdsw"
    PYTHON = "/usr/local/bin/python3"
else:
    try:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        BASE_DIR = os.getcwd()
    # Prefer the system python3; fall back to whatever is on PATH
    PYTHON = os.getenv("PYTHON_BIN", sys.executable or "python3")

BACKEND_DIR = os.path.join(BASE_DIR, "02_backend")
FRONTEND_DIR = os.path.join(BASE_DIR, "03_frontend")


def run_backend():
    env = {**os.environ, "PYTHONPATH": BACKEND_DIR}
    workers = int(os.getenv("UVICORN_WORKERS", "1"))
    cmd = [
        PYTHON, "-m", "uvicorn", "app:app",
        "--host", "0.0.0.0",
        "--port", str(APP_PORT),
        "--workers", str(workers),
        "--log-level", "info",
    ]
    if not IS_CLOUDERA:
        cmd.append("--reload")
    print(f"[launch] starting uvicorn on :{APP_PORT} workers={workers} cwd={BACKEND_DIR}")
    subprocess.run(cmd, cwd=BACKEND_DIR, env=env)


def run_frontend():
    if IS_CLOUDERA:
        req = os.path.join(BACKEND_DIR, "requirements.txt")
        if os.path.exists(req):
            print("[launch] installing Python deps...")
            subprocess.run([PYTHON, "-m", "pip", "install", "-r", req, "-q"], check=False)
        else:
            print(f"[launch] requirements.txt not found at {req}")

        dist = os.path.join(FRONTEND_DIR, "dist")
        if os.path.isdir(dist):
            print(f"[launch] frontend dist/ found at {dist} — will be served by FastAPI")
        else:
            print(f"[launch] no dist/ at {dist} — API-only mode")

        print(f"[launch] frontend ready on port {APP_PORT}")
    else:
        time.sleep(1.5)
        subprocess.run(["npm", "run", "dev"], cwd=FRONTEND_DIR)


# CML PBJ kernel doesn't set __name__ == "__main__", so guard with True.
if True:
    print(f"[launch] Starting Cloudera AI Agents ({'CML' if IS_CLOUDERA else 'local dev'})")
    print(f"[launch] BASE_DIR={BASE_DIR}  BACKEND_DIR={BACKEND_DIR}  PYTHON={PYTHON}")
    print(f"[launch] 02_backend exists: {os.path.isdir(BACKEND_DIR)}")

    if IS_CLOUDERA:
        run_frontend()
        run_backend()
    else:
        backend_thread = threading.Thread(target=run_backend, daemon=True)
        backend_thread.start()

        frontend_thread = threading.Thread(target=run_frontend, daemon=False)
        frontend_thread.start()

        try:
            frontend_thread.join()
        except KeyboardInterrupt:
            print("\n[launch] shutting down...")
            sys.exit(0)
