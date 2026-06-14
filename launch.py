"""
Unified launcher for Cloudera AI Agents.
Starts FastAPI backend (port 8000) and Vite frontend (port 5173).
On Cloudera AI: frontend binds to CDSW_APP_PORT and serves the built static files.
"""
import os
import subprocess
import sys
import time
import threading

# Use Anaconda Python if available (has all dependencies installed)
PYTHON = os.getenv("PYTHON_BIN", "/opt/anaconda3/bin/python")

BASE_DIR = os.path.dirname(__file__)
BACKEND_DIR = os.path.join(BASE_DIR, "02_backend")
FRONTEND_DIR = os.path.join(BASE_DIR, "03_frontend")

IS_CLOUDERA = bool(os.getenv("CDSW_APP_PORT") or os.getenv("CDSW_PROJECT_ID"))
APP_PORT = int(os.getenv("CDSW_APP_PORT", "8000"))


def run_backend():
    env = {**os.environ, "PYTHONPATH": BACKEND_DIR}
    cmd = [PYTHON, "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", str(APP_PORT), "--reload"]
    subprocess.run(cmd, cwd=BACKEND_DIR, env=env)


def run_frontend():
    if IS_CLOUDERA:
        # On CAI, build and serve via FastAPI static files
        dist = os.path.join(FRONTEND_DIR, "dist")
        if not os.path.isdir(dist):
            print("Building frontend for Cloudera AI...")
            subprocess.run(["npm", "run", "build"], cwd=FRONTEND_DIR, check=True)
        print(f"Frontend built — served by FastAPI on port {APP_PORT}")
    else:
        # Local dev: run Vite dev server
        time.sleep(1.5)  # let backend start first
        subprocess.run(["npm", "run", "dev"], cwd=FRONTEND_DIR)


if __name__ == "__main__":
    print(f"Starting Cloudera AI Agents {'(Cloudera CAI mode)' if IS_CLOUDERA else '(local dev mode)'}")

    if IS_CLOUDERA:
        # On CAI: build frontend then start backend (single process)
        run_frontend()
        run_backend()
    else:
        # Local: run both in parallel threads
        backend_thread = threading.Thread(target=run_backend, daemon=True)
        backend_thread.start()

        frontend_thread = threading.Thread(target=run_frontend, daemon=False)
        frontend_thread.start()

        try:
            frontend_thread.join()
        except KeyboardInterrupt:
            print("\nShutting down...")
            sys.exit(0)
