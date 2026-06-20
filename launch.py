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

IS_CLOUDERA = bool(os.getenv("CDSW_APP_PORT") or os.getenv("CDSW_PROJECT_ID"))
APP_PORT = int(os.getenv("CDSW_APP_PORT", "8000"))


def _candidate_roots():
    """Plausible project roots, in priority order."""
    roots = []
    for r in (os.getenv("CDSW_PROJECT_DIR"), "/home/cdsw", os.getcwd(),
              os.path.expanduser("~")):
        if r and r not in roots:
            roots.append(r)
    return roots


def _has_backend(path):
    """A dir is the backend iff it contains app.py."""
    return os.path.isfile(os.path.join(path, "app.py"))


def resolve_dirs():
    """
    Find 02_backend / 03_frontend wherever they actually live on CML.
    Self-heals by extracting backend.zip if only the zip is present.
    Prints a full diagnostic of every candidate root so failures are legible.
    """
    import zipfile

    # 1) Diagnostic: show what's actually on disk at each candidate root.
    for root in _candidate_roots():
        try:
            listing = sorted(os.listdir(root))
        except Exception as e:
            print(f"[resolve] root={root} UNREADABLE ({e})")
            continue
        print(f"[resolve] root={root} -> {listing}")

    # 2) Look for an already-extracted 02_backend under any root.
    for root in _candidate_roots():
        cand = os.path.join(root, "02_backend")
        if _has_backend(cand):
            print(f"[resolve] found backend at {cand}")
            return root, cand, os.path.join(root, "03_frontend")

    # 3) Not found — search for backend.zip under any root and extract it.
    for root in _candidate_roots():
        zip_path = os.path.join(root, "backend.zip")
        if os.path.exists(zip_path):
            print(f"[resolve] extracting {zip_path} into {root}")
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(root)
            cand = os.path.join(root, "02_backend")
            if _has_backend(cand):
                print(f"[resolve] extracted backend at {cand}")
                return root, cand, os.path.join(root, "03_frontend")

    # 4) Last resort: walk the project tree to locate app.py.
    for root in _candidate_roots():
        for dirpath, dirnames, filenames in os.walk(root):
            # don't descend into noise
            dirnames[:] = [d for d in dirnames if d not in
                           (".git", "node_modules", "__pycache__", ".cache")]
            if "app.py" in filenames and dirpath.endswith("02_backend"):
                print(f"[resolve] located backend via walk: {dirpath}")
                base = os.path.dirname(dirpath)
                return base, dirpath, os.path.join(base, "03_frontend")

    raise FileNotFoundError(
        "Could not locate 02_backend (with app.py) or backend.zip under any of: "
        + ", ".join(_candidate_roots())
    )


BASE_DIR, BACKEND_DIR, FRONTEND_DIR = resolve_dirs()


def run_backend():
    env = {**os.environ, "PYTHONPATH": BACKEND_DIR}
    workers = int(os.getenv("UVICORN_WORKERS", "2")) if IS_CLOUDERA else 1
    cmd = [
        PYTHON, "-m", "uvicorn", "app:app",
        "--host", "0.0.0.0",
        "--port", str(APP_PORT),
        "--workers", str(workers),
        "--log-level", "info",
    ]
    # --reload only in local dev (incompatible with multiple workers)
    if not IS_CLOUDERA:
        cmd.append("--reload")
    print(f"Starting uvicorn on port {APP_PORT} with {workers} worker(s)")
    subprocess.run(cmd, cwd=BACKEND_DIR, env=env)


def run_frontend():
    if IS_CLOUDERA:
        dist = os.path.join(FRONTEND_DIR, "dist")
        node_modules = os.path.join(FRONTEND_DIR, "node_modules")
        # Install Python deps
        req = os.path.join(BASE_DIR, "02_backend", "requirements.txt")
        if os.path.exists(req):
            print("Installing Python dependencies...")
            subprocess.run([PYTHON, "-m", "pip", "install", "-r", req, "-q"], check=False)

        # Find npm — try common locations
        npm = None
        for candidate in [os.getenv("NPM_BIN", ""), "/usr/bin/npm", "/usr/local/bin/npm",
                          "/opt/homebrew/bin/npm", "npm"]:
            if not candidate:
                continue
            try:
                if subprocess.run([candidate, "--version"], capture_output=True).returncode == 0:
                    npm = candidate
                    break
            except FileNotFoundError:
                continue

        if npm:
            if not os.path.isdir(node_modules):
                print(f"Installing npm deps with {npm}...")
                subprocess.run([npm, "install", "--silent"], cwd=FRONTEND_DIR, check=False)
            if not os.path.isdir(dist):
                print("Building React frontend...")
                subprocess.run([npm, "run", "build"], cwd=FRONTEND_DIR, check=False)
            else:
                print("Frontend dist/ already built.")
        else:
            print("npm not found — skipping frontend build. API-only mode.")

        print(f"Frontend served by FastAPI on port {APP_PORT}")
    else:
        time.sleep(1.5)
        subprocess.run(["npm", "run", "dev"], cwd=FRONTEND_DIR)


# Run unconditionally — CML PBJ kernel doesn't set __name__ == "__main__"
if True:
    print(f"Starting Cloudera AI Agents {'(Cloudera CAI mode)' if IS_CLOUDERA else '(local dev mode)'}")

    if IS_CLOUDERA:
        # Backend/frontend dirs already resolved (zip extracted if needed)
        # by resolve_dirs() at import time.
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
