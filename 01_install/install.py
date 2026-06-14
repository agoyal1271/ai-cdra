"""
Installer for Cloudera AI Agents.
Installs Python dependencies and npm packages.
"""
import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
BACKEND_DIR = os.path.join(BASE_DIR, "02_backend")
FRONTEND_DIR = os.path.join(BASE_DIR, "03_frontend")


def run(cmd, cwd=None, check=True):
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, cwd=cwd, check=check)


def install_python_deps():
    print("\n=== Installing Python dependencies ===")
    run([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
    run([sys.executable, "-m", "pip", "install", "-r", os.path.join(BACKEND_DIR, "requirements.txt")])


def install_node_deps():
    print("\n=== Installing Node.js dependencies ===")
    run(["npm", "install"], cwd=FRONTEND_DIR)


if __name__ == "__main__":
    install_python_deps()
    install_node_deps()
    print("\n✓ Installation complete. Run: python launch.py")
