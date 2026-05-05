import shutil
import subprocess
from pathlib import Path

import pytest

WEB_DIR = Path(__file__).parent.parent / "web"


@pytest.mark.skipif(shutil.which("npm") is None, reason="npm not installed")
def test_web_build():
    if not (WEB_DIR / "node_modules").exists():
        subprocess.run(["npm", "install"], cwd=WEB_DIR, check=True, capture_output=True)

    result = subprocess.run(
        ["npm", "run", "build"],
        cwd=WEB_DIR,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"Web build failed:\n{result.stderr}"
