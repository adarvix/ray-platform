import os
import sys
from pathlib import Path

APP = str(Path(__file__).parents[1] / "app")
sys.path.insert(0, APP)
# Ray worker processes are separate interpreters: they need the app on PYTHONPATH too.
os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [APP, os.environ.get("PYTHONPATH")]))
