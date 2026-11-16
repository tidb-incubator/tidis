"""Preserve the E2E entry point while making assertion failures fail CI."""
from pathlib import Path
import runpy
import sys
import unittest


class CheckedRunner(unittest.TextTestRunner):
    def run(self, suite):
        result = super().run(suite)
        if not result.wasSuccessful():
            raise SystemExit(1)
        return result


if __name__ == "__main__":
    test_dir = Path(__file__).resolve().parents[2] / "test"
    sys.path.insert(0, str(test_dir))
    unittest.TextTestRunner = CheckedRunner
    runpy.run_path(str(test_dir / "test_helper.py"), run_name="__main__")
