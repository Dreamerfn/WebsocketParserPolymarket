import tempfile
import unittest
from pathlib import Path

from collector.config import load_config


class TestConfigValidation(unittest.TestCase):
    def test_invalid_binance_levels_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.yaml"
            path.write_text("binance:\n  levels: 25\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_config(path)


if __name__ == "__main__":
    unittest.main()
