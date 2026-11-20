#!/usr/bin/env python3
"""Mutation fixtures for the wasm expected-failure gate."""
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("gate", Path(__file__).with_name("check-d1-wasm-blocking.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


def fixture(errors, status=101, summary=None):
    records = [{"reason": "compiler-message", "message": {"level": "error", "message": error, "children": []}} for error in errors]
    records.append({"reason": "build-finished", "success": status == 0})
    return subprocess.CompletedProcess([], status, "\n".join(map(json.dumps, records)),
                                       gate.SUMMARY if summary is None else summary)


class GateTests(unittest.TestCase):
    def test_expected_failure(self):
        self.assertTrue(gate.verify(fixture([gate.EXPECTED])))

    def test_unrelated_error(self):
        self.assertFalse(gate.verify(fixture([gate.EXPECTED, "unrelated error"])))

    def test_unexpected_success(self):
        self.assertFalse(gate.verify(fixture([gate.EXPECTED], status=0)))

    def test_extra_cargo_error(self):
        self.assertFalse(gate.verify(fixture([gate.EXPECTED], summary=gate.SUMMARY + "\nerror: unrelated")))

    def test_missing_or_duplicate_summary(self):
        for summary in ("", gate.SUMMARY + "\n" + gate.SUMMARY):
            self.assertFalse(gate.verify(fixture([gate.EXPECTED], summary=summary)))

    def test_malformed_output(self):
        result = fixture([gate.EXPECTED])
        result.stdout += "\nnot json"
        self.assertFalse(gate.verify(result))

    def test_no_raw_output(self):
        result = fixture([gate.EXPECTED, "secret marker"])
        with patch.object(gate.subprocess, "run", return_value=result), patch("sys.stderr") as stderr:
            self.assertEqual(gate.main(), 1)
            self.assertNotIn("secret marker", str(stderr.write.call_args_list))


if __name__ == "__main__":
    unittest.main()
