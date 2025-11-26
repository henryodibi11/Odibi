import unittest

import pandas as pd

from odibi.config import SchemaPolicyConfig
from odibi.engine.pandas_engine import PandasEngine


class TestSchemaEvolution(unittest.TestCase):
    def setUp(self):
        self.engine = PandasEngine()
        self.target_schema = {"id": "int64", "name": "object"}

    def test_enforce_drop_extra(self):
        """Test ENFORCE mode with IGNORE new columns (default for Enforce in implementation)."""
        # Extra column 'age'
        df = pd.DataFrame({"id": [2], "name": ["B"], "age": [30]})
        # Explicitly set IGNORE to be sure, though logic defaults to it if not FAIL
        policy = SchemaPolicyConfig(mode="enforce", on_new_columns="ignore")

        res = self.engine.harmonize_schema(df, self.target_schema, policy)

        self.assertEqual(list(res.columns), ["id", "name"])
        self.assertTrue("age" not in res.columns)

    def test_enforce_add_missing(self):
        """Test adding missing columns as null."""
        # Missing column 'name'
        df = pd.DataFrame({"id": [2]})
        policy = SchemaPolicyConfig(mode="enforce", on_missing_columns="fill_null")

        res = self.engine.harmonize_schema(df, self.target_schema, policy)

        self.assertEqual(list(res.columns), ["id", "name"])
        self.assertTrue(pd.isna(res.iloc[0]["name"]))

    def test_evolve_add_nullable(self):
        """Test EVOLVE mode allowing new columns."""
        # Extra column 'age'
        df = pd.DataFrame({"id": [2], "name": ["B"], "age": [30]})
        policy = SchemaPolicyConfig(mode="evolve", on_new_columns="add_nullable")

        res = self.engine.harmonize_schema(df, self.target_schema, policy)

        self.assertTrue("age" in res.columns)
        self.assertEqual(res.iloc[0]["age"], 30)
        # Should also have original columns
        self.assertTrue("id" in res.columns)
        self.assertTrue("name" in res.columns)

    def test_fail_on_new(self):
        """Test FAIL on new columns."""
        df = pd.DataFrame({"id": [2], "name": ["B"], "age": [30]})
        policy = SchemaPolicyConfig(mode="enforce", on_new_columns="fail")

        with self.assertRaises(ValueError) as cm:
            self.engine.harmonize_schema(df, self.target_schema, policy)
        self.assertIn("New columns", str(cm.exception))

    def test_fail_on_missing(self):
        """Test FAIL on missing columns."""
        df = pd.DataFrame({"id": [2]})
        policy = SchemaPolicyConfig(mode="enforce", on_missing_columns="fail")

        with self.assertRaises(ValueError) as cm:
            self.engine.harmonize_schema(df, self.target_schema, policy)
        self.assertIn("Missing columns", str(cm.exception))

    def test_evolve_add_missing(self):
        """Test EVOLVE mode with missing columns (should still add them)."""
        # Missing 'name', New 'age'
        df = pd.DataFrame({"id": [2], "age": [30]})
        policy = SchemaPolicyConfig(mode="evolve", on_new_columns="add_nullable")

        res = self.engine.harmonize_schema(df, self.target_schema, policy)

        # Check missing added
        self.assertTrue("name" in res.columns)
        self.assertTrue(pd.isna(res.iloc[0]["name"]))
        # Check new kept
        self.assertTrue("age" in res.columns)


if __name__ == "__main__":
    unittest.main()
