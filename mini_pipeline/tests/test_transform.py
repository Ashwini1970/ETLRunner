# Tests for transform module
import unittest
import pandas as pd
from pipeline import transform

class TestTransformFunctions(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [35, 28, 40],
            "temp": [98.6, 99.1, 97.9]
        })

    def test_filter_rows_greater_than(self):
        step = {"type": "filter", "column": "age", "operator": ">", "value": 30}
        result = transform.filter_rows(self.df, step)
        self.assertEqual(len(result), 2)
        self.assertTrue((result["age"] > 30).all())

    def test_add_column(self):
        step = {"type": "add_column", "name": "status", "value": "active"}
        result = transform.add_column(self.df.copy(), step)
        self.assertIn("status", result.columns)
        self.assertTrue((result["status"] == "active").all())

    def test_drop_column(self):
        step = {"type": "drop_column", "name": "temp"}
        result = transform.drop_column(self.df.copy(), step)
        self.assertNotIn("temp", result.columns)

    def test_drop_column_not_exist(self):
        step = {"type": "drop_column", "name": "xyz"}
        result = transform.drop_column(self.df.copy(), step)
        self.assertEqual(list(result.columns), list(self.df.columns))  # no change

    def test_map_column(self):
        step = {"type": "map_column", "column": "name", "mapping": {"Alice": "A", "Charlie": "C"}}
        result = transform.map_column(self.df.copy(), step)
        self.assertEqual(result.loc[result["name"] == "A"].shape[0], 1)
        self.assertEqual(result.loc[result["name"] == "C"].shape[0], 1)
        self.assertIn("Bob", result["name"].values)

if __name__ == "__main__":
    unittest.main()
