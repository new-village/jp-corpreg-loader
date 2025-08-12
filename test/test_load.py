""" test_load.py
"""
import unittest
import json
import pandas as pd
from src.load import load, read_csv

class TestLoadFunctions(unittest.TestCase):
    def setUp(self):
        """Load expected columns from configuration file before each test."""
        with open('src/config/header.json', 'r') as file:
            self.expected_columns = json.load(file)

    def test_load(self):
        """Test the load function with 'Shimane' prefecture."""
        result = load(prefecture='Shimane')

        # Validate results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), self.expected_columns)
        self.assertGreater(len(result), 20000)

    def test_read_csv(self):
        """Test the read_csv function with a specific CSV file."""
        result = read_csv('./test/data/31_tottori_test_20240329.csv')

        # Validate results
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), self.expected_columns)
        self.assertEqual(len(result), 5)
        self.assertEqual(result.iloc[0]['corporate_number'], '1000013050238')
        self.assertEqual(result.iloc[1]['name'], '島田商事株式会社')

if __name__ == '__main__':
    unittest.main()
