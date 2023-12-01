# tests/test_slack_data_loader.py
import unittest
import pandas as pd
from your_project.slack_data_loader import SlackDataLoader

class TestSlackDataLoader(unittest.TestCase):
    def setUp(self):
        # Initialize necessary objects or load test data
        self.data_loader = SlackDataLoader()

    def test_data_loader_columns(self):
        # Get the DataFrame using the loader
        df = self.data_loader.get_data()

        # Define the expected columns
        expected_columns = ['column1', 'column2']

        # Check if the columns match the expected ones
        self.assertListEqual(list(df.columns), expected_columns)

if __name__ == '__main__':
    unittest.main()
