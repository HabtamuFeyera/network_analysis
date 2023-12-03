# config.py

from __future__ import print_function
import argparse

parser = argparse.ArgumentParser(description='Slack Data Analysis Configuration')
parser.add_argument('--output', type=str, default='slack_data.csv',
                    help='Filename to write analysis output in CSV format')
parser.add_argument('--path', required=True, type=str, help='Directory where Slack data reside')
parser.add_argument('--channel', type=str, default='', help='Which channel to parse')
parser.add_argument('--userfile', type=str, default='users.json', help='Users profile information')

cfg = parser.parse_args()

if __name__ == "__main__":
    #Placeholder: Add any script-specific code here if required
    print(cfg)
