"""
Shut up linter
"""

import json
import sys

if __name__ == '__main__':
    with open('../resistance.json', 'r', encoding="utf-8") as file:
        data = json.load(file)

        print("article;resistance")
        for r in data:
            article = r[0]
            resistance = r[1]

            print(f"{article};{resistance}")

        sys.exit(0)
    sys.exit(-1)
