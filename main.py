
import sys
from pprint import pprint

from trys.graph import Pipeline



def main():
    pipeline = Pipeline()
    pipeline.generate_dag().run("2023-04-31")


if __name__ == "__main__":
    sys.exit(main())
