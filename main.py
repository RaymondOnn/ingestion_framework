from pprint import pprint
from src.exceptions import handle_exception
from src.pipeline.pipeline import Pipeline


# def main():
#     try:
#         Pipeline.from_yaml("examples/Loading csv into Postgres/job.yaml") \
#             .run("2023-04-31")
#     except Exception as err:
#         handle_exception(err, "Something went wrong")


# def hello():
#     print("hello")


def test():
    Pipeline.from_yaml("examples/Loading csv into Postgres/job.yaml") \
            .run("2023-04-31")


if __name__ == "__main__":
    # main()
    test()
