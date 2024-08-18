from src.exceptions import handle_exception
from src.pipeline.pipeline import Pipeline


def main():
    try:
        pipeline = Pipeline()
        pipeline.generate_dag().run("2023-04-31")
    except Exception as err:
        handle_exception(err, "Something went wrong")


def hello():
    print("hello")


def test():
    params = {"callback_func": hello}
    print(params)


if __name__ == "__main__":
    main()
    # test()
