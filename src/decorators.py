import functools
import logging
import os
import smtplib
import time
import traceback
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from email.mime.text import MIMEText

import psutil
from psutil._common import bytes2human

logging.basicConfig(level=logging.INFO)


def log_execution(func):
    """
    Decorator that logs the start and end of a function execution.

    Args:
        func (function): The function to be decorated.

    Returns:
        function: The decorated function.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """
        Wrapper function that logs the start and end of a function execution.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            The result of the decorated function.
        """
        # Log the start of the function execution
        logging.info(f"Executing {func.__qualname__} at {datetime.now()}")

        # Call the decorated function
        result = func(*args, **kwargs)

        # Log the end of the function execution
        logging.info(f"Finished executing {func.__qualname__}")
        return result

    return wrapper


def retry(max_tries=3, delay_seconds=1):
    """
    Decorator that retries a function a specified number of times.

    Args:
        max_tries (int): Max number of times the function should be retried.
        delay_seconds (int): Time to wait between retries in seconds.

    Returns:
        function: The decorated function.
    """

    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            """
            Wrapper function that retries a function a specified
            number of times.

            Args:
                *args: Variable length argument list.
                **kwargs: Arbitrary keyword arguments.

            Returns:
                The result of the decorated function.
            """
            tries = 0
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    tries += 1
                    if tries == max_tries:
                        raise e
                    logging.info(
                        f"{func.__qualname__} failed. \
                            Retrying in {delay_seconds} seconds... \
                                [{tries}/{max_tries}]"
                    )
                    time.sleep(delay_seconds)

        return wrapper_retry

    return decorator_retry


def timer(func):
    """
    Decorator that times the execution of a function.

    Args:
        func (function): The function to be decorated.

    Returns:
        function: The decorated function.
    """

    def wrapper(*args, **kwargs):
        """
        Wrapper function that times the execution of a function.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            tuple: The result of the decorated function and the duration of
                execution in seconds.
        """
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        duration = f"{end - start:.4f}"
        logging.info(f"{func.__qualname__} took {duration} seconds")
        return result, duration

    return wrapper


def email_on_failure(sender_email, password, recipient_email):
    """
    Decorator that sends an email with error details if a function fails.

    Args:
        sender_email (str): The email address of the sender.
        password (str): The password of the sender's email account.
        recipient_email (str): The email address of the recipient.

    Returns:
        function: The decorated function.
    """

    def decorator(func):
        """
        Wrapper function that sends an email with error details if a
        function fails.

        Args:
            func (function): The function to be decorated.

        Returns:
            function: The decorated function.
        """

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                err_msg = f"Error: \
                    {str(e)}\n\nTraceback:\n{traceback.format_exc()}"

                message = MIMEText(err_msg)
                message["Subject"] = f"Error: {func.__qualname__} failed"
                message["From"] = sender_email
                message["To"] = recipient_email

                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                    smtp.login(sender_email, password)
                    smtp.sendmail(
                        sender_email,
                        recipient_email,
                        message.as_string(),
                    )

                raise
            return wrapper

        return decorator


def threaded(func):
    """
    Decorator that runs a function in parallel using threads.

    Args:
        func (function): The function to be decorated.

    Returns:
        function: The decorated function.
    """

    def wrapper(*args, **kwargs):
        """
        Wrapper function that runs a function in parallel using threads.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            list: The results of the decorated function.
        """
        results = {}

        with ThreadPoolExecutor(max_workers=None) as executor:
            futures = {
                executor.submit(func, [i]): idx for idx, i in enumerate(args[0])  # noqa
            }
            tasks = len(futures)
            tenth = round(tasks / 10)
            logging.info(f"Formed pool of {tasks} tasks")

            for idx, future in enumerate(as_completed(futures)):
                i = futures[future]
                try:
                    data = future.result()
                    if len(data) == 1:
                        data = data[0]
                    results[i] = data
                except Exception as exc:
                    logging.exception(
                        f"{args[0][i]} generated an exception: {exc}"
                    )  # noqa

                if tenth != 0 and idx % tenth == 0:
                    logging.info(
                        f"Processed {idx // tenth * 10} of {tasks} tasks"
                    )  # noqa

        final = []
        for k, v in sorted(results.items()):
            final.append(v)

        return final

    return wrapper


def memory(func):
    """
    Decorator function to measure memory usage of a function.

    Args:
        func (function): The function to be wrapped.

    Returns:
        function: The wrapped function.
    """

    def wrapper(*args, **kwargs):
        """
        Wrapper function to measure memory usage before and after calling
        the decorated function.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            object: The result of the decorated function.
        """
        process = psutil.Process(os.getpid())
        start_mem = process.memory_info().rss
        result = func(*args, **kwargs)
        end_mem = process.memory_info().rss
        mem_delta = bytes2human(end_mem - start_mem)
        logging.info(f"{func.__qualname__} used {mem_delta} memory")
        return result

    return wrapper
