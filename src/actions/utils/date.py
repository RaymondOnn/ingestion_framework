from datetime import datetime


def is_date(date_str: str):
    try:
        #date_text = f"{year}-{month}-{day}"
        if date_str != datetime.strptime(date_str, "%Y-%m-%d") \
        .strftime('%Y-%m-%d'):
            raise ValueError
        return True
    except ValueError:
        return False