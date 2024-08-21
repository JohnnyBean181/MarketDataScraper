from unittest.mock import inplace
import eom_sse, eom_szse, eom_bse
from datetime import date, timedelta
from retrieve_data import get_eom


def get_last_day_of_last_month():
    today = date.today()
    first_day_of_this_month = today.replace(day=1)
    last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
    return last_day_of_last_month


def main():
    # eom_sse.execute()
    # eom_szse.execute()
    # eom_bse.execute()
    last_day = date.today()
    get_eom(last_day)

if __name__ == "__main__":
    main()
