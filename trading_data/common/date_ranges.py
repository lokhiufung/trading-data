
from datetime import datetime, timedelta
from typing import List


def get_dates(start_date: str, end_date: str) -> List[str]:
    # Convert the string dates to datetime objects
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    # Calculate the number of days between the two dates
    num_days = (end - start).days
    
    # Create a list of dates in the range
    date_list = [(start + timedelta(days=x)).strftime("%Y-%m-%d") for x in range(num_days + 1)]
    
    return date_list



def get_months(start_date: str, end_date: str) -> List[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    result = []

    while start <= end:
        result.append(start.strftime("%Y-%m"))
        # Move to the next month
        next_month = start.replace(day=28) + timedelta(days=4)  # ensures we move to the next month
        start = next_month - timedelta(days=next_month.day - 1)  # set to first day of the next month
    return result

