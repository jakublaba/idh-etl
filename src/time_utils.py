import enum

MONTH_MAP = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


class Season(enum.Enum):
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    WINTER = "winter"


def get_season(month_num: int) -> Season:
    if month_num in [12, 1, 2]:
        return Season.WINTER
    elif month_num in [3, 4, 5]:
        return Season.SPRING
    elif month_num in [6, 7, 8]:
        return Season.SUMMER
    elif month_num in [9, 10, 11]:
        return Season.AUTUMN
    else:
        raise ValueError("month_num must be in 1..12")


class TimeOfDay(enum.Enum):
    MORNING = "morning"
    MIDDAY = "midday"
    AFTERNOON = "afternoon"
    EVENING = "evening"
    NIGHT = "night"


def get_time_of_day(hour: int) -> TimeOfDay:
    if hour in range(6, 10):
        return TimeOfDay.MORNING
    elif hour in range(10, 14):
        return TimeOfDay.MIDDAY
    elif hour in range(14, 18):
        return TimeOfDay.AFTERNOON
    elif hour in range(18, 23):
        return TimeOfDay.EVENING
    else:
        return TimeOfDay.NIGHT
