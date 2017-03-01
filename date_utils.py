from datetime import date, timedelta, datetime

def int2date(n):
    """
    This function converts a number of days since Jan 1st 1970 <n> to a date.
    """
    return date(1970,1,1)+timedelta(days=n)

def date2int(d):
    """
    This function converts a date <d> to number of days since Jan 1st 1970.
    """
    return (d-date(1970,1,1)).days

def str2date(s, f="%Y%m%d"):
    """
    This function converts a string <s> in the format <f> to a date.
    """
    return datetime.strptime(s, f).date()
