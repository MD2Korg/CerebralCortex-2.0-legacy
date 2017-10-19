from functools import wraps
from datetime import datetime
def calculate_time(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        st = datetime.now()
        r = f(*args, **kwargs)
        et = datetime.now()
        print("Took ", et-st, " to process.")
        return r
    return wrapped
