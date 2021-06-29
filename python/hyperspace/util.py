import warnings
from functools import wraps

def deprecated(msg):
    def wrapper1(func):
        @wraps(func)
        def wrapper2(*args, **kwargs):
            warnings.warn(msg, DeprecationWarning)
            return func(*args, **kwargs)
        return wrapper2
    return wrapper1
