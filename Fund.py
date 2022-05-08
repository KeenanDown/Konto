import pandas as pd
import numpy as np

class fund:
    """A konto-compatible class for storing a simple asset.

    Arguments:
        value: float
            Value of the fund at the given time.
        time: str
            Date of the fund value.
    """

    def __init__(self, value, time):
        # Save the value.
        self.value = value

        # Save the date.
        self.date = pd.to_datetime(time)
