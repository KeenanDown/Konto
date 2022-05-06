import pandas as pd
import numpy as np
import pickle
import json

from Konto.Pools import konto

def load(file_name):
    """Load an account or a konto.

    Arguments:
        file_name: str
            The name of the file to open.
    """
    with open(file_name, 'rb') as file:
        account_to_return = pickle.load(file)

    return account_to_return

def bulk_load_accounts(list_of_account_filenames):
    """Bulk load a list of accounts into a konto.

    Arguments:
        list_of_account_filenames: list
            List of filenames for the saved accounts.

    Returns:
        konto: A konto consisting of the accounts.
    """
    return konto([load(account_string) for account_string in list_of_account_filenames])
