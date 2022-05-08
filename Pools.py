import pandas as pd
import numpy as np

class konto:
    """Object for storing multiple accounts summarised together. Useful for visualising an individual's cashflow across multiple accounts.

    Arguments:
        account_list = []: list
            A list of konto accounts or konto funds which should be summarised.

    Transfers of money make cashflow confusing, so by tagging both ends of a transfer as 'transfer', they will cancel out overall, leading to a clearer picture of pool behaviour.
    """
    def __init__(self, account_list = []):
        self.account_list = account_list

        # Concatenate data from all listed accounts.
        self.data = pd.concat([acc.data for acc in account_list]).sort_values('Date')

        # Note whether or not the data has been tagged so far.
        self.data_tagged = np.all([lambda acc: acc.data_tagged for acc in self.account_list])

        # Update the current konto value.
        self.value = sum([acc.value for acc in self.account_list])

    def resample(self, frequency = 'M'):
        """Resample the data using the given frequency.

        Optional arguments:
            frequency: str
                The timecode used to direct the resampling. E.g.
                'M' is month, 'D' is day, 'W' is week etc.

                Uses pandas.DataFrame.resample()

        Returns: pandas.SeriesGroupBy object
            Use .sum() or .aggregate() or others to dissect.
        """
        self.last_resample = self.data.resample(frequency)
        return self.last_resample

    def tagresample(self, list_of_tags = [], frequency = 'M'):
        """Filter the account according to a list of tags and resample based on frequency.

        Optional arguments:
            list_of_tags: list
                List of strings of tags required in the resample.
            frequency: str
                Code to direct the resampling. For example,
                'Y' will filter by year, 'M' by month, 'W', by day, 'D' by day etc. Relies on pd.DataFrame.resample().

        Returns:
            pandas.core.groupby.generic.SeriesGroupBy object. Use .sum(), .mean(), .aggregate(), .describe() to get required information.

        For example, to get account data resampled summarising the total purchases tagged 'online' across months, call:

        Account.TagResample(['online'], 'M').sum()
        """
        # Instance checks.
        if not(isinstance(list_of_tags, list)):
            raise TypeError('list_of_tags must be a list of strings.')
        elif not(isinstance(frequency, str)):
            raise TypeError('frequency must be a string representing a timecode.')
        elif not(self.data_tagged):
            raise TagError('data has not yet been tagged. Please tag all constituent accounts before using TagResample.')

        self.last_resample = self.data[self.data['Tags'].apply(lambda tlist: set(list_of_tags).issubset(set(tlist)))].resample(frequency)['Value']

        return self.last_resample

    def save(self, file_name):
        """Save the konto to a file so that it can be accessed again later.

        Arguments:
            file_name: str
                File name to write to.
        """
        with open(file_name, 'wb') as file:
            pickle.dump(self, file)
