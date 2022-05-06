import pandas as pd

class account:
    """Object for storing an account.

    Arguments:
        file: str.
            The .csv file used to import the account data.
        from_date: str
            The date on which the account begins. (YYYY-MM-DD)
        to_date: str
            The date when the account ends. (YYYY-MM-DD)

    Optional arguments:
        format: list = ['Date', 'Reference', 'Value']
            A list of strings specifying the columns of a .csv file.
            Konto will use the 'Date' column to order, the 'Reference' value to sort and the 'Value' to process the transaction quantities.
        dayfirst: bool = True
            Whether or not the dates in the .csv are written with the day first.
        eval_on_value: list = []
            A list of references which should be tagged according to both rerence and value, not just reference. E.g. payments to paypal might fall under different categories and the price will be used to infer the tags.
    """
    def __init__(self, file, from_date, to_date, format = ['Date', 'Reference', 'Value'], day_first = True, eval_on_value = []):
        # Error checking and importing
        if not(isinstance(file, str)):
            raise TypeError('file should be of type str.')

        try:
            self.from_date = pd.to_datetime(from_date)
        except TypeError:
            print("from_date couldn't parse. Try using a string YYYY-MM-DD.")

        try:
            self.from_date = pd.to_datetime(from_date)
        except TypeError:
            print("to_date couldn't parse. Try using a string YYYY-MM-DD.")

        # Get the DataFrame from the
        self.data = pd.read_csv(file, names = format, parse_dates = ['Date'], dayfirst = day_first)

        # Initialise the datetime index.
        self.data.set_index('Date', inplace = True)

        # Make sure data types in the frame are correct.
        self.data['Value'] = self.data['Value'].str.replace(',','')
        self.data = self.data.astype({'Reference': 'str', 'Value': 'float64'})

        # Select only references and values.
        self.sort_data = self.data[['Reference', 'Value']]

        # Initialise the tagging dictionaries.
        self.tagdict = {}
        self.tagdict_on_price = {}

        # Has the data been tagged?
        self.data_tagged = False

    # Explain how to print the data.
    def __str__(self):
        return self.data.__str__()

    def tag(self,
            tagging_dict = None, tagging_dict_on_price = None):
        """Creates a new column 'tags' on the Account.data, allowing the user to summarise the account data. User will be prompted to enter tags, separated by a space, in order to tag the data. E.g.:
        2022-12-25: PAYPAL DD, -125.00
        gift online amazon

        Optional arguments:
            tagging_dict: dict
                Alternative dictionary to use to map references to lists of tags.
            tagging_dict_on_price: dict
                Alternative dictionary to use to map references and values to lists of tags.
        """
        # Get the pre-defined tagging dictionaries if needed.
        if tagging_dict is None:
            tagging_dict = self.tagdict
        if tagging_dict_on_price is None:
            tagging_dict_on_price = self.tagdict_on_price

        # Check the types.
        if not(isinstance(tagging_dict, dict)):
            raise TypeError('tagging_dict must be of type dict.')
        elif not(isinstance(tagging_dict_on_price, dict)):
            raise TypeError('tagging_dict_on_price must be of type dict.')

        # Update the tagging dictionaries.
        self.tagdict = tagging_dict
        self.tagdict_on_price = tagging_dict_on_price

        # Initialise the column of tag lists.
        tagcolumn = []

        # Prompt the user to label unlabelled data.
        for i, row in self.sort_data.iterrows():
            tags_to_append = []
            if row[0] in self.tagdict_on_price:
                if (row[0], row[1]) in self.tagdict_on_price:
                    tags_to_append = self.tagdict_on_price[(row[0], row[1])]
                else:
                    print(str(i) + ", " + str(row[0]) + ", " + str(row[1]) +    ":")
                    tags_to_append = input().split()
                    tagdict_on_price[(row[0], row[1])] = tags_to_append
            else:
                if row[0] in self.tagdict:
                    tags_to_append = self.tagdict[row[0]]
                else:
                    print(str(i) + ", " + str(row[0]) + ", " + str(row[1])  + ":")
                    tags_to_append = input().split()
                    self.tagdict[row[0]] = tags_to_append
            tagcolumn.append(tags_to_append)

        # Update data_tagged.
        self.data_tagged = True

        # Send to a column.
        self.data['Tags'] = tagcolumn

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
            raise TagError('data has not yet been tagged. Please tag before using TagResample.')

        self.last_resample = self.data[self.data['Tags'].apply(lambda tlist: set(list_of_tags).issubset(set(tlist)))].resample(frequency)['Value']

        return self.last_resample
