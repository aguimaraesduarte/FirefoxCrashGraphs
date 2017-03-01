import numpy as np

def geometric_mean(pandas_col):
    """
    This function returns the geometric mean of a specified column from a pandas dataframe.
    The geometric mean is the exponential of the average of the logs of the data points. We add 1 to all logs because of potential 0s.
    This 1 is then removed from the final exponential.

    @params:
        pandas_col: [Series] column of the pandas dataframe to calculate the geometric mean of
    """
    return np.exp(np.log(pandas_col + 1).mean()) - 1
