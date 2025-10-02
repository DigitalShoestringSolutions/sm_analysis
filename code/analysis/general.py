from pandas import DataFrame
from math import isnan

def period_over_period_buckets(
    series_function,
    bucket_function,
    timestamp_key="_time",
    value_key="_value",
):

    async def wrapped(data_frame: DataFrame):
        """
        dataset is an iterable (such as a list)
        series_function and bucket_function must return a tuple of the form (sortable_key, label) when applied to an entries timestamp

        output is of the format:
        {
            "buckets":[<bucket_labels>],
            "series:{
                <series_label>:[<bucket_values>]
            }
        }
        """
        grouped_data = {}
        series = []
        buckets = []
        for index, row in data_frame.iterrows():
            if isnan(row[value_key]):
                continue
            # get keys
            bucket_key = bucket_function(row[timestamp_key])
            series_key = series_function(row[timestamp_key])

            # store value in output structure
            # create next nesting level if not present
            if grouped_data.get(bucket_key) is None:
                grouped_data[bucket_key] = {}
            grouped_data[bucket_key][series_key] = row[value_key]

            if series_key not in series:
                series.append(series_key)

            if bucket_key not in buckets:
                buckets.append(bucket_key)

        # sort the buckets and series into ascending order
        sorted_series = sorted(series, key=lambda key: key[0])
        sorted_buckets = sorted(buckets, key=lambda key: key[0])

        # create 2D array keyed first on bucket then series. Pulling value from the dataset (if not present return value=None)
        formated_output = {
            "buckets": [bucket_key[1] for bucket_key in sorted_buckets],
            "series": {
                series_key[1]: [
                    (
                        grouped_data[bucket_key].get(series_key, None)
                        if grouped_data.get(bucket_key) is not None
                        else None
                    )
                    for bucket_key in sorted_buckets
                ]
                for series_key in sorted_series
            },
        }

        return formated_output
    return wrapped
