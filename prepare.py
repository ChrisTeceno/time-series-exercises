import pandas as pd
import acquire
import os


def prep_store_data(df=acquire.join_data(), usecache=True):
    """Prepare the data for analysis, used cache if available"""
    filename = "prepped_store.csv"
    if usecache and os.path.exists(filename):
        print("Using cached data")
        return pd.read_csv(filename)
    # convert sale_date to datetime
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    # sort sale_date and use as index
    df.sort_values(by="sale_date", inplace=True)
    df.set_index("sale_date", inplace=True)
    # add month and day of week columns
    df["month"] = df.index.month
    df["day_of_week"] = df.index.dayofweek
    # change sale_amount to quantity_sold
    df.rename(columns={"sale_amount": "quantity_sold"}, inplace=True)
    # make column for sales_total
    df["sales_total"] = df["quantity_sold"] * df["item_price"]
    return df


def prep_german_power(ops=acquire.get_german_power()):
    """Prepare the data for analysis"""
    # make columns lower case
    ops.columns = [x.lower() for x in ops.columns]
    # change wind+solar to wind_and_solar
    ops.rename(columns={"wind+solar": "wind_and_solar"}, inplace=True)
    # convert date to datetime
    ops["date"] = pd.to_datetime(ops["date"])
    # sort date and use as index
    ops.sort_values(by="date", inplace=True)
    ops.set_index("date", inplace=True)
    # add month and year columns
    ops["month"] = ops.index.month
    ops["year"] = ops.index.year
    # fill nulls with 0 assuming the data is null due to 0 production
    ops.fillna(0, inplace=True)
    return ops
