import pandas as pd
import requests
import os


def get_items(usecache=True):
    """IF usecache is True, then the function will return the cached data. 
    Otherwise, it will make a request to the API and return the data."""
    filename = "items.csv"
    if usecache and os.path.exists(filename):
        print("Using cached data for items")
        return pd.read_csv(filename)
    print("Making request to API")
    domain = "https://api.data.codeup.com/"
    endpoint = "/api/v1/items"
    items = []
    url = domain + endpoint
    response = requests.get(url)
    data = response.json()
    items.extend(data["payload"]["items"])
    while data["payload"]["next_page"]:
        url = domain + data["payload"]["next_page"]
        response = requests.get(url)
        data = response.json()
        items.extend(data["payload"]["items"])
    df = pd.DataFrame(items)
    # df.set_index('item_id', inplace=True)
    print("Writing data to csv")
    df.to_csv(filename, index=False)
    return pd.DataFrame(df)


def get_stores(usecache=True):
    """IF usecache is True, then the function will return the cached data. 
    Otherwise, it will make a request to the API and return the data."""
    filename = "stores.csv"
    if usecache and os.path.exists(filename):
        print("Using cached data for stores")
        return pd.read_csv(filename)
    print("Making request to API")
    domain = "https://api.data.codeup.com/"
    endpoint = "/api/v1/stores"
    stores = []
    url = domain + endpoint
    response = requests.get(url)
    data = response.json()
    stores.extend(data["payload"]["stores"])
    while data["payload"]["next_page"]:
        url = domain + data["payload"]["next_page"]
        response = requests.get(url)
        data = response.json()
        stores.extend(data["payload"]["stores"])
    df = pd.DataFrame(stores)
    # df.set_index('store_id', inplace=True)
    print("Writing data to csv")
    df.to_csv(filename, index=False)
    return pd.DataFrame(df)


def get_sales(usecache=True):
    """IF usecache is True, then the function will return the cached data. 
    Otherwise, it will make a request to the API and return the data."""
    filename = "sales.csv"
    if usecache and os.path.exists(filename):
        print("Using cached data sales")
        return pd.read_csv(filename)
    print("Making request to API")
    domain = "https://api.data.codeup.com/"
    endpoint = "/api/v1/sales"
    sales = []
    url = domain + endpoint
    response = requests.get(url)
    data = response.json()
    sales.extend(data["payload"]["sales"])
    while data["payload"]["next_page"]:
        url = domain + data["payload"]["next_page"]
        response = requests.get(url)
        data = response.json()
        sales.extend(data["payload"]["sales"])
    df = pd.DataFrame(sales)
    # df.set_index('sale_id', inplace=True)
    print("Writing data to csv")
    df.to_csv(filename, index=False)
    return pd.DataFrame(df)


def join_data(usecache=True):
    """Join the data together or use cached data"""
    filename = "joined.csv"
    if usecache and os.path.exists(filename):
        print("Using cached data for joined data")
        return pd.read_csv(filename)
    items = get_items()
    stores = get_stores()
    sales = get_sales()
    df = pd.merge(sales, stores, how="inner", left_on="store", right_on="store_id")
    df = pd.merge(df, items, how="inner", left_on="item", right_on="item_id")
    return df


def get_german_power(usecache=True):
    """Get the data from the German power consumption dataset"""
    filename = "german_power.csv"
    if usecache and os.path.exists(filename):
        print("Using cached data")
        return pd.read_csv(filename)
    df = pd.read_csv(
        "https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv"
    )
    df.describe()
    return df
