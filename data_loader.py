"""
function to import data to data to MongoDB collection
"""
import pymongo
import csv
import os
from datetime import datetime

data_types = {
    "ZIP_CODE": int,
    "DELIVERY_SERVICE_CLASS": str,
    "DELIVERY_SERVICE_NAME": str,
    "ACCOUNT_IDENTIFIER": int,
    "INTERVAL_READING_DATE": lambda x: parse_date(x),
    "INTERVAL_LENGTH": float,
    "TOTAL_REGISTERED_ENERGY": float,
    "INTERVAL_HR0030_ENERGY_QTY": float,
    "INTERVAL_HR0100_ENERGY_QTY": float,
    "INTERVAL_HR0130_ENERGY_QTY": float,
    "INTERVAL_HR0200_ENERGY_QTY": float,
    "INTERVAL_HR0230_ENERGY_QTY": float,
    "INTERVAL_HR0300_ENERGY_QTY": float,
    "INTERVAL_HR0330_ENERGY_QTY": float,
    "INTERVAL_HR0400_ENERGY_QTY": float,
    "INTERVAL_HR0430_ENERGY_QTY": float,
    "INTERVAL_HR0500_ENERGY_QTY": float,
    "INTERVAL_HR0530_ENERGY_QTY": float,
    "INTERVAL_HR0600_ENERGY_QTY": float,
    "INTERVAL_HR0630_ENERGY_QTY": float,
    "INTERVAL_HR0700_ENERGY_QTY": float,
    "INTERVAL_HR0730_ENERGY_QTY": float,
    "INTERVAL_HR0800_ENERGY_QTY": float,
    "INTERVAL_HR0830_ENERGY_QTY": float,
    "INTERVAL_HR0900_ENERGY_QTY": float,
    "INTERVAL_HR0930_ENERGY_QTY": float,
    "INTERVAL_HR1000_ENERGY_QTY": float,
    "INTERVAL_HR1030_ENERGY_QTY": float,
    "INTERVAL_HR1100_ENERGY_QTY": float,
    "INTERVAL_HR1130_ENERGY_QTY": float,
    "INTERVAL_HR1200_ENERGY_QTY": float,
    "INTERVAL_HR1230_ENERGY_QTY": float,
    "INTERVAL_HR1300_ENERGY_QTY": float,
    "INTERVAL_HR1330_ENERGY_QTY": float,
    "INTERVAL_HR1400_ENERGY_QTY": float,
    "INTERVAL_HR1430_ENERGY_QTY": float,
    "INTERVAL_HR1500_ENERGY_QTY": float,
    "INTERVAL_HR1530_ENERGY_QTY": float,
    "INTERVAL_HR1600_ENERGY_QTY": float,
    "INTERVAL_HR1630_ENERGY_QTY": float,
    "INTERVAL_HR1700_ENERGY_QTY": float,
    "INTERVAL_HR1730_ENERGY_QTY": float,
    "INTERVAL_HR1800_ENERGY_QTY": float,
    "INTERVAL_HR1830_ENERGY_QTY": float,
    "INTERVAL_HR1900_ENERGY_QTY": float,
    "INTERVAL_HR1930_ENERGY_QTY": float,
    "INTERVAL_HR2000_ENERGY_QTY": float,
    "INTERVAL_HR2030_ENERGY_QTY": float,
    "INTERVAL_HR2100_ENERGY_QTY": float,
    "INTERVAL_HR2130_ENERGY_QTY": float,
    "INTERVAL_HR2200_ENERGY_QTY": float,
    "INTERVAL_HR2230_ENERGY_QTY": float,
    "INTERVAL_HR2300_ENERGY_QTY": float,
    "INTERVAL_HR2330_ENERGY_QTY": float,
    "INTERVAL_HR2400_ENERGY_QTY": float,
    "INTERVAL_HR2430_ENERGY_QTY": float,
    "INTERVAL_HR2500_ENERGY_QTY": float,
    "PLC_VALUE": float,
    "NSPL_VALUE": float
}

def parse_date(date_str):
    possible_formats = ["%m/%d/%y", "%m/%d/%Y", "%m-%d-%y", "%m-%d-%Y"]
    for fmt in possible_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    # If none of the formats match, handle it as needed
    return None

def convert_data_types(row):
    for col, data_type in data_types.items():
        if col in row and row[col]:
            row[col] = data_type(row[col])
    return row

def upload_folder(directory, collection, spatial, verbose=False):
    spatial_set = set(spatial)
    
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            if any(zip in filename for area in spatial_set):
                if verbose == True:
                    print(filename)
                with open(os.path.join(directory, filename), "r") as file:
                    reader = csv.DictReader(file)
                    data = [convert_data_types(row) for row in reader]
                    collection.insert_many(data)
                    if verbose == True:
                        print(f"{filename} has been added")

if __name__ == "__main__":
    
    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
    import geopandas as gpd
    
    shapefile_path = "data/spatial/geo_export_86dc231e-3ba9-473c-a7a5-0e89f969d1f6.shp"
    gdf = gpd.read_file(shapefile_path)
    chicago_zips = list(gdf["zip"])
    
    uri_string = "private/uri.txt"
    with open(uri_string, "r") as file:
        uri = file.read()
    del file, uri_string
    
    client = MongoClient(uri, server_api = ServerApi("1"))
    
    db = client["ComEd"]
    col = db["data"]
    
    
    upload_folder("./data/data/", col, chicago_zips, verbose=True)