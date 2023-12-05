from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta
import csv

# read API key
private_uri = "private/uri.txt"
with open(private_uri, "r") as file:
    uri = file.read()
del file, private_uri

# create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# select data collection
col = client["ComEd"]["data"]

# get search parameters
zip_codes = [60642, 60614, 60620, 60630, 60651, 60827, 60659, 60707, 60601, 60621, 60657, 60608, 60631, 60632, 60656, 60605, 60606, 60626, 60617, 60641, 60649, 60646, 60629, 60638, 60640, 60661, 60602, 60625, 60618, 60616, 60637, 60623, 60622, 60634, 60612, 60652, 60645, 60644, 60603, 60653, 60666, 60607, 60654, 60639, 60647, 60655, 60615, 60613, 60633, 60643, 60611, 60628, 60636, 60604, 60624, 60609, 60619, 60610, 60660]
start_day = datetime(2021, 1 ,1)
end_day = datetime(2021, 1, 31)

# aggregation pipeline
pipeline = [
    {
        "$match": {
            "INTERVAL_READING_DATE": {
                "$gte": start_day,
                "$lt": end_day + timedelta(days=1)
            },
            "ZIP_CODE": {"$in": zip_codes}
        }
    },
    {
        "$group": {
            "_id": {
                "ZIP_CODE": "$ZIP_CODE",
                "month": {"$month": "$INTERVAL_READING_DATE"},
                "day": {"$dayOfMonth": "$INTERVAL_READING_DATE"}
            },
            "total_energy": {"$sum": "$TOTAL_REGISTERED_ENERGY"},
            "HR0030": {"$sum": "$INTERVAL_HR0030_ENERGY_QTY"},
            "HR0100": {"$sum": "$INTERVAL_HR0100_ENERGY_QTY"},
            "HR0130": {"$sum": "$INTERVAL_HR0130_ENERGY_QTY"},
            "HR0200": {"$sum": "$INTERVAL_HR0200_ENERGY_QTY"},
            "HR0230": {"$sum": "$INTERVAL_HR0230_ENERGY_QTY"},
            "HR0300": {"$sum": "$INTERVAL_HR0300_ENERGY_QTY"},
            "HR0330": {"$sum": "$INTERVAL_HR0330_ENERGY_QTY"},
            "HR0400": {"$sum": "$INTERVAL_HR0400_ENERGY_QTY"},
            "HR0430": {"$sum": "$INTERVAL_HR0430_ENERGY_QTY"},
            "HR0500": {"$sum": "$INTERVAL_HR0500_ENERGY_QTY"},
            "HR0530": {"$sum": "$INTERVAL_HR0530_ENERGY_QTY"},
            "HR0600": {"$sum": "$INTERVAL_HR0600_ENERGY_QTY"},
            "HR0630": {"$sum": "$INTERVAL_HR0630_ENERGY_QTY"},
            "HR0700": {"$sum": "$INTERVAL_HR0700_ENERGY_QTY"},
            "HR0730": {"$sum": "$INTERVAL_HR0730_ENERGY_QTY"},
            "HR0800": {"$sum": "$INTERVAL_HR0800_ENERGY_QTY"},
            "HR0830": {"$sum": "$INTERVAL_HR0830_ENERGY_QTY"},
            "HR0900": {"$sum": "$INTERVAL_HR0900_ENERGY_QTY"},
            "HR0930": {"$sum": "$INTERVAL_HR0930_ENERGY_QTY"},
            "HR1000": {"$sum": "$INTERVAL_HR1000_ENERGY_QTY"},
            "HR1030": {"$sum": "$INTERVAL_HR1030_ENERGY_QTY"},
            "HR1100": {"$sum": "$INTERVAL_HR1100_ENERGY_QTY"},
            "HR1130": {"$sum": "$INTERVAL_HR1130_ENERGY_QTY"},
            "HR1200": {"$sum": "$INTERVAL_HR1200_ENERGY_QTY"},
            "HR1230": {"$sum": "$INTERVAL_HR1230_ENERGY_QTY"},
            "HR1300": {"$sum": "$INTERVAL_HR1300_ENERGY_QTY"},
            "HR1330": {"$sum": "$INTERVAL_HR1330_ENERGY_QTY"},
            "HR1400": {"$sum": "$INTERVAL_HR1400_ENERGY_QTY"},
            "HR1430": {"$sum": "$INTERVAL_HR1430_ENERGY_QTY"},
            "HR1500": {"$sum": "$INTERVAL_HR1500_ENERGY_QTY"},
            "HR1530": {"$sum": "$INTERVAL_HR1530_ENERGY_QTY"},
            "HR1600": {"$sum": "$INTERVAL_HR1600_ENERGY_QTY"},
            "HR1630": {"$sum": "$INTERVAL_HR1630_ENERGY_QTY"},
            "HR1700": {"$sum": "$INTERVAL_HR1700_ENERGY_QTY"},
            "HR1730": {"$sum": "$INTERVAL_HR1730_ENERGY_QTY"},
            "HR1800": {"$sum": "$INTERVAL_HR1800_ENERGY_QTY"},
            "HR1830": {"$sum": "$INTERVAL_HR1830_ENERGY_QTY"},
            "HR1900": {"$sum": "$INTERVAL_HR1900_ENERGY_QTY"},
            "HR1930": {"$sum": "$INTERVAL_HR1930_ENERGY_QTY"},
            "HR2000": {"$sum": "$INTERVAL_HR2000_ENERGY_QTY"},
            "HR2030": {"$sum": "$INTERVAL_HR2030_ENERGY_QTY"},
            "HR2100": {"$sum": "$INTERVAL_HR2100_ENERGY_QTY"},
            "HR2130": {"$sum": "$INTERVAL_HR2130_ENERGY_QTY"},
            "HR2200": {"$sum": "$INTERVAL_HR2200_ENERGY_QTY"},
            "HR2230": {"$sum": "$INTERVAL_HR2230_ENERGY_QTY"},
            "HR2300": {"$sum": "$INTERVAL_HR2300_ENERGY_QTY"},
            "HR2330": {"$sum": "$INTERVAL_HR2330_ENERGY_QTY"},
            "HR2400": {"$sum": "$INTERVAL_HR2400_ENERGY_QTY"},
        }
    }
]

result = col.aggregate(pipeline)

data_to_export = list(result)

# Specify the file name for the CSV export
csv_filename = "./data/results/time_series/chicago_aggregation_final.csv"


with open(csv_filename, 'w', newline='') as csvfile:
# Create fieldnames including all intervals
    fieldnames = [
        'ZIP_CODE', 'month', 'day', 'total_energy',
        'HR0030', 'HR0100', 'HR0130', 'HR0200', 'HR0230',
        'HR0300', 'HR0330', 'HR0400', 'HR0430', 'HR0500',
        'HR0530', 'HR0600', 'HR0630', 'HR0700', 'HR0730',
        'HR0800', 'HR0830', 'HR0900', 'HR0930', 'HR1000',
        'HR1030', 'HR1100', 'HR1130', 'HR1200', 'HR1230',
        'HR1300', 'HR1330', 'HR1400', 'HR1430', 'HR1500',
        'HR1530', 'HR1600', 'HR1630', 'HR1700', 'HR1730',
        'HR1800', 'HR1830', 'HR1900', 'HR1930', 'HR2000',
        'HR2030', 'HR2100', 'HR2130', 'HR2200', 'HR2230',
        'HR2300', 'HR2330', 'HR2400',
    ]

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    # Write the header row
    writer.writeheader()
    
    # Write the data rows
    for row in data_to_export:
        writer.writerow({
            'ZIP_CODE': row['_id']['ZIP_CODE'],
            'month': row['_id']['month'],
            'day': row['_id']['day'],
            'total_energy': row['total_energy'],
            'HR0030': row['HR0030'],
            'HR0100': row['HR0100'],
            'HR0130': row['HR0130'],
            'HR0200': row['HR0200'],
            'HR0230': row['HR0230'],
            'HR0300': row['HR0300'],
            'HR0330': row['HR0330'],
            'HR0400': row['HR0400'],
            'HR0430': row['HR0430'],
            'HR0500': row['HR0500'],
            'HR0530': row['HR0530'],
            'HR0600': row['HR0600'],
            'HR0630': row['HR0630'],
            'HR0700': row['HR0700'],
            'HR0730': row['HR0730'],
            'HR0800': row['HR0800'],
            'HR0830': row['HR0830'],
            'HR0900': row['HR0900'],
            'HR0930': row['HR0930'],
            'HR1000': row['HR1000'],
            'HR1030': row['HR1030'],
            'HR1100': row['HR1100'],
            'HR1130': row['HR1130'],
            'HR1200': row['HR1200'],
            'HR1230': row['HR1230'],
            'HR1300': row['HR1300'],
            'HR1330': row['HR1330'],
            'HR1400': row['HR1400'],
            'HR1430': row['HR1430'],
            'HR1500': row['HR1500'],
            'HR1530': row['HR1530'],
            'HR1600': row['HR1600'],
            'HR1630': row['HR1630'],
            'HR1700': row['HR1700'],
            'HR1730': row['HR1730'],
            'HR1800': row['HR1800'],
            'HR1830': row['HR1830'],
            'HR1900': row['HR1900'],
            'HR1930': row['HR1930'],
            'HR2000': row['HR2000'],
            'HR2030': row['HR2030'],
            'HR2100': row['HR2100'],
            'HR2130': row['HR2130'],
            'HR2200': row['HR2200'],
            'HR2230': row['HR2230'],
            'HR2300': row['HR2300'],
            'HR2330': row['HR2330'],
            'HR2400': row['HR2400'],
        })
        
print(f"Exported data to {csv_filename}")
client.close()


