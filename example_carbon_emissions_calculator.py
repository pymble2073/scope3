###### START OF SCRIPT ######
#
# Author: Niki Banerjee (Scope3)
# Last update: September 13th 2022.
#
# Objective: Allow Scope3 clients to calculate emissions off a CSV file
#
##############################


############################## IMPORT OF MODULES ##############################
import requests
import pandas as pd
# Note: make sure these two Python modules are installed in the machine/server before running this script.

############################## CONFIG TO UPDATE #########################################

# Info about the csv input file
csv_file_name = "Scope3_input.csv" # Note: the script assumes that the CSV is located in the same directory as this script. Please fill in the full path if that's not the case.
csv_separator = ","

# Info about the column names of the csv file
site_domain_header = 'sitename'
# app_id_header = '' # assuming no app store ID
# country_header = '' # assuming country is AU
device_header = 'devicetype'
impressions_header = 'impressions'
date_header = 'deliverydatekey'
creative_format_header = 'creativename'
creative_durations_header = 'duration'
creative_width_header = 'creativewidth'
creative_height_header = 'creativeheight'
# creative_bytes_header = '' # assuming creative_bytes not specified

includeRows = 'false' # change to 'true' if you want the API output to be broken out by row.

############################## READ FILE #########################################
def validate_input_file():
	# This function does nothing right now but in the future it could do a pre-check of the input files.
	# For example it could check whether any required input is missing.
	print("Validating input file...")

def create_json_data_from_csv():
	# This function reads the CSV, transform it into a datframe and then into the JSON object that will be used for the API call.
	print("Processing input file...")
	report_df = pd.read_csv(csv_file_name, sep=csv_separator)

	rows_to_compute = []
	for index, row in report_df.iterrows():
		row_dict = {}

		row_dict["site"] = {}
		row_dict["site"]["domain"] = row[site_domain_header]
		rows_to_compute.append(row_dict)

		row_dict["country"] = 'AU' # Overwritting country because the client's input file doesn't have any country column (and this is a required field).

		#row_dict["device"] = row[device_header]
		row_dict["device"] = 'mobile' # Overwritting device as a "dirty" shortcut because the client's example input file only had smartphone/tablet. This needs to be improved.
		row_dict["impressions"] = row[impressions_header]
		row_dict["date"] = row[date_header]
		# row_dict["date"] = '2022-09-01'
		
		row_dict["creative"] = {}
		row_dict["creative"]["width"] = row[creative_width_header]
		row_dict["creative"]["height"] = row[creative_height_header]
		row_dict["creative"]["format"] = row[creative_format_header]
		row_dict["creative"]["durationSeconds"] = row[creative_durations_header]

	report_json = {}
	report_json["rows"] = rows_to_compute
		
	return report_json


def evaluate_emissions(report_json):
	print("Calculating emissions...")
	url = "https://api.scope3.com/v1/calculate/daily?includeRows=" + includeRows
	payload = report_json
	headers = {
	    "Accept": "application/json",
	    "Content-Type": "application/json",
	    "AccessClientId": "YOUR_CLIENT_ID", # EDIT THIS BEFORE EXECUTION
	    "AccessClientSecret": "YOUR_CLIENT_SECRET" # EDIT THIS BEFORE EXECUTION
	}

	response = requests.post(url, json=payload, headers=headers)
	print("Output from the Scope3 API: ")
	print(response.json())

evaluate_emissions(create_json_data_from_csv())
