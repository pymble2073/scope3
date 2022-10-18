############################## IMPORT OF MODULES ##############################
import requests
import numpy as np
import pandas as pd
import json
# Note: make sure the above Python modules are installed in the machine/server before running this script.
pd.set_option('display.max_columns', None)

############################## CONFIG TO UPDATE #########################################
# PLEASE UPDATE THIS CONFIG SECTION BEFORE RUNNING THIS SCRIPT AGAINST AN INPUT FILE
########################################################################################

# Info about the csv input file
csv_file_name = "smallerFile.csv" # Note: the script assumes that the CSV is located in the same directory as this script. Please fill in the full path if that's not the case.
csv_separator = "," # Generally no need to change this

# Your Scope3 credentials
AccessClientId = "YOUR_CLIENT_ID"
AccessClientSecret = "YOUR_CLIENT_SECRET"

# Info about the column names of the csv file. This inputs will help this script how to comprehend the columns in your input file.
date_header = 'Date'
site_domain_header = 'App/URL'
storeId_header = 'app' # Leave as is if you don't have any column for app.
country_header = 'Country'
device_header = 'Device Type'
mobile_aliases = ['Smart Phone', 'Tablet', 'mobile', 'Mobile']
desktop_aliases = ['Desktop', 'desktop', 'PC']
impressions_header = 'Impressions'
date_format = "%d/%m/%Y"
creative_format_header = 'Creative Type'
banner_aliases = ['banner', 'Banner', 'Standard']
video_aliases = ['video', 'Video']
use_creative_size_column = True #Set to True if you have a column for Creative Size (e.g. 300x250), set to False if you prefer using separate width & height instead.
creative_size_header = 'Creative Size' # Keep as is if that column doesn't exist
creative_width_header = 'Creative Width'
creative_height_header =  'Creative Height'
creative_duration_header = 'Max Video Duration (seconds)'

# Output parameters
includeRows = 'true' # Leave as is.
max_json_rows = 50000 # Leave as is unless testing. This number is the number of rows 
scope3_api_version = 'v1' # Only internal Scope3 users currently have access to other API versions. 

############################## CONFIG TO UPDATE #########################################
############################## READ FILE #########################################
def isNaN_str(string):
	# This function returns true when an input string is NaN.
    return string != string

def isNan_int(int):
	return int != int

def prepare_input_file(csv_file):
	# This function creates a dataframe from the input CSV file.
	# This dataframe has a few additional columns: a new date and a new device_type field in a format that Scope3 API can interpret, as well as a new identifier column.
	
	# Starting script by creating a dataframe from the input CSV file.
	print("==================")
	print("START OF SCRIPT")
	print("Reading input file " + csv_file)
	report_df = pd.read_csv(csv_file, sep=csv_separator, on_bad_lines='skip')
	print("Number of valid rows found: " + str(len(report_df.index)) + " rows")

	# Building the three new columns
	report_df['scope3_row_identifier'] = np.arange(len(report_df))
	report_df['scope3_formatted_date'] = pd.to_datetime(report_df[date_header],format=date_format).dt.strftime('%Y-%m-%d')
	report_df['scope3_formatted_device_type'] = ['mobile' if x in mobile_aliases else 'desktop' if x in desktop_aliases else 'unknown' for x in report_df[device_header]]
	report_df['scope3_formatted_creative_format'] = ['banner' if x in banner_aliases else 'video' if x in video_aliases else 'unknown' for x in report_df[creative_format_header]]
	if use_creative_size_column and creative_size_header in report_df:
		report_df[['scope3_formatted_width','scope3_formatted_height']] = report_df[creative_size_header].str.split('x',expand=True)
	elif not use_creative_size_column and creative_width_header in report_df and creative_height_header in report_df:
		report_df['scope3_formatted_width'] = [0 if isNan_int(x) else report_df[creative_width_header] for x in report_df[creative_width_header]]
		report_df['scope3_formatted_height'] = [0 if isNan_int(x) else report_df[creative_height_header] for x in report_df[creative_height_header]]
	else:
		report_df['scope3_formatted_width'].values[:] = 0
		report_df['scope3_formatted_height'].values[:] = 0
	# Returning the dataframe
	return(report_df)


def evaluate_emissions(report_df):
	# This function uses the dataframe constructed in the previous function to build JSON objects and make the necessary number of calls to the Scope3 API to obtain emissions data.
	rows_to_compute = []
	for index, row in report_df.iterrows():
		row_dict = {}
		row_dict["identifier"] = str(row["scope3_row_identifier"])
		if isNaN_str(row[site_domain_header]):
			row_dict["app"] = {}
			row_dict["app"]["storeId"] = str(row[storeId_header])
		else:
			row_dict["site"] = {}
			row_dict["site"]["domain"] = str(row[site_domain_header])
		row_dict["country"] = row[country_header]
		row_dict["device"] = row["scope3_formatted_device_type"]
		row_dict["impressions"] = row[impressions_header]
		row_dict["date"] = str(row["scope3_formatted_date"])
		row_dict["creative"] = {}
		row_dict["creative"]["format"] = row['scope3_formatted_creative_format']

		if row["scope3_formatted_creative_format"] == 'video':
			row_dict["creative"]["durationSeconds"] = int(row[creative_duration_header])
		elif row["scope3_formatted_device_type"] == 'banner' and not isNan_int(row['scope3_formatted_width']) and not isNan_int(row['scope3_formatted_height']):
			row_dict["creative"]["width"] = int(row['scope3_formatted_width'])
			row_dict["creative"]["height"] = int(row['scope3_formatted_height'])
		
		rows_to_compute.append(row_dict)

	number_of_api_calls_to_make = len(rows_to_compute) // max_json_rows + 1
	print("Given the size of your input dataset the script will need to go through " + str(number_of_api_calls_to_make) + " loop(s) to fetch Scope3 emissions data.")

	if scope3_api_version == 'v2':
		url = "https://api.scope3.com/v1/calculate/daily?includeRows=" + includeRows + "&previewSupplyChain=True"
	else:
		url = "https://api.scope3.com/v1/calculate/daily?includeRows=" + includeRows

	headers = {
	    "Accept": "application/json",
	    "Content-Type": "application/json",
	    "AccessClientId": AccessClientId,
	    "AccessClientSecret": AccessClientSecret
	}

	api_result_df = pd.DataFrame()
	impressionsModeled = 0
	impressionsSkipped = 0

	for i in range(number_of_api_calls_to_make):
		print("Going through loop number " + str(i+1) + "...")
		report_json = {}
		report_json["rows"] = rows_to_compute[i*max_json_rows:(i+1)*max_json_rows-1]
		req = requests.post(url, json=report_json, headers=headers)
		response = req.json()
		response_rows_json_string = json.dumps(response['rows'])
		impressionsModeled += response['impressionsModeled']
		impressionsSkipped += response['impressionsSkipped']
		result_df = pd.read_json(response_rows_json_string)
		api_result_df = pd.concat([api_result_df, result_df],axis=0)
	
	api_result_df['totalEmissions'] = api_result_df['baseEmissions'] + api_result_df['supplyPathEmissions'] + api_result_df['creativeEmissions']
	api_result_df = api_result_df[['identifier', 'domainCoverage', 'baseEmissions', 'supplyPathEmissions', 'creativeEmissions', 'totalEmissions']]
	print("All loops completed, now joining a few things together to compute key stats and tables...")
	merged_df = report_df.merge(api_result_df, how='left', left_on='scope3_row_identifier', right_on='identifier')
	total_campaign_impressions = merged_df[impressions_header].sum()
	merged_df.to_csv(csv_file_name[:-4] + '_withScope3Emissions.csv', index=False)

	missedDomains_df = merged_df[merged_df.domainCoverage == 'missing']
	missedDomains_df = missedDomains_df[[site_domain_header, impressions_header]]
	merged_df = merged_df[merged_df.domainCoverage == 'modeled'] #Now Removing lines where we can't calculate emissions as we don't need them any more.
	scope3_measurement_rate_pct = round(impressionsModeled*100 / total_campaign_impressions, 1)
	total_emissions_in_grams = round(merged_df['totalEmissions'].sum(),0)
	base_emissions_in_grams = round(merged_df['baseEmissions'].sum(),0)
	supply_emissions_in_grams = round(merged_df['supplyPathEmissions'].sum(),0)
	creative_emissions_in_grams = round(merged_df['creativeEmissions'].sum(),0)
	avg_emissions_per_ad = round(total_emissions_in_grams / impressionsModeled,2)

	print("\n")
	print("==================")
	print("KEY STATS: ")
	print("Total campaign impressions: " + str(total_campaign_impressions) + " imps." )
	print("Total modelled impressions: " + str(impressionsModeled) + " imps." )
	print("Total skipped impressions: " + str(impressionsSkipped) + " imps." )
	print("Measurement Rate: " + str(scope3_measurement_rate_pct) + "%." )
	print("TOTAL emissions for the campaign: " + str(total_emissions_in_grams) + "g OR " + str(round(total_emissions_in_grams/1000000,5))+"mt." )
	print("Base emissions (v1) for the campaign: " + str(base_emissions_in_grams) + "g OR " + str(round(base_emissions_in_grams/1000000,5))+"mt." )
	print("Supply Path emissions (v1) for the campaign: " + str(supply_emissions_in_grams) + "g OR " + str(round(supply_emissions_in_grams/1000000,5))+"mt." )
	print("Creative emissions for the campaign: " + str(creative_emissions_in_grams) + "g OR " + str(round(creative_emissions_in_grams/1000000,5))+"mt." )
	print("Average CO2e emissions per impression: " + str(avg_emissions_per_ad) + "g." )
	print("END OF KEY STATS")

	groupedbyDomains_df = merged_df.groupby([site_domain_header]).aggregate({impressions_header:sum, 'totalEmissions':sum, 'baseEmissions':sum, 'supplyPathEmissions':sum,'creativeEmissions':sum})
	groupedbyDomains_df['avg_emissions_per_ad_in_grams'] = groupedbyDomains_df['totalEmissions'] / groupedbyDomains_df[impressions_header]
	print("==================")
	print("\n")
	print("STATS FOR KEY DOMAINS: ")
	print("We've just created " + csv_file_name[:-4] + "_withScope3Emissions.csv for you to download.")
	print("See top insights below.")

	print("\n")
	print("Top 10 DOMAINS by impressions: ")
	top10DomainsByImps_df = groupedbyDomains_df.sort_values(impressions_header, ascending=False).head(10)
	top10DomainsByImps_df.to_csv('scope3_emissions_top_domains.csv', index=False)
	print("We've created scope3_emissions_top_domains.csv. You can also view breakdown below:")
	print(top10DomainsByImps_df)

	print("\n")
	print("Top 10 DOMAINS by average emissions (having served more than 1000 imps) ")
	top10DomainsByEmissions_df = groupedbyDomains_df[groupedbyDomains_df[impressions_header]>1000].sort_values('avg_emissions_per_ad_in_grams', ascending=False).head(10)
	top10DomainsByEmissions_df.to_csv('scope3_emissions_top_emitting_domains.csv', index=False)
	print("We've created scope3_emissions_top_emitting_domains.csv. You can also view breakdown below:")
	print(top10DomainsByEmissions_df)

	print("\n")
	print("Breakdwon by DEVICE")
	groupedbyDevice_df = merged_df.groupby(device_header, as_index=False).aggregate({impressions_header:sum, 'totalEmissions':sum})
	groupedbyDevice_df['avg_emissions_per_ad_in_grams'] = groupedbyDevice_df['totalEmissions']/groupedbyDevice_df[impressions_header]
	groupedbyDevice_df.to_csv('scope3_emissions_by_device_type.csv', index=False)
	print(groupedbyDevice_df)

	print("\n")
	print("Breakdwon by FORMAT")
	groupedbyFormat_df = merged_df.groupby(creative_format_header, as_index=False).aggregate({impressions_header:sum, 'totalEmissions':sum})
	groupedbyFormat_df['avg_emissions_per_ad_in_grams'] = groupedbyFormat_df['totalEmissions']/groupedbyFormat_df[impressions_header]
	groupedbyFormat_df.to_csv('scope3_emissions_by_format_type.csv', index=False)
	print(groupedbyFormat_df)

	number_of_missed_domains = len(missedDomains_df.index)
	print("\n")
	print("Number of domains that weren't modeled: " + str(number_of_missed_domains) + " domain(s).")
	if number_of_missed_domains != 0:
		top10MissedDomains_df = missedDomains_df.sort_values(impressions_header, ascending=False).head(10)
		missedDomains_df.to_csv('scope3_emissions_top_missed_domains.csv', index=False)
		print("We've created scope3_emissions_missed_domains.csv. These are the top (10) domains that are missing:")
		print(top10MissedDomains_df)

	print("\n")
	print("==================")
	print("END OF SCRIPT")

evaluate_emissions(prepare_input_file(csv_file_name))
