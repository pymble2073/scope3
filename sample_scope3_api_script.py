############################## IMPORT OF MODULES ##############################
import requests
import numpy as np
import pandas as pd
import json
import datetime
import urllib.parse
# Note: make sure the above Python modules are installed in the machine/server before running this script.
pd.set_option('display.max_columns', None)

############################## CONFIG TO UPDATE #########################################
# PLEASE UPDATE THIS CONFIG SECTION BEFORE RUNNING THIS SCRIPT AGAINST AN INPUT FILE
########################################################################################

# Info about the csv input file
#csv_file_name = "Lenovo_input_video.csv" # Note: the script assumes that the CSV is located in the same directory as this script. Please fill in the full path if that's not the case.
csv_file_name = "sample_input_file.csv"
csv_separator = "," # Generally no need to change this

# Your Scope3 credentials
AccessClientId = "YOUR_CLIENT_ID"
AccessClientSecret = "YOUR_CLIENT_SECRET"

# Info about the column names of the csv file. This inputs will help this script how to comprehend the columns in your input file.
use_date_column = True #REQUIRED, set to false if no date column is provided or if you prefer using a recent date.
date_header = 'Date' #Leave as is if no date column is provided.
date_format = "%d/%m/%Y" #Leave as is if no date column is provided.

site_domain_or_app_header = 'App/URL' #REQUIRED
country_header = 'Country' #REQUIRED
impressions_header = 'Impressions' #REQUIRED

use_channel_column = True #REQUIRED
channel_header = 'Environment' #OPTIONAL, if not specified we assume all entries are for display-web
web_aliases = ['Web', 'web', 'Web optimized for device', 'Web optimized for device']
app_aliases = ['App', 'app', 'mobile_app']

device_type_header = 'Device Type' #REQUIRED
phone_aliases = ['Smart Phone', 'smart phone', 'mobile', 'Mobile', 'phone', 'Phone']
tablet_aliases = ['Tablet', 'tablet']
pc_aliases = ['Desktop', 'desktop', 'PC', 'pc']
tv_aliases = ['ctv', 'CTV', 'SmartTV', 'tv', 'TV']

creative_format_header = 'Creative Type'
banner_aliases = ['banner', 'Banner', 'Standard', 'Display', 'Publisher hosted']
video_aliases = ['video', 'Video']
text_aliases = ['Native site', 'text']
use_creative_size_column = True #Set to True if you have a column for Creative Size (e.g. 300x250), set to False if you prefer using separate width & height instead.
creative_size_header = 'Creative Size' # Keep as is if that column doesn't exist in your CSV.
creative_width_header = 'Creative Width'
creative_height_header =  'Creative Height'
creative_duration_header = 'Max Video Duration (seconds)'
click_header = 'Clicks' # Keep as is if that column doesn't exist in your CSV.

# Constants: do not modify unless required.
max_json_rows = 100000
scope3_api_version = '1' # Leave as is to use v1.1
preview_methology = 'false'
url = "https://api.scope3.com/v" + scope3_api_version + "/calculate/daily?includeRows=true&previewMethodology=" + preview_methology

############################## CONFIG TO UPDATE #########################################
############################## READ FILE #########################################
def normalizeDomain(url_field):
	# This function returns the domain component of a URL (url_field)
	# for example if https://www.nytimes.com/section/world is the input, nytimes.com is the output.
	url_to_process = url_field
	if 'http' not in url_to_process:
		url_to_process = "http://" + url_to_process
	url_to_process = url_to_process.replace('www.','')
	domain = urllib.parse.urlparse(url_to_process).netloc
	return domain

def normalizeApp(app_field):
	# This function returns the storeId contained in the app_field, allowing this script to support full store URLs too.
	# for example if https://apps.apple.com/au/app/9now/id542088539 is the input, 542088539 is the output.
	# And com.easybrain.sudoku.android remains com.easybrain.sudoku.android
	if "apps.apple.com" in app_field:
		storeId = app_field.split(sep="/id")[1]
	elif "play.google.com/store/apps/" in app_field:
		parsedURL = urllib.parse.urlparse(app_field)
		storeId = urllib.parse.parse_qs(parsedURL.query)['id'][0]
	else:
		storeId = app_field
	return storeId

def normalizeDomainOrApp(row):
	# This function returns 
	if row['scope3_formatted_channel'] == 'display-web':
		return normalizeDomain(row[site_domain_or_app_header])
	else:
		return normalizeApp(row[site_domain_or_app_header])

def prepare_input_file(csv_file):
	# This function creates a dataframe from the input CSV file.
	# This dataframe has all the columns that will be needed 

	print("==================")
	print("START OF SCRIPT")
	print("Reading input file " + csv_file)
	report_df = pd.read_csv(csv_file, sep=csv_separator, on_bad_lines='skip')
	print("Number of valid rows found: " + str(len(report_df.index)) + " rows")
	# Building the three new columns
	report_df['scope3_row_identifier'] = np.arange(len(report_df))

	if use_date_column:
		report_df['scope3_formatted_date'] = pd.to_datetime(report_df[date_header],format=date_format).dt.strftime('%Y-%m-%d')
	else:
		report_df['scope3_formatted_date'] = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
	report_df['scope3_formatted_device_type'] = ['phone' if x in phone_aliases else 'tablet' if x in tablet_aliases else 'pc' if x in pc_aliases else 'tv' if x in tv_aliases else 'pc' for x in report_df[device_type_header]]
	report_df['scope3_formatted_creative_format'] = ['banner' if x in banner_aliases else 'video' if x in video_aliases else 'text' if x in text_aliases else 'unknown' for x in report_df[creative_format_header]]
	report_df['scope3_formatted_channel'] = ['display-web' if x in web_aliases else 'display-app' if x in app_aliases else 'unknown' for x in report_df[channel_header]]
	report_df['scope3_formatted_width'] = 0
	report_df['scope3_formatted_height'] = 0
	if use_creative_size_column and creative_size_header in report_df:
		report_df[creative_size_header] = report_df[creative_size_header].replace({'Unknown':'0x0'})
		report_df[['scope3_formatted_width','scope3_formatted_height']] = report_df[creative_size_header].str.split('x',expand=True)
	elif not use_creative_size_column and creative_width_header in report_df and creative_height_header in report_df:
		report_df.loc[report_df[creative_width_header].astype(str).str.isdigit(), 'scope3_formatted_width'] = report_df[creative_width_header]
		report_df.loc[report_df[creative_height_header].astype(str).str.isdigit(), 'scope3_formatted_height'] = report_df[creative_height_header]

	# Returning the dataframe
	return(report_df)


def evaluate_emissions(report_df):
	# This function uses the dataframe constructed in the previous function to build JSON objects and make the necessary number of calls to the Scope3 API to obtain emissions data.
	# Key stats are calculated off the back of this data and displayed on terminal through prints, as well as exported as CSV files.
	rows_to_compute = []

	for index, row in report_df.iterrows():
		row_dict = {}
		row_dict["identifier"] = str(row["scope3_row_identifier"])
		row_dict["channel"] = str(row["scope3_formatted_channel"])
		if row["scope3_formatted_channel"] == 'display-web':
			row_dict["site"] = {}
			row_dict["site"]["domain"] = normalizeDomain(str(row[site_domain_or_app_header]))
		elif row["scope3_formatted_channel"] == 'display-app':
			row_dict["app"] = {}
			row_dict["app"]["storeId"] = normalizeApp(str(row[site_domain_or_app_header]))
		
		row_dict["country"] = row[country_header]
		row_dict["deviceType"] = row["scope3_formatted_device_type"]
		row_dict["impressions"] = int(row[impressions_header])
		row_dict["date"] = str(row["scope3_formatted_date"])
		row_dict["creative"] = {}
		row_dict["creative"]["format"] = row['scope3_formatted_creative_format']

		if row["scope3_formatted_creative_format"] == 'video':
			row_dict["creative"]["durationSeconds"] = int(row[creative_duration_header])
		elif row["scope3_formatted_creative_format"] == 'banner':
			row_dict["creative"]["width"] = int(row['scope3_formatted_width'])
			row_dict["creative"]["height"] = int(row['scope3_formatted_height'])

		rows_to_compute.append(row_dict)

	number_of_api_calls_to_make = len(rows_to_compute) // max_json_rows + 1
	print("Given the size of your input dataset the script will need to go through " + str(number_of_api_calls_to_make) + " loop(s) to fetch Scope3 emissions data.")

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
	
	api_result_df['totalEmissions'] = api_result_df['mediaDistributionEmissions'] + api_result_df['adSelectionEmissions'] + api_result_df['creativeDistributionEmissions']
	api_result_df = api_result_df[['identifier', 'domainCoverage', 'mediaDistributionEmissions', 'adSelectionEmissions', 'creativeDistributionEmissions', 'totalEmissions']]

	print("All loops completed, now joining a few things together to compute key stats and tables...")
	merged_df = report_df.merge(api_result_df, how='left', left_on='scope3_row_identifier', right_on='identifier')
	total_campaign_impressions = merged_df[impressions_header].sum()
	merged_df.to_csv(csv_file_name[:-4] + '_withScope3Emissions.csv', index=False)

	missedDomains_df = merged_df[merged_df.domainCoverage == 'missing']
	missedDomains_df = missedDomains_df[[site_domain_or_app_header, impressions_header]]
	merged_df = merged_df[merged_df.domainCoverage == 'modeled'] #Now Removing lines where we can't calculate emissions as we don't need them any more.
	scope3_measurement_rate_pct = round(impressionsModeled*100 / total_campaign_impressions, 1)
	total_emissions_in_grams = round(merged_df['totalEmissions'].sum(),0)
	mediaDistributionEmissions_in_grams = round(merged_df['mediaDistributionEmissions'].sum(),0)
	adSelectionEmissions_in_grams = round(merged_df['adSelectionEmissions'].sum(),0)
	creativeDistributionEmissions_in_grams = round(merged_df['creativeDistributionEmissions'].sum(),0)
	avg_emissions_per_ad = round(total_emissions_in_grams / impressionsModeled,2)

	print("\n")
	print("==================")
	print("KEY STATS: ")
	print("Total campaign impressions: " + str(total_campaign_impressions) + " imps." )
	print("Total modelled impressions: " + str(impressionsModeled) + " imps." )
	print("Total skipped impressions: " + str(impressionsSkipped) + " imps." )
	print("Measurement Rate: " + str(scope3_measurement_rate_pct) + "%." )
	print("TOTAL emissions for the campaign: " + str(total_emissions_in_grams) + "g OR " + str(round(total_emissions_in_grams/1000000,5))+"mt." )
	print("mediaDistributionEmissions for the campaign: " + str(mediaDistributionEmissions_in_grams) + "g OR " + str(round(mediaDistributionEmissions_in_grams/1000000,5))+"mt." )
	print("adSelectionEmissions (" + scope3_api_version + ") for the campaign: " + str(adSelectionEmissions_in_grams) + "g OR " + str(round(adSelectionEmissions_in_grams/1000000,5))+"mt." )
	print("creativeDistributionEmissions for the campaign: " + str(creativeDistributionEmissions_in_grams) + "g OR " + str(round(creativeDistributionEmissions_in_grams/1000000,5))+"mt." )
	print("Average CO2e emissions per impression: " + str(avg_emissions_per_ad) + "g." )
	print("END OF KEY STATS")

	if click_header in report_df:
		groupedbyDomains_df = merged_df.groupby([site_domain_or_app_header]).aggregate({impressions_header:sum, click_header:sum, 'totalEmissions':sum, 'mediaDistributionEmissions':sum, 'adSelectionEmissions':sum,'creativeDistributionEmissions':sum})
		groupedbyDomains_df['CTR%'] = groupedbyDomains_df[click_header]*100 / groupedbyDomains_df[impressions_header]
	else:
		groupedbyDomains_df = merged_df.groupby([site_domain_or_app_header]).aggregate({impressions_header:sum, 'totalEmissions':sum, 'mediaDistributionEmissions':sum, 'adSelectionEmissions':sum,'creativeDistributionEmissions':sum})
	groupedbyDomains_df['avg_adSelectionEmissions_per_ad_in_grams'] = groupedbyDomains_df['adSelectionEmissions'] / groupedbyDomains_df[impressions_header]

	print("==================")
	print("\n")
	print("STATS FOR KEY DOMAINS: ")
	print("We've just created " + csv_file_name[:-4] + "_withScope3Emissions.csv for you to download.")
	print("See top insights below.")

	print("\n")
	print("Top 10 DOMAINS by impressions: ")
	top10DomainsByImps_df = groupedbyDomains_df.sort_values(impressions_header, ascending=False).head(10)
	top10DomainsByImps_df.to_csv(csv_file_name[:-4] + '_scope3_emissions_top_domains.csv')
	print("We've created scope3_emissions_top_domains.csv. You can also view breakdown below:")
	print(top10DomainsByImps_df)

	print("\n")
	print("Top 10 DOMAINS by average emissions (having served more than 1000 imps) ")
	top10DomainsByEmissions_df = groupedbyDomains_df[groupedbyDomains_df[impressions_header]>1000].sort_values('avg_adSelectionEmissions_per_ad_in_grams', ascending=False).head(100)
	top10DomainsByEmissions_df.to_csv(csv_file_name[:-4] + '_scope3_emissions_most_emitting_domains.csv')
	print("We've created "+ csv_file_name[:-4] + "_scope3_emissions_most_emitting_domains.csv. You can also view breakdown below:")
	print(top10DomainsByEmissions_df)

	print("\n")
	print("Breakdwon by DEVICE")
	groupedbyDevice_df = merged_df.groupby(device_type_header, as_index=False).aggregate({impressions_header:sum, 'totalEmissions':sum})
	groupedbyDevice_df['avg_emissions_per_ad_in_grams'] = groupedbyDevice_df['totalEmissions']/groupedbyDevice_df[impressions_header]
	groupedbyDevice_df.to_csv(csv_file_name[:-4] + '_scope3_emissions_by_device_type.csv', index=False)
	print(groupedbyDevice_df)

	print("\n")
	print("Breakdwon by FORMAT")
	groupedbyFormat_df = merged_df.groupby(creative_format_header, as_index=False).aggregate({impressions_header:sum, 'totalEmissions':sum})
	groupedbyFormat_df['avg_emissions_per_ad_in_grams'] = groupedbyFormat_df['totalEmissions']/groupedbyFormat_df[impressions_header]
	groupedbyFormat_df.to_csv(csv_file_name[:-4] + '_scope3_emissions_by_format_type.csv')
	print(groupedbyFormat_df)

	number_of_missed_domains = len(missedDomains_df.index)
	print("\n")
	print("Number of domains that weren't modeled: " + str(number_of_missed_domains) + " domain(s).")
	if number_of_missed_domains != 0:
		top10MissedDomains_df = missedDomains_df.groupby(site_domain_or_app_header, as_index=False).aggregate({impressions_header:sum}).sort_values(impressions_header, ascending=False).head(10)
		missedDomains_df.to_csv(csv_file_name[:-4] + '_scope3_missing_domains.csv')
		print("We've created "+ csv_file_name[:-4] + "_scope3_missing_domains.csv. These are the top 10 domains that are missing:")
		print(top10MissedDomains_df)

	print("\n")
	print("==================")
	print("END OF SCRIPT")


evaluate_emissions(prepare_input_file(csv_file_name))
