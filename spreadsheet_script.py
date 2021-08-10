import time
import schedule
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
from service import all_email_permuter, MailtesterSingle, export_to_sheets
from schedule import every, repeat
from oauth2client import file
import gspread


def authenticate():
	gauth = GoogleAuth()
	# Try to load saved client credentials
	gauth.LoadCredentialsFile("mycreds.txt")
	if gauth.credentials is None:
		# Authenticate if they're not there
		gauth.LocalWebserverAuth()
	elif gauth.access_token_expired:
		# Refresh them if expired
		gauth.Refresh()
	else:
		# Initialize the saved creds
		gauth.Authorize()
	# Save the current credentials to a file
	gauth.SaveCredentialsFile("mycreds.txt")
	drive = GoogleDrive(gauth)
	return drive


def authorize_for_spreadsheet():
	store = file.Storage('mycreds.txt')
	credentials = store.get()
	# authorize the clientsheet
	client = gspread.authorize(credentials)
	sheet = client.open('testing').get_worksheet_by_id(0)
	return sheet


def read_file_and_update_data():
	sheet = authorize_for_spreadsheet()
	data = sheet.get_all_records()
	
	# demo file to be created for passing dataframes to excel file and then reading from it
	# due to error from directly reading the dataframe parsed from spreadsheet
	file = 'testing.xlsx'
	original_data = pd.DataFrame(data)
	original_data.to_excel(file, index=False)
	original_data = pd.read_excel(file)
	filtered_data = pd.isna(original_data['Email ID'])
	new_data = original_data[filtered_data]
	if new_data.empty or filtered_data.empty:
		return
	for row_name, data in new_data.iterrows():
		# print("row_name:", row_name, "\ndata:", data)
		fname = data['firstname'].strip().replace(" ", "")
		lname = data['lastname'].strip().replace(" ", "")
		domain = data['company name'].strip().replace(" ", "")
		# Specify the first name, last name and domain name
		permuted_emails = all_email_permuter(first_name=fname, last_name=lname, domain_name=domain)
		all_emails = []
		for email in permuted_emails:
			print('testing mail is :', email)
			verify = MailtesterSingle(email)
			if verify['result'] == 'ok':
				all_emails.append(email)
				data['Email ID'] = ",".join(map(str, all_emails))
		print(all_emails)
	# filter out records with emails from original file
	filtered_df = original_data[original_data['Email ID'].notnull()]
	
	# merge updated record with original records
	new_dataframe = pd.concat([filtered_df, new_data]).drop_duplicates().reset_index(drop=True)
	new_dataframe.to_excel(file, index=False)
	# writing the data to the spreadsheet
	export_to_sheets(sheet, new_dataframe, mode='w')
	return


# deleting the file locally created
def cleanup():
	import os
	if os.path.exists("testing.xlsx"):
		os.remove("testing.xlsx")
	else:
		return


@repeat(every(2).hours)
def main_function():
	authenticate()
	read_file_and_update_data()
	cleanup()


main_function()


# scheduler
schedule.every(2).hours.do(main_function)


def run_scheduler():
	while True:
		# Checks whether a scheduled task
		# is pending to run or not
		schedule.run_pending()
		time.sleep(1)
