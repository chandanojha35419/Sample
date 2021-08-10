import time

import schedule
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
from service import all_email_permuter, MailtesterSingle
from schedule import every, repeat


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


def read_file_and_update_data(drive):
	file_id = '1JLlBd-xt7OQ6ImmG43K8l4ZcPzxy7Jm2'  # Drive Excel File ID
	if not file_id:
		return
	file_name = 'testing'  # Give Any Name
	downloaded = drive.CreateFile({'id': file_id})
	downloaded.GetContentFile(file_name)
	
	original_data = pd.read_excel(file_name)
	
	filtered_data = pd.isna(original_data['Email ID'])
	new_data = original_data[filtered_data]
	if new_data.empty or filtered_data.empty:
		return
	for row_name, data in new_data.iterrows():
		# print("row_name:", row_name, "\ndata:", data)
		fname = data['firstname'].strip()
		lname = data['lastname'].strip()
		domain = data['company name'].strip()
		# Specify the first name, last name and domain name
		permuted_emails = all_email_permuter(first_name=fname, last_name=lname, domain_name=domain)
		all_emails = []
		for email in permuted_emails:
			print('testing mail is %s :', email)
			verify = MailtesterSingle(email)
			if verify['result'] == 'ok':
				all_emails.append(email)
				data['Email ID'] = ",".join(map(str, all_emails))

	# filter out records with emails from original file
	filtered_df = original_data[original_data['Email ID'].notnull()]
	
	'''
	The catch is that new updated records are being placed to the end of the file instead of their place in original file
	'''
	
	# merge updated record with original records
	new_dataframe = pd.concat([filtered_df, new_data]).drop_duplicates().reset_index(drop=True)
	new_dataframe.to_excel("testing.xlsx", index=False)
	return (drive, file_id, file_name)


def update_file_content(drive, file_id, file_name):
	"""
	Update a file content of an exiting file on Google Drive.
	Parameters
	----------
	drive: GoogleDrive object
	file_id : str (exiting file File ID)

	Returns(optional) not used here
	-------
	str = updated file's File ID
	"""
	old_file = drive.CreateFile(
		{'id': f"{file_id}", 'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'})
	old_file.SetContentFile(file_name)
	old_file.Upload()
	old_file_id = old_file['id']


# deleting the file locally created
def cleanup():
	import os
	if os.path.exists("testing.xlsx"):
		os.remove("testing.xlsx")
	else:
		return


@repeat(every(2).hours)
def main_function():
	drive = authenticate()
	data = read_file_and_update_data(drive)
	if data:
		drive, file_id, file_name = data[0], data[1], data[2]
		update_file_content(drive, file_id, file_name)
	cleanup()


main_function()

## scheduler ##

schedule.every(2).hours.do(main_function)


def run_scheduler():
	while True:
		# Checks whether a scheduled task
		# is pending to run or not
		schedule.run_pending()
		time.sleep(1)
