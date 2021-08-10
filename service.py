import itertools
import gspread_dataframe as gd
import gspread as gs
import requests
import pandas as pd


# noinspection PyTypeChecker
def all_email_permuter(first_name, last_name, domain_name):
	"""
	Excepts first_name, last_name and domain_name as arguments and returns a
	list of all permutations of possible mail id's.
	"""
	first_name = first_name.lower()
	last_name = last_name.lower()
	domain_name = domain_name.lower()
	all_names = [[first_name, first_name[0]], [last_name, last_name[0]]]
	punctuations = ". _ - ".split()
	a = list(itertools.product(all_names[0], all_names[1], punctuations))
	b = list(itertools.product(all_names[0], all_names[1]))
	combinations = [s for x in a for s in itertools.permutations(x, 3) if s[0] not in punctuations if
					s[-1] not in punctuations]
	combinations.extend(["".join(s) for x in b
						 for s in itertools.permutations(x, 2)
						 if s[0] not in punctuations
						 if s[-1] not in punctuations])
	combinations = ["".join(s) for s in combinations]
	combinations.extend([first_name, last_name])
	permuted_emails = [f"{s}@{domain_name}" for s in combinations]
	return permuted_emails


# mail tester
def MailtesterSingle(email):
	key = 'Enter Your MailTester API key'
	verify = "https://api.mailtester.com/api/singlemail?secret="
	url = verify + key + "&email=" + email
	r = requests.get(url)
	return r.json()


# writing to spreadsheet(contain multiple modes for file for further use)
def export_to_sheets(worksheet, df, mode='r'):
	ws = worksheet
	if (mode == 'w'):
		ws.clear()
		gd.set_with_dataframe(worksheet=ws, dataframe=df, include_index=False, include_column_header=True, resize=True)
		return True
	elif (mode == 'a'):
		ws.add_rows(df.shape[0])
		gd.set_with_dataframe(worksheet=ws, dataframe=df, include_index=False, include_column_header=False,
							  row=ws.row_count + 1, resize=False)
		return True
	else:
		return gd.get_as_dataframe(worksheet=ws)

