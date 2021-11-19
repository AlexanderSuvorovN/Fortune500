import time
import urllib3
import csv
import mysql.connector
import re
from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from bs4 import BeautifulSoup

print()
print("Connecting to database...")
try:
	cnx = mysql.connector.connect(
		host="localhost",
		database="f500",
		user="app_f500_general",
		passwd="app_f500_general"
	)
except mysql.connector.Error as err:
	if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
		print("Something is wrong with your user name or password")
	elif err.errno == errorcode.ER_BAD_DB_ERROR:
		print("Database does not exist")
	else:
		print(err)

http = urllib3.PoolManager()

print("Querying companies...")
cursor = cnx.cursor(dictionary=True)
query = ("SELECT c.`id`, c.`name`, c.`rank`, c.`revenue`, c.`f500_url`, c.`ceo`, c.`ceo_title`, c.`sector`, c.`industry`, c.`hq_location`, c.`website`, c.`employees` FROM companies c ORDER BY c.`rank` ASC")
cursor.execute(query)
companies_rows = cursor.fetchall()
counter = 0
print("Creating companies list...")
companies = []
for row in companies_rows:
	company = {}
	company["id"] = row["id"]
	company["name"] = row["name"]
	company["rank"] = row["rank"]
	company["revenue"] = row["revenue"]
	company["f500_url"] = row["f500_url"]
	company["ceo"] = row["ceo"]
	company["ceo_title"] = row["ceo_title"]
	company["sector"] = row["sector"]
	company["industry"] = row["industry"]
	company["hq_location"] = row["hq_location"]
	company["website"] = row["website"]
	company["employees"] = row["employees"]
	companies.append(company)

csv_filename = "./f500-companies.csv"
print("Writing " + csv_filename + " file...")
with open(csv_filename, "w", newline='', encoding='utf-8') as csv_file:
	fieldnames = ["name", "industry", "website"]
	writer = csv.DictWriter(csv_file, dialect = csv.excel, fieldnames = fieldnames, quoting = csv.QUOTE_ALL)
	writer.writeheader()
	for company in companies:
		csvrow = {}
		csvrow["name"] = company["name"]
		csvrow["industry"] = company["industry"]
		csvrow["website"] = company["website"]
		print("\twriting data for '" + company["name"] + "'...")
		writer.writerow(csvrow)
	print("\tcomplete...")
print("Closing connections...")
cursor.close()
cnx.close()
print("Complete.")
print()