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
verified_counter = 0
updated_counter = 0
print("Verifying companies...")
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
	print("\tverifying '" + company["name"] + "' (#" + str(company["id"]) + ")...")
	if not company["revenue"] or not company["hq_location"] or not company["sector"] or not company["industry"] or not company["website"] or not company["employees"]:
		print("\t\tsome data for '" + company["name"] + "' (#" + str(company["id"]) + ") is missing...")
		print("\t\trequesting '" + company["f500_url"] + "'...")
		company_response = http.request("GET", company["f500_url"])
		print("\t\t\tparsing...")
		company_soup = BeautifulSoup(company_response.data, "html5lib")
		cards = company_soup.find_all("div", class_ = "company-info-card-data")
		#print(cards)
		company["ceo"] = cards[0].p.text.strip()
		company["ceo_title"] = cards[1].p.text.strip()
		company["sector"] = cards[2].p.text.strip()
		company["industry"] = cards[3].p.text.strip()
		company["hq_location"] = cards[4].p.text.strip()
		company["website"] = cards[5].a.text.strip()
		company["employees"] = re.sub("[^\d\.]", "", cards[7].p.text.strip())
		#print(company)
		print("\t\t\tupdating database...")
		company_update_query = "UPDATE companies c SET c.`ceo` = %(ceo)s, c.`ceo_title` = %(ceo_title)s, c.`sector` = %(sector)s, c.`industry` = %(industry)s, c.`hq_location` = %(hq_location)s, c.`website` = %(website)s, c.`employees` = %(employees)s WHERE `id` = %(id)s LIMIT 1"
		#company_update_query = "UPDATE companies c SET c.`ceo` = %(ceo)s WHERE `id` = %(id)s LIMIT 1"
		cursor.execute(company_update_query, company)
		cnx.commit()
		print("\t\t\tcomplete...")
		verified_counter += 1
		updated_counter += 1
		print()
		if updated_counter % 60 != 0:
			print("\tsleeping 60 seconds...")
			time.sleep(60)
		else:
			print("\tcollected data from " + str(updated_counter) + " pages: sleeping 15 minutes...")
			time.sleep(60 * 15)
	else:
		print("\t\tdata for '" + company["name"] + "' (#" + str(company["id"]) + ") is set...")
		verified_counter += 1
	companies.append(company)
	print()
	print("\t" + str(verified_counter) + " records verified, " + str(updated_counter) + " records updated so far...")
	print()
print("Verification complete: " + str(verified_counter) + " companies verified, " + str(updated_counter) + " companies updated.")
print()
"""
csv_filename = "./f500-companies.csv"
print("Writing " + csv_filename + " file...")
with open(csv_filename, "w", newline='', encoding='utf-8') as csv_file:
	fieldnames = ["name", "industry", "website"]
	writer = csv.DictWriter(csv_file, fieldnames = fieldnames, quoting = csv.QUOTE_ALL)
	#for company in companies:
	print("\twriting records...")
	writer.writerows(companies)
	print("\tcomplete...")
"""
print("Closing connections...")
cursor.close()
cnx.close()
print("Complete.")
print()