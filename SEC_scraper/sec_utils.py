# This script will download all the 10-K
# provided that of company symbol and its cik code.

from bs4 import BeautifulSoup
import random
import requests
import os
import time

headers = {"User-Agent": "bdai_db@bdai.snu.ac.kr"} # for snu bdai members

class SecCrawler():

	def __init__(self):
		self.hello = "Welcome to SEC Cralwer!"

	def download_files(self, companyCode, cik, docList, filing_type):
		dataList = [] # Save every text document 

		for j in range(len(docList)):
			base_url = docList[j]
			print(base_url)
			r = requests.get(base_url, headers = headers)
			data = r.text
			#path = f"datas/{docNameList[j]}"

			#soup = BeautifulSoup(data, 'html.parser')
			#tt = soup.get_text
			#filename = open(path, "wb")
			#filename.write(tt)
			dataList.append(data)

		return dataList

	def filing_10K(self, companyCode, cik, priorto, count):
		#generate the url to crawl
		base_url = f"http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=10-K&dateb={priorto}&owner=exclude&output=xml&count={count}"
		print(f"started 10-K for code: {companyCode}")

		r = requests.get(base_url, headers = headers)
		data = r.text # get html code 

		time.sleep(random.uniform(1, 3))

		soup = BeautifulSoup(data, features = "lxml") # Initializing to crawl again
		linkList=[] # List of all links from the CIK page
		# If the link is .htm convert it to .html
		for filing in soup.find_all('filing'):
			type_tag = filing.find('type')
			link_tag = filing.find('filinghref')

			# only search for 10-K, not for 10-K/A
			if type_tag.string == "10-K":
				URL = link_tag.string
				if link_tag.string.split(".")[len(link_tag.string.split("."))-1] == "htm":
					URL += "l"
				linkList.append(URL)

		print(f"Number of files to download : {len(linkList)}")
		print("Start downloading....")

		docList = [] # List of URL to the text documents

		for k in range(len(linkList)):
			requiredURL = str(linkList[k])[0:len(linkList[k])-11] # https://www.sec.gov/Archives/edgar/data/1278021/000095017023003824/0000950170-23-003824.index.html -> hhtps://~~~23-003824.txt 
			txtdoc = requiredURL+".txt"
			docList.append(txtdoc)

		print(docList)
		try:
			result = self.download_files(companyCode, cik, docList, '10-K')
			return result
		except:
			print("Not able to save the file :( ")

		print(f"Successfully downloaded all {len(linkList)} files")