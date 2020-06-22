import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import scraperwiki
import PyPDF2
import camelot
from os import listdir
import pandas as pd
import numpy as np

files = listdir("pdfs/")

# testfiles = ['COVID-19 - PM 18 April 2020 - State and Territory Update.pdf', 'COVID-19 - PM 17 April 2020 - State and Territory Update.pdf']
testfiles = ["COVID-19 - PM 9 June 2020 - State and Territory Update.pdf"]

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def cleanDataframe(df,filename):


	# df['filename'] = filename
	
	dateStrs = [df[0].iloc[0],  df[0].iloc[1], df[1].iloc[1]]

	dateStr = ""

	print(dateStrs)
	for i, s in enumerate(dateStrs):
		if "1400hrs" in s or "1600" in s:
			dateStr = dateStrs[i]


	# dateStr = df[0].iloc[1]
	print(dateStr)

	if dateStr == "" and filename != 'COVID-19 - PM 17 April 2020 - State and Territory Update.pdf':
		raise NameError('HiThere')
	# 	dateStr = df[1].iloc[1]

	# elif "1400hrs" not in dateStr:
	# 	dateStr = df[0].iloc[0]
	
	dateList = dateStr.split(" ")
	print(dateList)

	if filename != 'COVID-19 - PM 17 April 2020 - State and Territory Update.pdf':
		if dateList[2] == "1600hrs":
			date = dateList[3] + " " + dateList[4] + " 2020"
		elif dateList[-3] != "1600hrs":
			date = dateList[-3] + " " + dateList[-2] + " " + dateList[-1]
		else:
			date = dateList[-2] + " " + dateList[-1] + " 2020"
		date = date.replace(".","")	

		print(date)	
		endPos = df.loc[df[0]=='Total'].index[0] + 1
		startPos = 2
		trimmed = df[startPos:endPos]
		trimmed.columns = ['Source', 'Australia', 'ACT','NSW','NT','QLD','SA','TAS','VIC','WA']
		trimmed = trimmed.drop(2)

		trimmed['Date'] = date
		trimmed['Filename'] = str(filename)
		trimmed = trimmed[trimmed['Australia'].apply(lambda x: x.isnumeric())]
	
		if len(trimmed.index) != 6:
			print("Something is wrong")
		else:	
			trimmed['Source'].iloc[0] = 'Overseas acquired'
			trimmed['Source'].iloc[1] = 'Locally acquired - contact known'
			trimmed['Source'].iloc[2] = 'Locally acquired - contact unknown'
			trimmed['Source'].iloc[3] = 'Locally acquired - interstate travel'
			trimmed['Source'].iloc[4] = 'Under investigation'
			trimmed['Source'].iloc[5] = 'Total'

		return trimmed

	else:
		date = str(filename.split("-")[2].replace("PM","").strip())
		columns = ['Source', 'Australia', 'ACT','NSW','NT','QLD','SA','TAS','VIC','WA']
		trimmed = df.drop(0)
		
		trimmed.columns = columns

		trimmed['Date'] = date
		trimmed['Filename'] = str(filename)
		trimmed = trimmed[trimmed['Australia'].apply(lambda x: x.isnumeric())]
		trimmed = trimmed.append({"Source":'Overseas acquired', "Australia":4209,"ACT":80, "NSW":1733,"NT":24,"QLD":769,"SA":296,"TAS":76,"VIC":774,"WA":458,"Date":date,"Filename":filename}, ignore_index=True)
		
		trimmed['Source'].iloc[0] = 'Locally acquired - contact known'
		trimmed['Source'].iloc[1] = 'Locally acquired - contact unknown'
		trimmed['Source'].iloc[2] = 'Locally acquired - interstate travel'
		trimmed['Source'].iloc[3] = 'Under investigation'
		trimmed['Source'].iloc[4] = 'Total'
		print(trimmed)
		return trimmed

def readPDF(filename):

	pdfReader = PyPDF2.PdfFileReader("pdfs/" + filename)
	
	tables = camelot.read_pdf("pdfs/" + filename, pages="3",  flavor='stream')
	
	# camelot.plot(tables[0], kind='text')
	# plt.show()

	# print(tables[0].df)
	print("Processing", filename)
	clean_df = cleanDataframe(tables[0].df,filename)
		
	data = clean_df.to_dict('records')
	# print(data)
	# print(data[0]['Date'])
	scraperwiki.sqlite.save(unique_keys=["Date","Filename","Source"], data=data,table_name="transmission_source")

fileList = []

doneFiles = scraperwiki.sqlite.select("* from files_done")
# doneFiles = []
doneFileSet = set()

for row in doneFiles:
	doneFileSet.add(row['Filename'])

for file in files:
	if ".pdf" in file:
		print(file)
		if file not in doneFileSet:
			print("Checking...")
			fileList.append({"Filename":file})
			readPDF(file)
		else:
			print("Already done")	

scraperwiki.sqlite.save(unique_keys=["Filename"],data=fileList,table_name="files_done")