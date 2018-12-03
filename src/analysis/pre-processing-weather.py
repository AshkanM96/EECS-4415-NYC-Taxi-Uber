import csv
import sys
import fileinput
from dateutil.parser import parse


# check if the data is between 2014 April- 2014 July or if the date between 2015 April - 2015 July
def withinDate(date):
	#parse the date
	temp=parse(date)
	#check that the date fall within the expected range
	if(temp.year == 2014 or temp.year == 2015):
		if(temp.month >= 4 and temp.month <= 7):
			return True

# check if the city is NYC
def city_is_NewYork(city):
	if("New York City" in city):
		return True
# filter the weather event, the weather can have three states Snow,Rain and Normal
def filter_event(event):
	if("Snow" in event):
		return "Snow"
	if("Rain" in event):
		return "Rain"
	return "normal"	

# read the file that contains the raw data using stdin
with sys.stdin as csv_file:
	weather_file=  csv.reader(csv_file, delimiter = ",")
	# skip the header
	next(weather_file)
	# iterate through every row
	for row in weather_file:
		#check if the city is New York and the date is between 2014-4 and 2014-7 or 2015-4 and 2015-7
		if(withinDate(row[1]) and city_is_NewYork(row[24])):
			#print the date ,mean temperature, weather event  
			print(row[1]+","+row[3]+","+filter_event(row[22]))

			# After this program finishes, we direct the output to a text file, then convert the text file to a csv file.
			