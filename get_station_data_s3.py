import urllib2
import boto
import json
import pandas as pd
import time
import datetime
from decimal import Decimal
from pandas.io.json import json_normalize
from boto.dynamodb2.table import Table
from boto.s3.key import Key

# Summary
# 1. Get citibike station data
# 2. Process station data JSON and turn it into a CSV (with header removed)
# 3. Upload csv data to S3. S3 data file by day. If current day exists, append, otherwise, write new

def timeStrToTimeSlug(tmstr):
  tmobj = datetime.datetime.strptime(tmstr, '%Y-%m-%d %I:%M:%S %p')
  return tmobj.strftime('%Y-%m-%d-%H-%M-%S')

def timeStrToDateSlug(tmstr):
  tmobj = datetime.datetime.strptime(tmstr, '%Y-%m-%d %I:%M:%S %p')
  return tmobj.strftime('%Y-%m-%d')

def slugToTime(slug):
  return datetime.datetime.strptime(slug, '%Y-%m-%d-%H-%M-%S')


api_url = "https://www.citibikenyc.com/stations/json/"
json_str = urllib2.urlopen(api_url).read()
json_obj = json.loads(json_str)

pingtime = json_obj['executionTime']
slug = timeStrToDateSlug(pingtime)
print "getting station data at %s" % slug

stations_data = json_obj['stationBeanList']
print "number of stations this ping %d" % len(stations_data)

for station in stations_data:
  station['pingtime'] = pingtime
    
df = json_normalize(stations_data)
new_data = df.to_csv(header=False, index=False)

conn = boto.connect_s3()
bucket = conn.get_bucket('citibike')
filename = "citibike-stations-%s.csv" % slug
todays_key = bucket.get_key(filename)

if todays_key:
  print "%s exists: appending data" % filename
  existing_data = todays_key.get_contents_as_string()
  combined_data = existing_data + new_data
  todays_key.set_contents_from_string(combined_data)
else:
  print "%s does not exist: writing new file" % filename
  k = Key(bucket)
  k.key = filename
  k.set_contents_from_string(new_data)

# Logging ping activity
log_key = bucket.get_key("activity-log.txt")
existing_log = log_key.get_contents_as_string()
increment_log = timeStrToTimeSlug(pingtime) + "\n"
new_log = increment_log + existing_log 
log_key.set_contents_from_string(new_log)

