# FCFTW Kill Scraper v1.0
# Licensed under the GNU General Public License version 3.0, or any later version
# Please see the full license in the LICENSE file

import urllib2
import httplib
import json
from datetime import datetime, timedelta
import StringIO
import time
import gzip
import csv

corpID = 98007161
starttime = datetime.fromtimestamp(time.mktime(time.strptime("201212010000", "%Y%m%d%H%M")))
endtime = datetime.fromtimestamp(time.mktime(time.strptime("201303070000", "%Y%m%d%H%M")))

killsfile = open('all_kills.csv', 'wb')
killswriter = csv.DictWriter(killsfile, ['time', 'type', 'involved'])
killswriter.writeheader()

statsfile = open('kill_stats.csv', 'wb')
statswriter = csv.DictWriter(statsfile, ['hour', 'activity'])
statswriter.writeheader()

statsdict = {}
for x in [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]:
    statsdict.update({str(x): 0})

print "Beginning run..."

def build_url(start, end):
    return "https://zkillboard.com/api/corporationID/%s/startTime/%s/endTime/%s/no-items/" % (corpID, start.strftime("%Y%m%d%H%M"), end.strftime("%Y%m%d%H%M"))

def get_data(url):
    """
    Takes a zKillboard API URL and returns parsed values as a list of dicts.
    """
    time.sleep(5)
    request = urllib2.Request(url)
    request.add_header('User-Agent', 'FCFTW Kill Parser')
    request.add_header('Accept-encoding', 'gzip')
    opener = urllib2.build_opener()
    comp_data = opener.open(request)
    gzip_data = comp_data.read()
    stream = StringIO.StringIO(gzip_data)
    gzip_handler = gzip.GzipFile(fileobj=stream)
    opener.close()


    return json.loads(gzip_handler.read())

def write_stats():
    for x in [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]:
        statswriter.writerow({'hour': str(x),'activity': statsdict[str(x)]})

def process_data(data):
    """
    Get stats from parsed zKillboard data.
    """
    for kill in data:
        kill_time = kill['killTime']
        if kill['victim']['corporationID'] == corpID:
            kill_type = "LOSS"
        else:
            kill_type = "KILL"
        involved = len(kill['attackers'])
        killswriter.writerow({'time': kill_time, 'type': kill_type, 'involved': involved})
        kill_hour = datetime.fromtimestamp(time.mktime(time.strptime(kill_time, "%Y-%m-%d %H:%M:%S"))).hour
        statsdict[str(kill_hour)] += 1


start = starttime
while start < endtime:
    end = start + timedelta(weeks=1)
    print "Attempting to process week: %s to %s" % (start, end)
    data = get_data(build_url(start, end))
    if len(data) < 200:
        print "Less than 200 kills, processing..."
        process_data(data)
    else:
        print  "More than 200 kills, breaking down to days...."
        day_start = start
        while day_start < end:
            day_end = day_start + timedelta(days=1)
            print "Attempting to process day: %s to %s" % (day_start, day_end)
            day_data = get_data(build_url(day_start, day_end))
            process_data(day_data)
            day_start = day_end
    start = end

print "Writing summary statistics..."
write_stats()
print "Run complete."

