import schedule
import time
import pymysql
import requests
import matplotlib as mpl
import numpy as np
import timeit
import pandas as pd
import smtplib
import datetime
from datetime import datetime as dt
from operator import itemgetter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import letter
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from email.mime.text import MIMEText

dbs = ['Pisner_24', 'Pisner_29']

# Android
db1 = pymysql.connect(host="r33data-cluster.cluster-cop1qqqzffyg.us-east-2.rds.amazonaws.com",
                     user="IMHR",
                     passwd="7L4ypF)fqicX&FxdRnHc",
                     db=dbs[0], connect_timeout=1200)

# iOS
db2 = pymysql.connect(host="r33data-cluster.cluster-cop1qqqzffyg.us-east-2.rds.amazonaws.com",
                     user="IMHR",
                     passwd="7L4ypF)fqicX&FxdRnHc",
                     db=dbs[1], connect_timeout=1200)

def fetch_ids_droid():
    cur = db1.cursor()
    cur.execute("SELECT device_id FROM aware_device;")
    all_items = cur.fetchall()
    device_ids = [item[0] for item in all_items]
    cur = db1.cursor()
    cur.execute("SELECT label FROM aware_device;")
    all_items = cur.fetchall()
    labels = [item[0] for item in all_items]
    cur = db1.cursor()
    cur.execute("SELECT timestamp FROM aware_device;")
    all_items = cur.fetchall()
    timestamps = [item[0] for item in all_items]
    timestamps_con = [dt.fromtimestamp(timestamp / 1000.0).strftime("%x %X").split(' ')[0] for timestamp in
                      timestamps]
    return device_ids, labels, timestamps_con

def fetch_ids_ios():
    cur = db2.cursor()
    cur.execute("SELECT device_id FROM aware_device;")
    all_items = cur.fetchall()
    device_ids = [item[0] for item in all_items]
    cur = db2.cursor()
    cur.execute("SELECT label FROM aware_device;")
    all_items = cur.fetchall()
    labels = [item[0] for item in all_items]
    cur = db2.cursor()
    cur.execute("SELECT timestamp FROM aware_device;")
    all_items = cur.fetchall()
    timestamps = [item[0] for item in all_items]
    timestamps_con = [dt.fromtimestamp(timestamp / 1000.0).strftime("%x %X").split(' ')[0] for timestamp in
                      timestamps]
    return device_ids, labels, timestamps_con


# Fetch device ids
def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z

def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta


# Set device id input
def check_all(data_inventory_dict, device_id_check):
    start_time = timeit.default_timer()
    five_min_ints = 2016
    hour_ints = 168
    min_ints = 10080
    print("%s%s%s" % ('Checking device id: ', device_id_check, '...'))
    if device_id_check in device_ids_all:
        data_inventory_dict['subject_id'] = id_dict[device_id_check]
        data_inventory_dict['device_id_exists'] = True

        # Calls
        # Check droid db
        cur = db1.cursor(pymysql.cursors.DictCursor)
        query = 'SELECT * FROM calls WHERE (device_id = %s) AND (timestamp >= %s)'
        cur.execute(query, (device_id_check, date_min_wk_unix,))
        response = cur.fetchall()
        if response:
            print('Call data found in android passive-all db. Fetching...')
            for i in range(len(response)):
                del response[i]['_id']
                del response[i]['device_id']
                response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
            print(response)
            data_inventory_dict['call_data'] = response
        else:
            # Check ios db
            cur = db2.cursor(pymysql.cursors.DictCursor)
            query = 'SELECT * FROM calls WHERE (device_id = %s) AND (timestamp >= %s)'
            cur.execute(query, (device_id_check, date_min_wk_unix,))
            response = cur.fetchall()
            if response:
                print('Call data found in ios passive-all db. Fetching...')
                for i in range(len(response)):
                    del response[i]['_id']
                    del response[i]['device_id']
                    response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
                print(response)
                data_inventory_dict['call_data'] = response
            else:
                print('Call data missing!')
                data_inventory_dict['call_data'] = dict()
                data_inventory_dict['call_data']['call_duration'] = str(np.nan)
                data_inventory_dict['call_data']['call_type'] = str(np.nan)
        print('\n\n')

        # Locations
        cur = db1.cursor(pymysql.cursors.DictCursor)
        query = 'SELECT * FROM locations WHERE (device_id = %s) AND (timestamp >= %s)'
        cur.execute(query, (device_id_check, date_min_wk_unix,))
        response = cur.fetchall()
        if response:
            print('Location data found in android passive-all db. Fetching...')
            for i in range(len(response)):
                del response[i]['_id']
                del response[i]['device_id']
                response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
            print(response)
            data_inventory_dict['location_data'] = response
            data_inventory_dict['location_%_weekly_samples_exp'] = str(float(len(response))/float(min_ints))
        else:
            # Check ios db
            cur = db2.cursor(pymysql.cursors.DictCursor)
            query = 'SELECT * FROM locations WHERE (device_id = %s) AND (timestamp >= %s)'
            cur.execute(query, (device_id_check, date_min_wk_unix,))
            response = cur.fetchall()
            if response:
                for i in range(len(response)):
                    del response[i]['_id']
                    del response[i]['device_id']
                    response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
                print(response)
                data_inventory_dict['location_data'] = response
                data_inventory_dict['location_%_weekly_samples_exp'] = 0
            else:
                print('Location data missing!')
                data_inventory_dict['location_data'] = dict()
                data_inventory_dict['location_data']['double_altitude'] = str(np.nan)
                data_inventory_dict['location_data']['double_bearing'] = str(np.nan)
                data_inventory_dict['location_data']['double_altitude'] = str(np.nan)
                data_inventory_dict['location_data']['double_bearing'] = str(np.nan)
                data_inventory_dict['location_data']['double_latitude'] = str(np.nan)
                data_inventory_dict['location_data']['double_longitude'] = str(np.nan)
                data_inventory_dict['location_data']['double_speed'] = str(np.nan)
                data_inventory_dict['location_data']['label'] = str(np.nan)
                data_inventory_dict['location_data']['provider'] = str(np.nan)
                data_inventory_dict['location_%_weekly_samples_exp'] = str(np.nan)
        print('\n\n')

        # Ambient noise
        cur = db1.cursor(pymysql.cursors.DictCursor)
        query = 'SELECT * FROM plugin_ambient_noise WHERE (device_id = %s) AND (timestamp >= %s)'
        cur.execute(query, (device_id_check, date_min_wk_unix,))
        response = cur.fetchall()
        if response:
            print('Ambient noise data found in android passive-all db. Fetching...')
            for i in range(len(response)):
                del response[i]['_id']
                del response[i]['device_id']
                response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
            print(response)
            data_inventory_dict['ambient_noise'] = response
            data_inventory_dict['ambient_noise_%_weekly_samples_exp'] = str(float(len(response)) / float(five_min_ints))
        else:
            # Check ios db
            cur = db2.cursor(pymysql.cursors.DictCursor)
            query = 'SELECT * FROM plugin_ambient_noise WHERE (device_id = %s) AND (timestamp >= %s)'
            cur.execute(query, (device_id_check, date_min_wk_unix,))
            response = cur.fetchall()
            if response:
                print('Ambient noise data found in ios passive-all db. Fetching...')
                for i in range(len(response)):
                    del response[i]['_id']
                    del response[i]['device_id']
                    response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
                print(response)
                data_inventory_dict['ambient_noise_data'] = response
                data_inventory_dict['ambient_noise_%_weekly_samples_exp'] = 0
            else:
                print('Ambient noise data missing!')
                data_inventory_dict['ambient_noise_data'] = dict()
                data_inventory_dict['ambient_noise_data']['double_frequency'] = str(np.nan)
                data_inventory_dict['ambient_noise_data']['double_decibels'] = str(np.nan)
                data_inventory_dict['ambient_noise_%_weekly_samples_exp'] = str(np.nan)
        print('\n\n')

        # Screen
        cur = db1.cursor(pymysql.cursors.DictCursor)
        query = 'SELECT * FROM screen WHERE (device_id = %s) AND (timestamp >= %s)'
        cur.execute(query, (device_id_check, date_min_wk_unix,))
        response = cur.fetchall()
        if response:
            print('Screen useage data found in android passive-all db. Fetching...')
            for i in range(len(response)):
                del response[i]['_id']
                del response[i]['device_id']
                response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
            print(response)
            data_inventory_dict['screen_useage'] = response
        else:
            # Check ios db
            cur = db2.cursor(pymysql.cursors.DictCursor)
            query = 'SELECT * FROM screen WHERE (device_id = %s) AND (timestamp >= %s)'
            cur.execute(query, (device_id_check, date_min_wk_unix,))
            response = cur.fetchall()
            if response:
                print('Screen useage data found in ios passive-all db. Fetching...')
                for i in range(len(response)):
                    del response[i]['_id']
                    del response[i]['device_id']
                    response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
                print(response)
                data_inventory_dict['screen_useage'] = response
            else:
                print('Screen useage data missing!')
                data_inventory_dict['screen_useage'] = dict()
                data_inventory_dict['screen_useage']['screen_status'] = str(np.nan)
        print('\n\n')
    else:
        print('Device missing!')
    print("%s%s" % ('Finished query in: ', timeit.default_timer() - start_time))
    return data_inventory_dict

today = datetime.date.today()
month_margin = datetime.timedelta(days=30)
date_min = today - month_margin
date_max = today + month_margin
date_min = date_min.strftime("%m/%d/%y")
date_max = date_max.strftime("%m/%d/%y")

week_margin = datetime.timedelta(days=7)
date_wk_min = today - week_margin
date_wk_max = today + week_margin
date_min_wk_unix = time.mktime(date_wk_min.timetuple())
date_max_wk_unix = time.mktime(date_wk_max.timetuple())
# five_min_ints = list(perdelta(date_wk_min, date_wk_max, datetime.timedelta(minutes=5)))
# hour_ints = list(perdelta(date_wk_min, date_wk_max, datetime.timedelta(hours=1)))
[device_ids_droid, labels_droid, droid_timestamps] = fetch_ids_droid()
ix_today_droid = [i for i, x in enumerate(droid_timestamps) if ((x <= date_max) and (x <= date_min))]
labels_droid = [labels_droid[i] for i in ix_today_droid]
device_ids_droid = [device_ids_droid[i] for i in ix_today_droid]
if today.strftime("%m/%d/%y") in droid_timestamps:
    print('New android device added today!')
else:
    print('No new android devices added today!')
[device_ids_ios, labels_ios, ios_timestamps] = fetch_ids_ios()
ix_today_ios = [i for i, x in enumerate(ios_timestamps) if ((x <= date_max) and (x <= date_min))]
labels_ios = [labels_ios[i] for i in ix_today_ios]
device_ids_ios = [device_ids_ios[i] for i in ix_today_ios]
if today.strftime("%m/%d/%y") in ios_timestamps:
    print('New ios device added today!')
else:
    print('No new ios devices added today!')
id_dict1 = dict(zip(device_ids_droid, labels_droid))
id_dict2 = dict(zip(device_ids_ios, labels_ios))
if len(device_ids_droid) > 0 and len(device_ids_ios) > 0:
    device_ids_all = device_ids_droid + device_ids_ios
    id_dict = merge_two_dicts(id_dict1, id_dict2)
    empty_list = False
elif len(device_ids_droid) > 0:
    device_ids_all = device_ids_droid
    id_dict = id_dict1
    empty_list = False
elif len(device_ids_ios) > 0:
    device_ids_all = device_ids_ios
    id_dict = id_dict2
    empty_list = False
else:
    print('No new devices added today!')
    empty_list = True

# Begin id checkdict_all_devices = dict()
for device_id_check in device_ids_all:
    if empty_list is not True:
        # Android
        db1 = pymysql.connect(host="r33data-cluster.cluster-cop1qqqzffyg.us-east-2.rds.amazonaws.com",
                              user="IMHR",
                              passwd="7L4ypF)fqicX&FxdRnHc",
                              db=dbs[0], connect_timeout=720)
        # iOS
        db2 = pymysql.connect(host="r33data-cluster.cluster-cop1qqqzffyg.us-east-2.rds.amazonaws.com",
                              user="IMHR",
                              passwd="7L4ypF)fqicX&FxdRnHc",
                              db=dbs[1], connect_timeout=720)
        data_inventory_dict = dict()
        data_inventory_dict = check_all(data_inventory_dict, device_id_check)

        missing_items = []
        missing_measures = []
        measures = list(data_inventory_dict.keys())[2:]
        for measure in measures:
            for key, value in data_inventory_dict[measure].items():
                if value == np.nan or value == 0:
                    print(key)
                    print(value)
                    missing_items.append(key)
                    missing_measures.append(measure)
        Subject = "%s%s%s%s" % ('TEST AWARE DATA COMPLETION CHECK for subject: ', id_dict[device_id_check], ' at ',

                                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))
        if len(missing_items) == 0:
            message_text = 'ALL MEASURES ACQUIRED! PASS.'
        else:
            message_text = str(len(missing_items)) + ' MISSING VARIABLES AND ' + str(len(missing_measures)) + \
                           ' MISSING MEASURES FOUND FOR: subject ' + str(id_dict[device_id_check]) + ':\n\nMEASURES CONTAINING MISSING VARIABLES:' \
                           + str(list(set(missing_measures))) + '\n\nMISSING VARIABLES:' + str(missing_items) + '\n\n' \
                           + str(data_inventory_dict)
        message = 'Subject: {}\n\n{}'.format(Subject, message_text)
    else:
        Subject = "%s%s%s" % ('TEST AWARE DATA COMPLETION CHECK COMPLETED AT: ',
                                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), '. NO NEW DEVICES FOUND!')
        message_text = None
        message = 'Subject: {}\n\n{}'.format(Subject, message_text)
    server = smtplib.SMTP('smtp.gmail.com', 587)  # port 465 or 587
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login('r33aware@gmail.com', 'beevers01!')
    server.sendmail(to_addrs=['dpisner@utexas.edu'], from_addr='r33aware@gmail.com', msg=message)
    #'semeon.risom@gmail.com', 'keanjhsu@utexas.edu', 'kdcaffey@utexas.edu', 'jocelyn.labrada@utexas.edu'
    server.close()
