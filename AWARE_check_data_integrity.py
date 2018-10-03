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
import itertools
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

def fetch_ids_droid(date_min_unix):
    cur = db1.cursor(pymysql.cursors.DictCursor)
    query = 'SELECT device_id FROM aware_device WHERE (timestamp >= %s)'
    cur.execute(query, (date_min_unix,))
    all_items = cur.fetchall()
    device_ids = [list(item.values())[0] for item in all_items]

    cur = db1.cursor(pymysql.cursors.DictCursor)
    query = 'SELECT label FROM aware_device WHERE (timestamp >= %s)'
    cur.execute(query, (date_min_unix,))
    all_items = cur.fetchall()
    labels = [list(item.values())[0] for item in all_items]

    cur = db1.cursor(pymysql.cursors.DictCursor)
    query = 'SELECT timestamp FROM aware_device WHERE (timestamp >= %s)'
    cur.execute(query, (date_min_unix,))
    all_items = cur.fetchall()
    timestamps = [list(item.values())[0] for item in all_items]
    timestamps_con = [dt.fromtimestamp(timestamp / 1000.0).strftime("%x %X").split(' ')[0] for timestamp in
                      timestamps]
    return device_ids, labels, timestamps_con


def fetch_ids_ios(date_min_unix):
    cur = db2.cursor(pymysql.cursors.DictCursor)
    query = 'SELECT device_id FROM aware_device WHERE (timestamp >= %s)'
    cur.execute(query, (date_min_unix,))
    all_items = cur.fetchall()
    device_ids = [list(item.values())[0] for item in all_items]

    cur = db2.cursor(pymysql.cursors.DictCursor)
    query = 'SELECT label FROM aware_device WHERE (timestamp >= %s)'
    cur.execute(query, (date_min_unix,))
    all_items = cur.fetchall()
    labels = [list(item.values())[0] for item in all_items]

    cur = db2.cursor(pymysql.cursors.DictCursor)
    query = 'SELECT timestamp FROM aware_device WHERE (timestamp >= %s)'
    cur.execute(query, (date_min_unix,))
    all_items = cur.fetchall()
    timestamps = [list(item.values())[0] for item in all_items]
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

def date_generator(from_date):
  while True:
    yield dt.date(from_date)
    from_date = from_date - datetime.timedelta(days=1)

# Set device id input
def check_device_enroll(data_inventory_dict, device_id_check, date_min_wk_unix):
    print("%s%s%s" % ('Checking device id: ', device_id_check, '...'))
    if device_id_check in device_ids_all:
        data_inventory_dict['subject_id'] = id_dict[device_id_check]
        data_inventory_dict['device_id_exists'] = True

        # Device Basic Info
        # Check droid db
        cur = db1.cursor(pymysql.cursors.DictCursor)
        query = 'SELECT * FROM aware_device WHERE (device_id = %s) AND (timestamp >= %s)'
        cur.execute(query, (device_id_check, date_min_wk_unix,))
        response = cur.fetchall()
        if response:
            print('Device data found in android passive-all db. Fetching...')
            for i in range(len(response)):
                del response[i]['_id']
                del response[i]['device_id']
                response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
            # print(response)
            data_inventory_dict['device_data'] = response
        else:
            # Check ios db
            cur = db2.cursor(pymysql.cursors.DictCursor)
            query = 'SELECT * FROM aware_device WHERE (device_id = %s) AND (timestamp >= %s)'
            cur.execute(query, (device_id_check, date_min_wk_unix,))
            response = cur.fetchall()
            if response:
                print('Device data found in ios passive-all db. Fetching...')
                for i in range(len(response)):
                    del response[i]['_id']
                    del response[i]['device_id']
                    response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
                # print(response)
                data_inventory_dict['device_data'] = response
            else:
                print('Device data missing!')
                data_inventory_dict['device_data'] = dict()
                data_inventory_dict['device_data']['board'] = str(np.nan)
                data_inventory_dict['device_data']['brand'] = str(np.nan)
                data_inventory_dict['device_data']['build_id'] = str(np.nan)
                data_inventory_dict['device_data']['device'] = str(np.nan)
                data_inventory_dict['device_data']['hardware'] = str(np.nan)
                data_inventory_dict['device_data']['label'] = str(np.nan)
                data_inventory_dict['device_data']['manufacturer'] = str(np.nan)
                data_inventory_dict['device_data']['model'] = str(np.nan)
                data_inventory_dict['device_data']['product'] = str(np.nan)
                data_inventory_dict['device_data']['release'] = str(np.nan)
                data_inventory_dict['device_data']['release_type'] = str(np.nan)
                data_inventory_dict['device_data']['sdk'] = str(np.nan)
                data_inventory_dict['device_data']['serial'] = str(np.nan)
                data_inventory_dict['device_data']['timestamp'] = str(np.nan)
    return data_inventory_dict


def check_all(data_inventory_dict, device_id_check, date_min_wk_unix):
    start_time = timeit.default_timer()
    five_min_ints = 2016
    min_ints = 10080
    if device_id_check in device_ids_all:
        # Device Debug
        # Check droid db
        cur = db1.cursor(pymysql.cursors.DictCursor)
        query = 'SELECT * FROM aware_debug WHERE (device_id = %s) AND (timestamp >= %s) AND (type > %s)'
        cur.execute(query, (device_id_check, date_min_wk_unix, '1',))
        response = cur.fetchall()
        if response:
            print('Device debug data found in android passive-all db. Fetching...')
            for i in range(len(response)):
                del response[i]['_id']
                del response[i]['device_id']
                response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
            # print(response)
            data_inventory_dict['device_debug'] = response
        else:
            # Check ios db
            cur = db2.cursor(pymysql.cursors.DictCursor)
            query = 'SELECT * FROM aware_debug WHERE (device_id = %s) AND (timestamp >= %s) AND (type > %s)'
            cur.execute(query, (device_id_check, date_min_wk_unix, '1'))
            response = cur.fetchall()
            if response:
                print('Device debug data found in ios passive-all db. Fetching...')
                for i in range(len(response)):
                    del response[i]['_id']
                    del response[i]['device_id']
                    response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
                # print(response)
                data_inventory_dict['device_debug'] = response
            else:
                print('Debug data missing!')
                data_inventory_dict['device_debug'] = dict()
                data_inventory_dict['device_debug']['event'] = str(np.nan)
                data_inventory_dict['device_debug']['type'] = str(np.nan)
                data_inventory_dict['device_debug']['label'] = str(np.nan)
                data_inventory_dict['device_debug']['network'] = str(np.nan)
                data_inventory_dict['device_debug']['app_version'] = str(np.nan)
                data_inventory_dict['device_debug']['device'] = str(np.nan)
                data_inventory_dict['device_debug']['os'] = str(np.nan)
                data_inventory_dict['device_debug']['battery'] = str(np.nan)
                data_inventory_dict['device_debug']['battery_state'] = str(np.nan)

        # Locations
        cur = db1.cursor(pymysql.cursors.DictCursor)
        query = 'SELECT * FROM locations WHERE (device_id = %s) AND (timestamp >= %s) AND (provider = %s)'
        cur.execute(query, (device_id_check, date_min_wk_unix, 'fused',))
        response = cur.fetchall()
        if response:
            print('Location data found in android passive-all db. Fetching...')
            for i in range(len(response)):
                del response[i]['_id']
                del response[i]['device_id']
                response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
            # print(response)
            data_inventory_dict['location_data'] = response
            data_inventory_dict['location_%_weekly_samples_exp'] = str(float(len(response))/float(min_ints))
        else:
            # Check ios db
            cur = db2.cursor(pymysql.cursors.DictCursor)
            query = 'SELECT * FROM locations WHERE (device_id = %s) AND (timestamp >= %s) AND (provider = %s)'
            cur.execute(query, (device_id_check, date_min_wk_unix, 'fused',))
            response = cur.fetchall()
            if response:
                for i in range(len(response)):
                    del response[i]['_id']
                    del response[i]['device_id']
                    response[i]['timestamp'] = dt.fromtimestamp(response[i]['timestamp'] / 1000.0).strftime("%x %X")
                # print(response)
                data_inventory_dict['location_data'] = response
                data_inventory_dict['location_%_weekly_samples_exp'] = str(float(len(response))/float(min_ints))
            else:
                print('Location data missing!')
                data_inventory_dict['location_data'] = dict()
                data_inventory_dict['location_data']['double_altitude'] = str(np.nan)
                data_inventory_dict['location_data']['double_bearing'] = str(np.nan)
                data_inventory_dict['location_data']['double_latitude'] = str(np.nan)
                data_inventory_dict['location_data']['double_longitude'] = str(np.nan)
                data_inventory_dict['location_data']['double_speed'] = str(np.nan)
                data_inventory_dict['location_data']['label'] = str(np.nan)
                data_inventory_dict['location_data']['provider'] = str(np.nan)
                data_inventory_dict['location_%_weekly_samples_exp'] = str(np.nan)

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
            # print(response)
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
                # print(response)
                data_inventory_dict['ambient_noise_data'] = response
                data_inventory_dict['ambient_noise_%_weekly_samples_exp'] = 0
            else:
                print('Ambient noise data missing!')
                data_inventory_dict['ambient_noise_data'] = dict()
                data_inventory_dict['ambient_noise_data']['double_frequency'] = str(np.nan)
                data_inventory_dict['ambient_noise_data']['double_decibels'] = str(np.nan)
                data_inventory_dict['ambient_noise_%_weekly_samples_exp'] = str(np.nan)

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
            # print(response)
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
                # print(response)
                data_inventory_dict['screen_useage'] = response
            else:
                print('Screen useage data missing!')
                data_inventory_dict['screen_useage'] = dict()
                data_inventory_dict['screen_useage']['screen_status'] = str(np.nan)
    else:
        print('Device missing!')
    print("%s%s" % ('Finished query in: ', timeit.default_timer() - start_time))
    return data_inventory_dict

today = datetime.date.today()
month_margin = datetime.timedelta(days=30)
week_margin = datetime.timedelta(days=7)
date_min = today - month_margin - week_margin
date_max = today + month_margin
date_min_str = date_min.strftime("%m/%d/%y")
date_max_str = date_max.strftime("%m/%d/%y")

date_min_unix = int(time.mktime(date_min.timetuple()))*1000
date_max_unix = int(time.mktime(date_max.timetuple()))*1000
date_wk_min = today - week_margin
date_wk_max = today + week_margin
date_min_wk_unix = int(time.mktime(date_wk_min.timetuple()))
date_max_wk_unix = int(time.mktime(date_wk_max.timetuple()))
# five_min_ints = list(perdelta(date_wk_min, date_wk_max, datetime.timedelta(minutes=5)))
# hour_ints = list(perdelta(date_wk_min, date_wk_max, datetime.timedelta(hours=1)))

[device_ids_droid, labels_droid, droid_timestamps] = fetch_ids_droid(date_min_unix)
intro_msg = []
if today.strftime("%m/%d/%y") in droid_timestamps:
    intro_msg.append('New android device added today!')
else:
    intro_msg.append('No new android devices added today!')
[device_ids_ios, labels_ios, ios_timestamps] = fetch_ids_ios(date_min_unix)

if today.strftime("%m/%d/%y") in ios_timestamps:
    intro_msg.append('New ios device added today!')
else:
    intro_msg.append('No new ios devices added today!')
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
    intro_msg.append('No new devices added today!')
    empty_list = True

# Begin id checkdict_all_devices = dict()
message_text = str(intro_msg[0]) + '\n' + str(intro_msg[1])
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
        data_inventory_dict = check_device_enroll(data_inventory_dict, device_id_check, date_min_wk_unix)
        try:
            enroll_date_raw = data_inventory_dict['device_data'][0]['timestamp'].split(' ')[0]
        except:
            continue
        enroll_date = dt.strptime(enroll_date_raw, "%m/%d/%y")
        now = datetime.datetime.now()
        month_margin = datetime.timedelta(days=30)
        week_margin = datetime.timedelta(days=7)
        date_study_end = enroll_date + month_margin
        date_week_start = now - week_margin
        week_dates = list(itertools.islice(date_generator(now), 7))

        data_inventory_dict = check_all(data_inventory_dict, device_id_check, date_min_wk_unix)
        print('\n\n\n\nEnrollment in AWARE for R33 Study: ' + enroll_date_raw + ' and ' + date_study_end.strftime("%m/%d/%y") + '...')

        missing_items = []
        missing_measures = []
        measures = list(data_inventory_dict.keys())[2:]
        measures_dicts = [m for m in measures if ('%' not in m) and m is not 'device_data' and m is not 'device_debug']
        for measure in measures_dicts:
            for key, value in data_inventory_dict[measure][0].items():
                if value == np.nan or value == 'nan' or value == '0' or value == 0:
                    #print('Missing ' + measure + '...')
                    missing_items.append(key)
                    missing_measures.append(measure)
            timestamp_list = []
            for samp in range(len(data_inventory_dict[measure])):
                timestamp_list.append(data_inventory_dict[measure][samp]['timestamp'])
            date_list = set([date.split(' ')[0] for date in timestamp_list])
            datetime_list = [dt.strptime(date, "%m/%d/%y") for date in list(date_list)]
            date_wk_rec = sorted([dt.date(dt_y) for dt_y in datetime_list if dt_y > date_week_start])
            wk_days = len(date_wk_rec)
            if wk_days < 7:
                if enroll_date > date_week_start:
                    num_days_exp_missing = (enroll_date - date_week_start).days
                    num_days_exp = 7 - num_days_exp_missing
                    missing_days_wk = str(num_days_exp - wk_days)
                    dates_expected = [date for date in week_dates if date > dt.date(enroll_date)]
                    dates_missing = [date for date in dates_expected if date not in date_wk_rec]
                else:
                    missing_days_wk = str(7 - wk_days)
                    dates_missing = [date for date in week_dates if date not in date_wk_rec]
            else:
                dates_missing = []
                missing_days_wk = str(0)
            print(missing_days_wk + ' missing days of data for ' + measure + ' this past week')

            # Compile debug log for dates missing
            debug_list = []
            for i in range(len(data_inventory_dict['device_debug'])):
                try:
                    debug_ts = dt.strptime(data_inventory_dict['device_debug'][i]['timestamp'].split(' ')[0], "%m/%d/%y")
                    if dt.date(debug_ts) in dates_missing:
                        debug_list.append(data_inventory_dict['device_debug'][i])
                except:
                    pass

            data_inventory_dict["%s%s" % ('missing_days_past_week_', measure)] = dates_missing
            data_inventory_dict["%s%s" % ('num_missing_days_past_week_', measure)] = missing_days_wk
            data_inventory_dict["%s%s" % ('debug_missing_days_', measure)] = debug_list
            missing_dates_keys = [i for i in list(data_inventory_dict.keys()) if i.startswith('missing')]
            num_missing_dates_keys = [i for i in list(data_inventory_dict.keys()) if 'num_missing' in i]
            debug_missing_dates_keys = [i for i in list(data_inventory_dict.keys()) if 'debug_missing' in i]

        # Cleanup dictionary
        try:
            del data_inventory_dict['device_id_exists']
        except:
            pass
        try:
            del data_inventory_dict['location_data']
        except:
            pass
        try:
            del data_inventory_dict['ambient_noise']
        except:
            pass
        try:
            del data_inventory_dict['screen_useage']
        except:
            pass

        missing_wk_days = []
        for i in range(len(missing_dates_keys)):
            missing_wk_days.append(data_inventory_dict[num_missing_dates_keys[i]])

        if len(missing_items) == 0 and len(set(missing_wk_days)) == 0:
            message_text_sub = 'ALL MEASURES ACQUIRED! PASS.'
        else:
            miss_measures = list(set(missing_measures))
            if len(miss_measures) == 0:
                miss_measures = 'Collecting All Measures!'
            num_missing_items = len(missing_items)
            if len(missing_items) == 0:
                missing_items = 'Collecting All Items!'
            message_text_sub = 'PARTICIPANT: ' + str(id_dict[device_id_check]) + '\n' + 'DEVICE ID: ' + \
                               str(device_id_check) + '\nDEVICE DATA: ' + str(data_inventory_dict['device_data']) + '\n' + \
                                str(len(miss_measures)) + ' MISSING PLUGINS/SENSORS: ' \
                           + str(miss_measures) + '\n' + str(len(missing_measures)) + ' MISSING METRICS: ' + str(missing_items) + '\n'

            for i in range(len(missing_dates_keys)):
                measure = num_missing_dates_keys[i].split('week_')[-1]
                missing_days_wk = data_inventory_dict[num_missing_dates_keys[i]]
                dates_missing = [date.strftime("%m/%d/%y") for date in data_inventory_dict[missing_dates_keys[i]]]
                debug_list = data_inventory_dict[debug_missing_dates_keys[i]]
                debug_list_short = []
                event_list = []
                for i in range(len(debug_list)):
                    debug_list_reduced = debug_list[i]
                    try:
                        del debug_list_reduced['type']
                    except:
                        pass
                    try:
                        del debug_list_reduced['device']
                    except:
                        pass
                    try:
                        del debug_list_reduced['os']
                    except:
                        pass
                    try:
                        del debug_list_reduced['app_version']
                    except:
                        pass
                    try:
                        del debug_list_reduced['battery_state']
                    except:
                        pass
                    debug_list_short.append(debug_list_reduced)
                    event_list.append(debug_list_reduced['event'])
                unique_events = list(set(event_list))

                if len(debug_list_short) == 0:
                    debug_empty = True
                    debug_list_short = 'Debug mode not activated for this participant!'
                    unique_events = 'Not available. Please contact participant ASAP to activate debug mode!'
                else:
                    debug_empty = False
                if len(dates_missing) > 0:
                    missingness_sec = missing_days_wk + ' MISSING DATES FOR ' + measure + ': ' + str(dates_missing)
                    missingness_sec = missingness_sec + '\nDEBUG LOG: ' + str(debug_list_short)
                else:
                    missingness_sec = ''
                if debug_empty is False:
                    missingness_sec = missingness_sec + '\nTROUBLESHOOT TARGET: ' + str(unique_events)
                message_text_sub = message_text_sub + missingness_sec

                # print(message_text_sub)

        message_text = message_text + '\n\n' + message_text_sub

Subject = "%s%s" % ('AWARE WEEKLY DATA COMPLETION CHECK ', now.strftime('%m/%d/%y'))
msg = MIMEMultipart('alternative')
#message = 'Subject: {}\n\n{}'.format(Subject, message_text)
msg['Subject'] = Subject
msg['From'] = 'r33aware@gmail.com'
msg['To'] = 'dpisner@utexas.edu'
html = """\
<html>
  <head></head>
  <body>
    $code
  </body>
</html>
"""
message_text_for_html = message_text.replace("\n", "<br />\n")
message_text_for_html = message_text_for_html.replace('PARTICIPANT:', '<h2 style="color: blue;">PARTICIPANT:</h2>')
message_text_for_html = message_text_for_html.replace('DEVICE ID:', '<h3 style="font-weight:bold">DEVICE ID:</h3>')
message_text_for_html = message_text_for_html.replace('DEVICE DATA:', '<h3 style="font-weight:bold">DEVICE DATA:</h3>')
if 'DEBUG LOG:' in message_text_for_html:
    message_text_for_html = message_text_for_html.replace('DEBUG LOG:', '<h3 style="color: orange;">DEBUG LOG:</h3>')
if 'TROUBLESHOOT TARGET:' in message_text_for_html:
    message_text_for_html = message_text_for_html.replace('TROUBLESHOOT TARGET:', '<h3 style="color: orange";>TROUBLESHOOT TARGET:</h3>')
if '0 MISSING PLUGINS/SENSORS' in message_text_for_html:
    message_text_for_html = message_text_for_html.replace('0 MISSING PLUGINS/SENSORS:', '<h3 style="color: green;">0 MISSING PLUGINS/SENSORS:</h3>')
elif 'MISSING PLUGINS/SENSORS' in message_text_for_html:
    nums = re.findall(r"(\d+) MISSING PLUGINS/SENSORS:", message_text_for_html)
    for num in nums:
        message_text_for_html = message_text_for_html.replace("%s%s" % (num, ' MISSING PLUGINS/SENSORS:'), '<h3 style="color: red;">' + "%s%s" % (num, ' MISSING PLUGINS/SENSORS:') + '</h3>')
if '0 MISSING METRICS' in message_text_for_html:
    message_text_for_html = message_text_for_html.replace('0 MISSING METRICS:', '<h3 style="color: green;">0 MISSING METRICS:</h3>')
elif 'MISSING METRICS' in message_text_for_html:
    nums = re.findall(r"(\d+) MISSING METRICS:", message_text_for_html)
    for num in nums:
        message_text_for_html = message_text_for_html.replace("%s%s" % (num, ' MISSING METRICS:'), '<h3 style="color: red;">' + "%s%s" % (num, ' MISSING METRICS:') + '</h3>')
if '0 MISSING DATES' in message_text_for_html:
    message_text_for_html = message_text_for_html.replace('0 MISSING DATES FOR', '<h3 style="color: green;">0 MISSING DATES FOR</h3>')
elif 'MISSING DATES' in message_text_for_html:
    nums = re.findall(r"(\d+) MISSING DATES FOR", message_text_for_html)
    for num in nums:
        message_text_for_html = message_text_for_html.replace("%s%s" % (num, ' MISSING DATES FOR'), '<h3 style="color: red;">' + "%s%s" % (num, ' MISSING DATES FOR') + '</h3>')

s = Template(html).safe_substitute(code=message_text_for_html)
part1 = MIMEText(message_text, 'plain')
part2 = MIMEText(s, 'html')
msg.attach(part1)
msg.attach(part2)
server = smtplib.SMTP('smtp.gmail.com', 587)  # port 465 or 587
server.ehlo()
server.starttls()
server.ehlo()
server.login('r33aware@gmail.com', 'beevers01')
#server.sendmail(to_addrs=['dpisner@utexas.edu'], from_addr='r33aware@gmail.com', msg=msg.as_string())
server.sendmail(to_addrs=['dpisner@utexas.edu','semeon.risom@gmail.com', 'keanjhsu@utexas.edu', 'kdcaffey@utexas.edu', 'jocelyn.labrada@utexas.edu', 'shumake@utexas.edu', 'beevers@utexas.edu'], from_addr='r33aware@gmail.com', msg=msg.as_string())
server.close()
