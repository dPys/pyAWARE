from __future__ import division
import time
import ast
import pymysql
import requests
import multiprocessing
import numpy as np
import pandas as pd
import itertools
from matplotlib import pyplot as plt
from datetime import datetime, timedelta
from scipy import stats
import warnings
warnings.simplefilter("ignore")

# Android fetches
def fetch_ids_droid():
    cur = db1.cursor()
    cur.execute("SELECT device_id FROM aware_device;")
    all_items = cur.fetchall()
    device_ids = [item[0] for item in all_items]
    cur = db1.cursor()
    cur.execute("SELECT label FROM aware_device;")
    all_items = cur.fetchall()
    labels = [item[0] for item in all_items]
    return device_ids, labels


def fetch_ids_ios():
    cur = db2.cursor()
    cur.execute("SELECT device_id FROM aware_device;")
    all_items = cur.fetchall()
    device_ids = [item[0] for item in all_items]
    cur = db2.cursor()
    cur.execute("SELECT label FROM aware_device;")
    all_items = cur.fetchall()
    labels = [item[0] for item in all_items]
    return device_ids, labels

# Load data
[device_ids_droid, labels_droid] = fetch_ids_droid()
id_dict1 = dict(zip(device_ids_droid, labels_droid))
[device_ids_ios, labels_ios] = fetch_ids_ios()
id_dict2 = dict(zip(device_ids_ios, labels_ios))


def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


def summarise_calls(df):
    # Calls
    total_calls = len(df['call_types'].dropna())
    if total_calls != 0:
        try:
            missed_calls = len(df.loc[df['call_types'] == 'Missed'])
        except:
            missed_calls = np.nan
        try:
            incoming_calls = len(df.loc[df['call_types'] == 'Incoming'])
        except:
            incoming_calls = np.nan
        try:
            outgoing_calls = len(df.loc[df['call_types'] == 'Outgoing'])
        except:
            outgoing_calls = np.nan
        try:
            perc_outgoing_calls = float(outgoing_calls) / float(total_calls)
        except:
            perc_outgoing_calls = np.nan
        try:
            perc_incoming_calls = float(incoming_calls) / float(total_calls)
        except:
            perc_incoming_calls = np.nan
        try:
            perc_missed_calls = float(missed_calls) / float(total_calls)
        except:
            perc_missed_calls = np.nan
        try:
            avg_outgoing_call_length = df.loc[df['call_types'] == 'Outgoing']['call_durs'].mean()
        except:
            avg_outgoing_call_length = np.nan
        try:
            total_outgoing_call_length = df.loc[df['call_types'] == 'Outgoing']['call_durs'].sum()
        except:
            total_outgoing_call_length = np.nan
        try:
            df_call_returns = df.loc[((df['call_types'] == 'Missed') | (df['call_types'] == 'Outgoing'))]
            df_call_returns = df_call_returns.loc[df_call_returns.call_types + df_call_returns.call_types.shift() != 'OutgoingOutgoing']
            df_call_returns = df_call_returns.loc[df_call_returns.call_types != df_call_returns.call_types.shift()]
            try:
                if df_call_returns.call_types[-1] == 'Missed':
                    df_call_returns.drop(df_call_returns.tail(1).index,inplace=True)
            except:
                pass
            df_call_returns_series = pd.to_datetime(df_call_returns.index).to_series().diff().dt.total_seconds().dropna()
            avg_call_return_time = float(np.nanmean(df_call_returns_series))/60
            perc_calls_returned_from_missed = len(df_call_returns.loc[df_call_returns['call_types'] == 'Missed']) / missed_calls
        except:
            avg_call_return_time = np.nan
            perc_calls_returned_from_missed = np.nan

        try:
            total_callers = len(df['trace'].dropna().tolist())
            unique_callers = len(set(df['trace'].dropna().tolist()))
            perc_unique_callers = float(unique_callers)/float(total_callers)
        except:
            perc_unique_callers = np.nan

        if total_calls == 0 and missed_calls == 0 and incoming_calls == 0 and outgoing_calls == 0:
            total_calls = np.nan
            missed_calls = np.nan
            incoming_calls = np.nan
            outgoing_calls = np.nan

        if avg_outgoing_call_length == 0:
            avg_outgoing_call_length = np.nan

        if avg_call_return_time == 0:
            avg_call_return_time = np.nan
    else:
        total_calls = np.nan
        missed_calls = np.nan
        incoming_calls = np.nan
        outgoing_calls = np.nan
        perc_outgoing_calls = np.nan
        perc_incoming_calls = np.nan
        perc_missed_calls = np.nan
        avg_outgoing_call_length = np.nan
        total_outgoing_call_length = np.nan
        perc_calls_returned_from_missed = np.nan
        perc_unique_callers = np.nan
        avg_call_return_time = np.nan

    return total_calls, missed_calls, incoming_calls, outgoing_calls, perc_outgoing_calls, \
           perc_incoming_calls, perc_missed_calls, avg_outgoing_call_length, total_outgoing_call_length, \
           avg_call_return_time, perc_calls_returned_from_missed, perc_unique_callers


def summarise_text_messages(df):
    # Texts
    total_texts = len(df['message_type'].dropna())
    if total_texts != 0:
        try:
            incoming_texts = len(df.loc[df['message_type'] == 'Incoming'])
        except:
            incoming_texts = np.nan
        try:
            outgoing_texts = len(df.loc[df['message_type'] == 'Outgoing'])
        except:
            outgoing_texts = np.nan
        try:
            perc_outgoing_texts = float(outgoing_texts) / float(total_texts)
            perc_incoming_texts = float(incoming_texts) / float(total_texts)
        except:
            perc_outgoing_texts = np.nan
            perc_incoming_texts = np.nan
        try:
            df_text_returns = df.loc[((df['message_type'] == 'Incoming') | (df['message_type'] == 'Outgoing'))]
            df_text_returns = df_text_returns.loc[df_text_returns.message_type + df_text_returns.message_type.shift() != 'OutgoingOutgoing']
            df_text_returns = df_text_returns.loc[df_text_returns.message_type != df_text_returns.message_type.shift()]
            try:
                if df_text_returns.message_type[-1] == 'Incoming':
                    df_text_returns.drop(df_text_returns.tail(1).index,inplace=True)
            except:
                pass
            df_text_returns_series = pd.to_datetime(df_text_returns.index).to_series().diff().dt.total_seconds().dropna()
            avg_text_return_time = float(np.nanmean(df_text_returns_series))/60
            perc_texts_returned_from_incoming = len(df_text_returns.loc[df_text_returns['message_type'] == 'Incoming']) / incoming_texts
        except:
            avg_text_return_time = np.nan
            perc_texts_returned_from_incoming = np.nan

        try:
            num_unique_texters = len(set(df['text_trace'].dropna().tolist())) - 1
            if num_unique_texters <= 0:
                num_unique_texters = np.nan
        except:
            num_unique_texters = np.nan

        if avg_text_return_time == 0:
            avg_text_return_time = np.nan
    else:
        total_texts = np.nan
        incoming_texts = np.nan
        outgoing_texts = np.nan
        perc_outgoing_texts = np.nan
        perc_incoming_texts = np.nan
        avg_text_return_time = np.nan
        perc_texts_returned_from_incoming = np.nan
        num_unique_texters = np.nan

    return total_texts, incoming_texts, outgoing_texts, perc_outgoing_texts, perc_incoming_texts, avg_text_return_time, perc_texts_returned_from_incoming, num_unique_texters


def summarise_screen_time(df):
    # Screen time
    screen_status = df['screen_status'].dropna()
    if len(screen_status) > 100:
        time_unlocked = len(screen_status.loc[screen_status == 3])
        time_off = len(screen_status.loc[screen_status == 0])
        total_screen_samples = len(screen_status)
        perc_total_time_unlocked = float(len(screen_status.loc[screen_status == 3]) / total_screen_samples)
        perc_total_time_off = float(len(screen_status.loc[screen_status == 0]) / total_screen_samples)
    else:
        time_unlocked = np.nan
        time_off = np.nan
        total_screen_samples = np.nan
        perc_total_time_unlocked = np.nan
        perc_total_time_off = np.nan
    return time_unlocked, time_off, total_screen_samples, perc_total_time_unlocked, perc_total_time_off


def summarise_noise(df):
    try:
        noise_frequency = float(np.nanmean(df['noise_frequency']))
        noise_decibels = float(np.nanmean(df['noise_decibels']))
    except:
        noise_frequency = np.nan
        noise_decibels = np.nan
    return noise_frequency, noise_decibels


def summarise_weather(df):
    df.index = pd.to_datetime(df.index)
    try:
        df['temperature'] = df['temperature'].apply(
            lambda x: x + 273 if float(x) < 0 else (((float(x) - 32) * 5.0 / 9.0) if float(x) > 48 else x))
    except:
        pass
    try:
        avg_temp = np.nanmean(df['temperature'].values)
    except:
        avg_temp = np.nan
    try:
        avg_humidity = np.nanmean(df['humidity'].resample('D', label='right').mean().dropna().values)
    except:
        avg_humidity = np.nan
    try:
        avg_windiness = np.nanmean(df['wind_speed'].resample('D', label='right').mean().dropna().values)
    except:
        avg_windiness = np.nan
    try:
        avg_cloudiness = np.nanmean(df['cloudiness'].resample('D', label='right').mean().dropna().values)
    except:
        avg_cloudiness = np.nan
    try:
        total_rainfall = np.nansum(df['rain'].resample('D', label='right').mean().dropna().values)
    except:
        total_rainfall = np.nan
    try:
        avg_pressure = np.nanmean(df['pressure'].resample('D', label='right').mean().dropna().values)
    except:
        avg_pressure = np.nan

    return avg_temp, avg_humidity, avg_windiness, avg_cloudiness, total_rainfall, avg_pressure


def summarise_activities(df):
    try:
        df.loc[df['activities'].str.len() == 2, 'activities'] = np.nan
    except:
        pass
    df['activities'] = df['activities'].astype('float64')
    total_activity_samples = len(df['activities'].dropna())
    try:
        total_activities = len(df.loc[np.round(df['activities']) != 1.0]['activities'].dropna())
    except:
        total_activities = np.nan
    if total_activity_samples != 0:
        try:
            total_automotive = len(df.loc[np.round(df['activities']) == 5.0]['activities'])
        except:
            total_automotive = np.nan
        try:
            perc_automotive = total_automotive / total_activity_samples
        except:
            perc_automotive = np.nan
        try:
            total_running = len(df.loc[np.round(df['activities']) == 3.0]['activities'])
        except:
            total_running = np.nan
        try:
            perc_running = total_running / total_activity_samples
        except:
            perc_running = np.nan
        try:
            total_walking = len(df.loc[np.round(df['activities']) == 2.0]['activities'])
        except:
            total_walking = np.nan
        try:
            perc_walking = total_walking / total_activity_samples
        except:
            perc_walking = np.nan
        try:
            total_stationary = len(df.loc[np.round(df['activities']) == 1.0]['activities'])
        except:
            total_stationary = np.nan
        try:
            perc_stationary = total_stationary / total_activity_samples
        except:
            perc_stationary = np.nan
    else:
        total_activities = np.nan
        total_automotive = np.nan
        perc_automotive = np.nan
        total_running = np.nan
        perc_running = np.nan
        total_walking = np.nan
        perc_walking = np.nan
        total_stationary = np.nan
        perc_stationary = np.nan
    return total_activities, total_automotive, perc_automotive, total_running, perc_running, total_walking, perc_walking, total_stationary, perc_stationary


id_dict = merge_two_dicts(id_dict1, id_dict2)

device_ids = device_ids_droid + device_ids_ios

metric_list_names = ['total_calls', 'missed_calls', 'incoming_calls', 'outgoing_calls',
                     'perc_outgoing_calls', 'perc_incoming_calls', 'perc_missed_calls', 'avg_outgoing_call_length',
                     'total_outgoing_call_length', 'avg_call_return_time',
                     'perc_calls_returned_from_missed', 'perc_unique_callers', 'noise_frequency', 'noise_decibels',
                     'time_unlocked', 'time_off', 'total_screen_samples', 'perc_total_time_unlocked', 'perc_total_time_off',
                     'avg_temp', 'avg_humidity', 'avg_windiness',
                     'avg_cloudiness', 'total_rainfall', 'avg_pressure', 'total_activities', 'total_automotive',
                     'perc_automotive', 'total_running', 'perc_running',
                     'total_walking', 'perc_walking', 'total_stationary', 'perc_stationary', 'total_texts',
                     'incoming_texts', 'outgoing_texts', 'perc_outgoing_texts', 'perc_incoming_texts',
                     'avg_text_return_time', 'perc_texts_returned_from_incoming', 'num_unique_texters']

df_summary = pd.DataFrame(columns=metric_list_names)
good_subs = list(set(list(filter(None, [x for x in list(id_dict.values()) if 'R33' in x]))))
df_summary['subject_id'] = good_subs
df_summary['subject_id'] = df_summary['subject_id'].astype('object')


def getKeysByValue(dictOfElements, valueToFind):
    listOfKeys = list()
    listOfItems = dictOfElements.items()
    for item in listOfItems:
        if item[1] == valueToFind:
            listOfKeys.append(item[0])
    return listOfKeys


device_ids_unique = list(set(device_ids))
device_ids_unique_filtered = []
for x in good_subs:
    device_ids_unique_filtered.append(getKeysByValue(id_dict, x)[0])

# Loop across subjects
for device_id in device_ids_unique_filtered:
    subject_id = id_dict[device_id]
    print(subject_id)
    try:
        df = pd.read_csv("/Users/PSYC-dap3463/Box Sync/MDL Projects/Derek/Mobile_data/AWARE_analysis/all_preprocessed" + str(subject_id) + '.csv')
    except:
        print('No data file found. Continuing...')
        continue

    df = df.set_index(pd.to_datetime(df['timestamps']))
    df = df.drop(columns='subject_id')
    # Filter ts to include near-average level of samples
    df_days = df.groupby(df.index.date).count()
    mean_num_t_stamps = df_days['timestamps'].mean()
    min_num_t_stamps = mean_num_t_stamps - 3*(df_days['timestamps'].std())
    max_num_t_stamps = mean_num_t_stamps + 3*(df_days['timestamps'].std())
    df_good_days = df_days.loc[(df_days['timestamps'] > min_num_t_stamps) & (df_days['timestamps'] < max_num_t_stamps)].index.values.tolist()
    df_bad_days = df_days.loc[(df_days['timestamps'] < min_num_t_stamps) | (df_days['timestamps'] > max_num_t_stamps)].index.values.tolist()
    df = df[~df['timestamps'].isin(df_bad_days)]
    df = df[~df.index.isnull()]
    if len(df) > 0:
        start_date = sorted(list(set(df_good_days)))[0]
        stop_date = start_date + timedelta(days=35)
        df_study = df.truncate(before=start_date, after=stop_date, axis=0)
        df_weeks = df_study.set_index(df_study.to_period(freq='w').index)
        weeks = sorted(list(set(list((df_weeks.index)))))
        if len(weeks) > 4:
            weeks = weeks[0:4]
    else:
        print('Empty dataframe. Continuing...')
        continue

    try:
        [total_calls, missed_calls, incoming_calls, outgoing_calls, perc_outgoing_calls,
        perc_incoming_calls, perc_missed_calls, avg_outgoing_call_length, total_outgoing_call_length,
        avg_call_return_time, perc_calls_returned_from_missed, perc_unique_callers] = summarise_calls(df)

        try:
            # Week 1
            df_week = df_weeks[(df_weeks.index == weeks[0])]
            df_week = df_week.set_index('timestamps')
            [total_calls_wk1, missed_calls_wk1, incoming_calls_wk1, outgoing_calls_wk1, perc_outgoing_calls_wk1,
            perc_incoming_calls_wk1, perc_missed_calls_wk1, avg_outgoing_call_length_wk1, total_outgoing_call_length_wk1,
            avg_call_return_time_wk1, perc_calls_returned_from_missed_wk1, perc_unique_callers_wk1] = summarise_calls(df_week)
        except:
            print('Calls: Missing week 1')

        try:
            # Week 2
            df_week = df_weeks[(df_weeks.index == weeks[1])]
            df_week = df_week.set_index('timestamps')
            [total_calls_wk2, missed_calls_wk2, incoming_calls_wk2, outgoing_calls_wk2,
             perc_outgoing_calls_wk2, perc_incoming_calls_wk2, perc_missed_calls_wk2, avg_outgoing_call_length_wk2,
             total_outgoing_call_length_wk2, avg_call_return_time_wk2,
             perc_calls_returned_from_missed_wk2, perc_unique_callers_wk2] = summarise_calls(df_week)
        except:
            print('Calls: Missing week 2')

        try:
            # Week 3
            df_week = df_weeks[(df_weeks.index == weeks[2])]
            df_week = df_week.set_index('timestamps')
            [total_calls_wk3, missed_calls_wk3, incoming_calls_wk3, outgoing_calls_wk3,
             perc_outgoing_calls_wk3, perc_incoming_calls_wk3, perc_missed_calls_wk3, avg_outgoing_call_length_wk3,
             total_outgoing_call_length_wk3, avg_call_return_time_wk3,
             perc_calls_returned_from_missed_wk3, perc_unique_callers_wk3] = summarise_calls(df_week)
        except:
            print('Calls: Missing week 3')

        try:
            # Week 4
            df_week = df_weeks[(df_weeks.index == weeks[3])]
            df_week = df_week.set_index('timestamps')
            [total_calls_wk4, missed_calls_wk4, incoming_calls_wk4, outgoing_calls_wk4,
             perc_outgoing_calls_wk4, perc_incoming_calls_wk4, perc_missed_calls_wk4, avg_outgoing_call_length_wk4,
             total_outgoing_call_length_wk4, avg_call_return_time_wk4,
             perc_calls_returned_from_missed_wk4, perc_unique_callers_wk4] = summarise_calls(df_week)
        except:
            print('Calls: Missing week 4')

    except:
        print('Calls: Missing calls!')
        pass

    try:
        [time_unlocked, time_off, total_screen_samples, perc_total_time_unlocked, perc_total_time_off] = summarise_screen_time(df)

        try:
            df_week = df_weeks[(df_weeks.index == weeks[0])]
            df_week = df_week.set_index('timestamps')
            [time_unlocked_wk1, time_off_wk1, total_screen_samples_wk1, perc_total_time_unlocked_wk1, perc_total_time_off_wk1] = summarise_screen_time(df_week)
        except:
            print('Screen: Missing week 1')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[1])]
            df_week = df_week.set_index('timestamps')
            [time_unlocked_wk2, time_off_wk2, total_screen_samples_wk2, perc_total_time_unlocked_wk2, perc_total_time_off_wk2] = summarise_screen_time(df_week)
        except:
            print('Screen: Missing week 2')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[2])]
            df_week = df_week.set_index('timestamps')
            [time_unlocked_wk3, time_off_wk3, total_screen_samples_wk3, perc_total_time_unlocked_wk3, perc_total_time_off_wk3] = summarise_screen_time(df_week)
        except:
            print('Screen: Missing week 3')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[3])]
            df_week = df_week.set_index('timestamps')
            [time_unlocked_wk4, time_off_wk4, total_screen_samples_wk4, perc_total_time_unlocked_wk4, perc_total_time_off_wk4] = summarise_screen_time(df_week)
        except:
            print('Screen: Missing week 4')

    except:
        print('Missing screen time!')
        pass

    try:
        [noise_frequency, noise_decibels] = summarise_noise(df)

        try:
            df_week = df_weeks[(df_weeks.index == weeks[0])]
            df_week = df_week.set_index('timestamps')
            [noise_frequency_wk1, noise_decibels_wk1] = summarise_noise(df_week)
        except:
            print('Ambient Noise: Missing week 1')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[1])]
            df_week = df_week.set_index('timestamps')
            [noise_frequency_wk2, noise_decibels_wk2] = summarise_noise(df_week)
        except:
            print('Ambient Noise: Missing week 2')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[2])]
            df_week = df_week.set_index('timestamps')
            [noise_frequency_wk3, noise_decibels_wk3] = summarise_noise(df_week)
        except:
            print('Ambient Noise: Missing week 3')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[3])]
            df_week = df_week.set_index('timestamps')
            [noise_frequency_wk4, noise_decibels_wk4] = summarise_noise(df_week)
        except:
            print('Ambient Noise: Missing week 4')

    except:
        print('Missing ambient noise data!')
        pass

    try:
        [total_texts, incoming_texts, outgoing_texts, perc_outgoing_texts, perc_incoming_texts, avg_text_return_time, perc_texts_returned_from_incoming, num_unique_texters] = summarise_text_messages(df)

        try:
            df_week = df_weeks[(df_weeks.index == weeks[0])]
            df_week = df_week.set_index('timestamps')
            [total_texts_wk1, incoming_texts_wk1, outgoing_texts_wk1, perc_outgoing_texts_wk1, perc_incoming_texts_wk1, avg_text_return_time_wk1, perc_texts_returned_from_incoming_wk1, num_unique_texters_wk1] = summarise_text_messages(df_week)
        except:
            print('Text Messages: Missing week 1')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[1])]
            df_week = df_week.set_index('timestamps')
            [total_texts_wk2, incoming_texts_wk2, outgoing_texts_wk2, perc_outgoing_texts_wk2, perc_incoming_texts_wk2, avg_text_return_time_wk2, perc_texts_returned_from_incoming_wk2, num_unique_texters_wk2] = summarise_text_messages(df_week)
        except:
            print('Text Messages: Missing week 2')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[2])]
            df_week = df_week.set_index('timestamps')
            [total_texts_wk3, incoming_texts_wk3, outgoing_texts_wk3, perc_outgoing_texts_wk3, perc_incoming_texts_wk3, avg_text_return_time_wk3, perc_texts_returned_from_incoming_wk3, num_unique_texters_wk3] = summarise_text_messages(df_week)
        except:
            print('Text Messages: Missing week 3')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[3])]
            df_week = df_week.set_index('timestamps')
            [total_texts_wk4, incoming_texts_wk4, outgoing_texts_wk4, perc_outgoing_texts_wk4, perc_incoming_texts_wk4, avg_text_return_time_wk4, perc_texts_returned_from_incoming_wk4, num_unique_texters_wk4] = summarise_text_messages(df_week)
        except:
            print('Text Messages: Missing week 4')

    except:
        print('Missing text message data!')
        pass

    try:
        # Weather
        [avg_temp, avg_humidity, avg_windiness, avg_cloudiness, total_rainfall, avg_pressure] = summarise_weather(df)

        try:
            df_week = df_weeks[(df_weeks.index == weeks[0])]
            df_week = df_week.set_index('timestamps')
            [avg_temp_wk1, avg_humidity_wk1, avg_windiness_wk1, avg_cloudiness_wk1, total_rainfall_wk1, avg_pressure_wk1] = summarise_weather(df_week)
        except:
            print('Weather: Missing week 1')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[1])]
            df_week = df_week.set_index('timestamps')
            [avg_temp_wk2, avg_humidity_wk2, avg_windiness_wk2, avg_cloudiness_wk2, total_rainfall_wk2, avg_pressure_wk2] = summarise_weather(df_week)
        except:
            print('Weather: Missing week 2')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[2])]
            df_week = df_week.set_index('timestamps')
            [avg_temp_wk3, avg_humidity_wk3, avg_windiness_wk3, avg_cloudiness_wk3, total_rainfall_wk3, avg_pressure_wk3] = summarise_weather(df_week)
        except:
            print('Weather: Missing week 3')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[3])]
            df_week = df_week.set_index('timestamps')
            [avg_temp_wk4, avg_humidity_wk4, avg_windiness_wk4, avg_cloudiness_wk4, total_rainfall_wk4, avg_pressure_wk4] = summarise_weather(df_week)
        except:
            print('Weather: Missing week 4')

    except:
        print('Missing weather!')
        pass

    try:
        # Activities
        [total_activities, total_automotive, perc_automotive, total_running, perc_running, total_walking, perc_walking, total_stationary, perc_stationary] = summarise_activities(df)

        try:
            df_week = df_weeks[(df_weeks.index == weeks[0])]
            df_week = df_week.set_index('timestamps')
            [total_activities_wk1, total_automotive_wk1, perc_automotive_wk1, total_running_wk1,
             perc_running_wk1, total_walking_wk1, perc_walking_wk1, total_stationary_wk1, perc_stationary_wk1] = summarise_activities(df_week)
        except:
            print('Activities: Missing week 1')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[1])]
            df_week = df_week.set_index('timestamps')
            [total_activities_wk2, total_automotive_wk2, perc_automotive_wk2, total_running_wk2,
             perc_running_wk2, total_walking_wk2, perc_walking_wk2, total_stationary_wk2, perc_stationary_wk2] = summarise_activities(df_week)
        except:
            print('Activities: Missing week 2')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[2])]
            df_week = df_week.set_index('timestamps')
            [total_activities_wk3, total_automotive_wk3, perc_automotive_wk3, total_running_wk3,
             perc_running_wk3, total_walking_wk3, perc_walking_wk3, total_stationary_wk3, perc_stationary_wk3] = summarise_activities(df_week)
        except:
            print('Activities: Missing week 3')

        try:
            df_week = df_weeks[(df_weeks.index == weeks[3])]
            df_week = df_week.set_index('timestamps')
            [total_activities_wk4, total_automotive_wk4, perc_automotive_wk4, total_running_wk4,
             perc_running_wk4, total_walking_wk4, perc_walking_wk4, total_stationary_wk4, perc_stationary_wk4] = summarise_activities(df_week)
        except:
            print('Activities: Missing week 4')
    except:
        print('Missing activities!')
        pass

    for metric in metric_list_names:
        try:
            eval(metric)
        except:
            globals()[metric] = np.nan
        try:
            eval("%s%s" % (metric, '_wk1'))
        except:
            globals()["%s%s" % (metric, '_wk1')] = np.nan
        try:
            eval("%s%s" % (metric, '_wk2'))
        except:
            globals()["%s%s" % (metric, '_wk2')] = np.nan
        try:
            eval("%s%s" % (metric, '_wk3'))
        except:
            globals()["%s%s" % (metric, '_wk3')] = np.nan
        try:
            eval("%s%s" % (metric, '_wk4'))
        except:
            globals()["%s%s" % (metric, '_wk4')] = np.nan
        try:
            df_summary.loc[df_summary['subject_id'] == subject_id, metric] = eval(metric)
        except:
            df_summary.loc[df_summary['subject_id'] == subject_id, metric] = np.nan
        try:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk1')] = eval("%s%s" % (metric, '_wk1'))
        except:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk1')] = np.nan
        try:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk2')] = eval("%s%s" % (metric, '_wk2'))
        except:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk2')] = np.nan
        try:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk3')] = eval("%s%s" % (metric, '_wk3'))
        except:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk3')] = np.nan
        try:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk4')] = eval("%s%s" % (metric, '_wk4'))
        except:
            df_summary.loc[df_summary['subject_id'] == subject_id, "%s%s" % (metric, '_wk4')] = np.nan

        del globals()[metric]
        del globals()["%s%s" % (metric, '_wk1')]
        del globals()["%s%s" % (metric, '_wk2')]
        del globals()["%s%s" % (metric, '_wk3')]
        del globals()["%s%s" % (metric, '_wk4')]

    print('\n')

cols = list(df_summary)
cols.insert(0, cols.pop(cols.index('subject_id')))
df_summary = df_summary.ix[:, cols]
df_summary.to_csv('/Users/PSYC-dap3463/Box Sync/MDL Projects/Derek/Mobile_data/AWARE_analysis/all_mets_summary.csv', index=False)
