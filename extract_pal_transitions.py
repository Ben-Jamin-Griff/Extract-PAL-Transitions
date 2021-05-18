import os
import pandas as pd
import numpy as np
import time
import tkinter as tk
from tkinter import filedialog
import datetime
from uos_activpal.io.raw import load_activpal_data
import warnings
warnings.filterwarnings("ignore")


# FUNCTIONS
def print_progress_bar(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print(prefix + " complete")

def collect_events(EventTimes,  dataFrameObject, data, meta, case):
    for i, eventTime in enumerate(EventTimes):
        print_progress_bar(i, len(EventTimes)-1, 'Processing ' + case + ' transitions:')
        df_sort = data.iloc[(data.Time - eventTime).abs().argsort()[:1]]
        x = df_sort.index.tolist()
        transitionIDs = np.full(shape=120,fill_value=i,dtype=int)
        recordingID = [meta.file_code[1:]]*120
        serialNo = ["AP" + str(meta.device_id)]*120
        eventDf = data.iloc[x[0]-60:x[0]+60]
        eventDf = eventDf.assign(SerialNumber = serialNo)
        eventDf = eventDf.assign(RecordingID = recordingID)
        eventDf = eventDf.assign(TransitionId = transitionIDs)
        eventDf = eventDf[['TransitionId', 'RecordingID', 'SerialNumber','Time','Index','X', 'Y', 'Z']]
        eventDf.columns = ['Transition Id', 'Recording Id', 'Serial Number', 'Time', 'Sample Number', 'X Axis', 'Y Axis', 'Z Axis']
        dataFrameObject = dataFrameObject.append(eventDf, ignore_index = True)
    return dataFrameObject

def count_transitions(case, dfEvents):
    Transitions = 0
    EventTimes = []
    idx = 0
    while idx < len(dfEvents.index)-1:
        if case == 'StSi':
            if dfEvents.iloc[idx, 3] != 0 and dfEvents.iloc[idx+1, 3] == 0:
                eventTime = dfEvents.iloc[idx+1, 0]
                EventTimes.append(eventTime)
                Transitions += 1
            idx += 1
        elif case == 'SiSt':
            if dfEvents.iloc[idx, 3] == 0 and dfEvents.iloc[idx+1, 3] != 0:
                eventTime = dfEvents.iloc[idx+1, 0]
                EventTimes.append(eventTime)
                Transitions += 1
            idx += 1
    return Transitions, EventTimes

def load_event_data(filename = None):
        if filename is not None:
            fEvent = filename
        else:
            root = tk.Tk()
            root.withdraw()
            fEvent = filedialog.askopenfilename(title = "Load thigh event data")
            return fEvent

def load_raw_data(filename = None):
        if filename is not None:
            fRaw = filename
        else:
            root = tk.Tk()
            root.withdraw()
            fRaw = filedialog.askopenfilename(title = "Load shank acceleration data")
            return fRaw

# PROCESSING
#fRaw = load_raw_data()
#fEvent = load_event_data()
start = time.time()
# Collect all relevant filenames in the current directory
eventFilesToConvert = []
allFiles = [f for f in os.listdir('.') if os.path.isfile(f)]
for f in allFiles:
    try:
        if f[-10:] == "Events.csv":
            eventFilesToConvert.append(f)
    except:
        pass

rawFilesToConvert = []
allFiles = [f for f in os.listdir('.') if os.path.isfile(f)]
for f in allFiles:
    try:
        if f[-5:] == ".datx":
            rawFilesToConvert.append(f)
    except:
        pass

## Pair related filenames
pairdFiles = []
for fEvent in eventFilesToConvert:
    for fRaw in rawFilesToConvert:
        if fEvent[:10] == fRaw[:10]:
            pairdFiles.append([fEvent, fRaw])

for f in pairdFiles:
    print("Processing file: " + f[0][:-11])
    dfEvents = pd.read_csv(f[0])
    # Counting sit to stand transitions
    sitToStandTransitions, sitToStandEventTimes = count_transitions('SiSt', dfEvents)
    # Counting sit to stand transitions
    standToSitTransitions, standToSitEventTimes = count_transitions('StSi', dfEvents)
    sitToStandEventTimes = pd.to_datetime(sitToStandEventTimes, unit='d', origin='1899-12-30')
    standToSitEventTimes = pd.to_datetime(standToSitEventTimes, unit='d', origin='1899-12-30')
    # Creating DF
    dfObjSiSt = pd.DataFrame(columns=['Transition Id', 'Recording Id', 'Serial Number', 'Time', 'Sample Number', 'X Axis', 'Y Axis', 'Z Axis'])
    dfObjStSi = pd.DataFrame(columns=['Transition Id', 'Recording Id', 'Serial Number', 'Time', 'Sample Number', 'X Axis', 'Y Axis', 'Z Axis'])
    # Load in the raw accel data
    meta, signals = load_activpal_data(f[1])
    total_time = meta.stop_datetime - meta.start_datetime
    total_samples = int(total_time.total_seconds() * 20)
    arr = np.array([meta.start_datetime + datetime.timedelta(seconds=i*0.05) for i in range(total_samples)])
    x = signals[:total_samples,0]
    y = signals[:total_samples,1]
    z = signals[:total_samples,2]
    index_array = range(0, total_samples)
    chunk = pd.DataFrame({'Time':arr, 'Index':index_array, 'X':x, 'Y':y, 'Z':z})
    # Find the index of the SiStEventTimes & SiStEventTimes by looping over them and collect relevant transition data
    dfObjSiSt = collect_events(sitToStandEventTimes, dfObjSiSt, chunk, meta, 'SiSt')
    dfObjStSi = collect_events(standToSitEventTimes, dfObjStSi, chunk, meta, 'StSi')
    # Save the DFs to csv
    dfObjSiSt.to_csv(f[0][:-27] + '_sit_to_stand_transitions.csv', index=False)
    print("Sit to stand transitions: " + str(sitToStandTransitions))
    dfObjStSi.to_csv(f[0][:-27] + '_stand_to_sit_transitions.csv', index=False)
    print("Stand to sit transitions: " + str(standToSitTransitions))
    end = time.time()
    # Save the meta data to log file
    with open(f[0][:-27] + "_log.txt", "a+") as text_file:
        print(f"Filename: {fEvent[:-4]}", file=text_file)
        print(f"Sit to stand transitions: {sitToStandTransitions}", file=text_file)
        print(f"Stand to sit transitions: {standToSitTransitions}", file=text_file)
        print(f"Software runtime: {end - start} seconds", file=text_file)
        print("----------")