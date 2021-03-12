import os
import pandas as pd
import numpy as np
import time
from dask import dataframe as dd


# Functions
def collect_events(EventTimes,  dataFrameObject, data):
    for i, eventTime in enumerate(EventTimes):
        df_sort = data.iloc[(data.Time.astype(float) - float(eventTime)).abs().argsort()[:1]]
        x = df_sort.index.tolist()
        y = df_sort.Time.tolist()
        timeDiff = abs(float(y[0]) - float(eventTime))
        if timeDiff < 1*10**-6:
            transitionIDs = np.full(shape=120,fill_value=i,dtype=int)
            serialNo = [f[0][:-11]]*120
            eventDf = data.iloc[x[0]-60:x[0]+60]
            eventDf = eventDf.assign(SerialNumber = serialNo)
            eventDf = eventDf.assign(RecordingID = serialNo)
            eventDf = eventDf.assign(TransitionId = transitionIDs)
            eventDf = eventDf[['TransitionId', 'RecordingID', 'SerialNumber','Time','Index','X', 'Y', 'Z']]
            eventDf.columns = ['Transition Id', 'Recording Id', 'Serial Number', 'Time', 'Sample Number', 'X Axis', 'Y Axis', 'Z Axis']
            dataFrameObject = dataFrameObject.append(eventDf, ignore_index = True)
    return dataFrameObject

def count_transitions(case):
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
        if f[-25:] == "AccelDataUncompressed.csv":
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
    start = time.time()
    # Loop through group of files and begin processing
    print("Processing file: " + f[0][:-11])
    dfEvents = pd.read_csv(f[0])

    # Counting sit to stand transitions
    sitToStandTransitions, sitToStandEventTimes = count_transitions('SiSt')

    # Counting sit to stand transitions
    standToSitTransitions, standToSitEventTimes = count_transitions('StSi')

    dfObjSiSt = pd.DataFrame(columns=['Transition Id', 'Serial Number', 'Time', 'Sample Number', 'X Axis', 'Y Axis', 'Z Axis'])
    dfObjStSi = pd.DataFrame(columns=['Transition Id', 'Serial Number', 'Time', 'Sample Number', 'X Axis', 'Y Axis', 'Z Axis'])
    
    # Load in the raw accel data into chunks
    for chunk in dd.read_csv(f[1],  blocksize=25e6):
        print(chunk)
        breakpoint()
        chunk = chunk['sep=;'].apply(lambda x: pd.Series(x.split(';')))
        chunk.columns = ["Time", "Index", "X", "Y", "Z"]
        try:
            chunk.apply(pd.to_numeric)
            chunk = chunk.reset_index(drop=True)
        except:
            chunk = chunk.iloc[1:,]
            chunk = chunk.reset_index(drop=True)
            chunk.apply(pd.to_numeric)
        # Find the index of the SiStEventTimes & SiStEventTimes by looping over them and collect relevant transition data
        dfObjSiSt = collect_events(sitToStandEventTimes, dfObjSiSt, chunk)
        dfObjStSi = collect_events(standToSitEventTimes, dfObjStSi, chunk)

    dfObjSiSt.to_csv(f[0][:-27] + '_sit_to_stand_transitions.csv', index=False)
    print("Sit to stand transitions: " + str(sitToStandTransitions))

    dfObjStSi.to_csv(f[0][:-27] + '_stand_to_sit_transitions.csv', index=False)
    print("Stand to sit transitions: " + str(standToSitTransitions))

    end = time.time()

    with open("Log.txt", "a+") as text_file:
        print(f"Processing file: {f[:-4]}", file=text_file)
        print(f"Sit to stand transitions: {sitToStandTransitions}", file=text_file)
        print(f"Stand to sit transitions: {standToSitTransitions}", file=text_file)
        print(f"Runtime to process file pair is {end - start}", file=text_file)
        print("----------")