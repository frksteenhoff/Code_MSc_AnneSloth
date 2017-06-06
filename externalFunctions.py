# ------------------------------------------------------------------------------------------- #
# AUTHOR: Henriette Steenhoff, s134869
# All rights reserved, May 2017
#
# External functions used in 'Code_MSc_AnneSloth_s112862.ipynb' -- the data extraction 
# and plotting script for Anne Sloth Bidstrup's Master Thesis @ Danmarks Tekniske Universitet
# ------------------------------------------------------------------------------------------- #


# ------------------------------------------------------------------------------------------- #
## LIBRARIES
# ------------------------------------------------------------------------------------------- #
import pandas as pd
import re
import time
from datetime import date, timedelta
import datetime
import numpy as np
from collections import Counter 
from os import *
from os.path import isfile, join
from fractions import Fraction
import math

# Plotting tools
import plotly 
from IPython.display import Image 
import plotly.plotly as py
import plotly.graph_objs as go
# API access to plotting tools
plotly.tools.set_credentials_file(username='frksteenhoff2', api_key ='duu8hsfRmuI5rF2EU8o5')

# ------------------------------------------------------------------------------------------- #
## DATA PATHS
# ------------------------------------------------------------------------------------------- #
# Folder structure for different data -- Using Anne's folder structure from Dropbox
#weekNumber = date.today().isocalendar()[1]-1

base_path = "C:/Users/frksteenhoff/Dropbox/Data eksempel til Henriette/"
# Data locations
weekNumber = 18
netpath   = base_path + "Data week " + str(weekNumber) + "/Netatmo"
weekpath  = base_path + "Data week " + str(weekNumber)
PIRpath   = base_path + "Data week " + str(weekNumber) + "/ProcessedData/PIRReed/"
COMpath   = base_path + "Data week " + str(weekNumber) + "/ProcessedData/CompAcc/"

# Change back known folder structure
testpath  = base_path + "Program - extractWork/"
viz_path  = base_path + "Data week " + str(weekNumber) + "/Netatmo/Visualization"


# ------------------------------------------------------------------------------------------- #
## FUNCTIONS
# ------------------------------------------------------------------------------------------- #

# This function is used when saving files to a certain directory. It is used when the data 
# has been processed such that only the needed data is left. This is being done when merging 
# Acc/Compas and PIR/Reed.
# 
# Saving a dataframe as an .xlsx file
# alias     - room identifier
# string    - censortype
# dataFrame - all observations to save given as pandas dataframe
# feature   - the desired feature
# pattern   - xx
def saveDataToFile(alias, string, dataFrame, feature, pattern):
    # Taking care of aaaaall the different spellings of the rooms... adding to different files
    temp = dataFrame.loc[dataFrame[feature].str.contains(pattern)]
    
    # Only save file if it actually contains something
    if not temp.empty:
        full_path_filename = (alias+string).strip(' ') 
        writer = pd.ExcelWriter(full_path_filename, engine='xlsxwriter')
        print "Saving values for '%s', in all: %d" % (alias, len(temp))
        temp.to_excel(writer)
        writer.save()        

# ------------------------------------------------------------------------------------------- #

# Saving a dataframe to a file
# dataframe -- pandas dataframe to save
# string like name for file
# path -- the path to the folder to save the file
def saveDataframeToPath(dataFrame, fileName, path):
    chdir(path)
    writer = pd.ExcelWriter(fileName+".xlsx", engine='xlsxwriter')
    dataFrame.to_excel(writer)
    writer.save()        

# ------------------------------------------------------------------------------------------- #


# Extracting outdoor temperature information from outdoor "have file" for use in humidity calculation
# Input:
# dataFrame - have to contain timestamp named 'Timezone : Europe/Copenhagen', 'Hour' and 'Temperature'

# Average temp per hour of day for week 
def haveCalculation(dataFrame):
    hour_cnt = {}
    daysInWeek = []
    # Extract all days in week from 1st day and seven days forward
    # due to missing data this has to be stated explicitly
    for days in pd.date_range(dataFrame['Timezone : Europe/Copenhagen'].unique().min(), periods=7):
        daysInWeek.append(days.day)
        
    # Extract temp for each hour of each day
    for day in daysInWeek:
        for hour in range(0,24):
            # Find relevant hour of day
            dayCombo   = str(day)+"-"+str(hour)
            hourlyTemp = dataFrame.loc[dataFrame['con'].isin([dayCombo])]
            
            # Summing the temperatures
            # If no entries for given hour - 
            # impute with median for remaining days at same hour
            if len(hourlyTemp) == 0:
                dailyTemp = dataFrame.loc[dataFrame['Hour'].isin([hour])]
                hour_cnt[dayCombo] = dailyTemp['Temperature'].median()
            else:
                # Else add average of hour for specific day
                hour_cnt[dayCombo] = hourlyTemp['Temperature'].sum(axis=0)/(len(hourlyTemp))
    return hour_cnt

# ------------------------------------------------------------------------------------------- #


# Functions for extracting data from pandas dataframe
# dataFrame to use
# dataFrame feature
# dataFrame feature to sum over
def calculateHourlyTemp(dataFrame, hourName, tempName):
    hour_cnt = {}
    for hour in list(set(dataFrame[hourName])):
        hourly = dataFrame.loc[dataFrame[hourName].isin([hour])]
        hour_cnt[hour] = hourly[tempName].sum(axis=0)/(len(hourly))
    return hour_cnt

# ------------------------------------------------------------------------------------------- #


# Calculate mean temperature for each time slot of each weekday (specialized function)
# Plot against each households' temperatures during week
# 
# Calculates:
# - Hour of day
# - Day of week
# - A mean temperature based on all temperatures within a given hour of day
def createMeanTempForRoom(dataFrame):
    # This should not be possible with the new method
    if len(dataFrame.columns) > 5:
        # If data includes temp. co2, humidity, noise and pressure
        dataFrame.iloc[:,7] = pd.to_datetime(dataFrame.iloc[:,7])
        # Append our to existing dataframe
        dataFrame['Hour'] = dataFrame.iloc[:,7].dt.hour
        dataFrame['Day'] = dataFrame.iloc[:,7].dt.weekday
        dataFrame['newTemp'] = np.zeros(len(dataFrame))
    else:
        # If data includes temp, co2, humidity
        dataFrame.iloc[:,1] = pd.to_datetime(dataFrame.iloc[:,1])
        # Append our to existing dataframe
        dataFrame['Hour'] = dataFrame.iloc[:,1].dt.hour
        dataFrame['Day'] = dataFrame.iloc[:,1].dt.weekday
        dataFrame['newTemp'] = np.zeros(len(dataFrame))

    # New dataframe for weekly time series
    uniq_hour = pd.DataFrame()

    # Calculate mean temperature per hour of each weekday
    for day in list(set(dataFrame['Day'])):
        # Get all values from current day
        days = dataFrame.loc[dataFrame['Day'].isin([day])]

        for hour in list(set(days.Hour)):
            # Get all temp for each hours within days 
            hours = days.loc[days['Hour'].isin([np.int64(hour)])]

            # Add mean temperature
            temp = hours.iloc[[0]]
            temp.newTemp = hours['Temperature'].sum(axis=0)/len(hours)
            if not temp.empty:
                # Add the temperatures to dataframe with one entry per hour of day
                uniq_hour = uniq_hour.append([temp])
    return uniq_hour

# ------------------------------------------------------------------------------------------- #


# Plot temperatures for each room against overall temp
# Encoding documentation in Python, https://docs.python.org/2/howto/unicode.html
def createTempPlot(dataFrame, tempLiv, tempBed, location_n, room_n, pattern, tAxes, bgBorder, rangeMin, rangeMax):
    #Change back to correct working directory
    # If directory path does not exist - create it
    if not path.exists(viz_path):
        makedirs(viz_path)
    chdir(viz_path)
    
    # Save plot to proper location
    plotType = '-temp' # type: temperature
        
    # Plotting weekly temperature overview - single home
    trace1 = go.Scatter(
              x = pd.to_datetime(dataFrame.iloc[:,1]),
              y = list(dataFrame.Temperature),
        name = 'Din bolig', # Style name/legend entry with html tags
        connectgaps=False
    )

    # Choose whether to use living room/kitchen temp or bedroom temp - overall
    print room_n
    new_pat = re.compile(pattern)
    if new_pat.match(room_n):
        trace2 = go.Scatter(
              x = pd.to_datetime(tempLiv.Timezone),
              y = list(tempLiv.newTemp),
        name = 'Hedelyngen',
        connectgaps = False
        )
    else:
        trace2 = go.Scatter(
              x = pd.to_datetime(tempBed.Timezone),
              y = list(tempBed.newTemp),
        name = 'Hedelyngen',
        connectgaps = False
    )
        
    data = [trace1, trace2]

    # Setting layout details for plot
    layout = go.Layout(
        autosize = False,
        width = 600,
        height = 350,
        paper_bgcolor = 'rgba(0, 0, 0, 0)',
        plot_bgcolor = "rgba(0, 0, 0, 0)",
        showlegend = False,
        xaxis=dict(
            tickfont=dict(
                size=14,
                color=tAxes
            )
        ),

        yaxis=dict(
            range=[rangeMin,rangeMax],
            zeroline=True,
            titlefont=dict(
                size=16,
                color=tAxes
            ),
            tickfont=dict(
                size=16,
                color=tAxes
            )
        ),

        legend=dict(
            x=0,
            y=1.0,
            bgcolor=bgBorder,
            bordercolor=bgBorder
        )
    )

    # Give name for plot to be saved
    room = room_n.encode("ascii", "ignore").replace("/", "")
    filen = location_n + plotType + "-" + room + ".png"
    filen = filen.replace(" ", "")

    # Create and save figure
    fig = go.Figure(data=data, layout=layout)
    py.image.save_as(fig, filename=filen)
    #Image(fullPathToPlot) # Display a static image

# ------------------------------------------------------------------------------------------- #


# Plot humidity for each room
# Extract ``have`` file information -- outdoor temperature
def createHumidityPlot(dataFrame, hour_cnt_netatmo, hour_cnt_have, room_n, location_n, col1, col2, col3, livpat, bedpat):
    # If directory path does not exist - create it
    livingroom_pattern = re.compile(livpat)
    bedroom_pattern = re.compile(bedpat)
    if not path.exists(viz_path):
        makedirs(viz_path)
    chdir(viz_path)
    
    # initialize arrays for limit values rh_gul and rh_roed
    room     = ""
    rh_gul   = []
    rh_roed  = []
    pmv_list = []
    pmi_list = []
    hr_data  = []
    rh_boundaries = pd.DataFrame()
    
    # Calculations for humidity equation
    # Constants
    for i in range(0,24):
        t_i   = np.asarray(hour_cnt_netatmo.values()) + 273.15  # converted to Kelvin
        t_ude = np.asarray(hour_cnt_have.values())    + 273.15  # converted to Kelvin
        t_v   = np.add(Fraction(1,3)*t_ude[i], Fraction(2,3)*t_i[i])

        # Limit equations
        p_mv  = math.exp(77.3450 + 0.0057*t_v    - 7235.0 / t_v)    / (t_v**8.2)
        p_mi  = math.exp(77.3450 + 0.0057*t_i[i] - 7235.0 / t_i[i]) / (t_i[i]**8.2)
        
        # Equation for upper and lower bound
        rh_gul.append(0.6 * p_mv / p_mi)
        rh_roed.append(0.75 * p_mv / p_mi)
        pmi_list.append(p_mi)
        pmv_list.append(p_mv)

    print "Humidity boundaries calculated"
    # Humidity
    # Only for rooms with humidity measure
    hr_data = dataFrame[['Humidity', 'Kelvin', 'Hour', 'Temperature','Date','Time']] # Using Kelvin temperatures
    hr_data['rh_gul'] = np.zeros(len(hr_data))
    hr_data['rh_roed'] = np.zeros(len(hr_data))
    # Initialize rh-dict for value groups
    rh_dict = dict.fromkeys(['middleValue', 'lowValue', 'highValue'], 0)
    
    # Check netatmo data against humidity boundaries for each hour of day
    for i in range(0, len(hr_data['Humidity'])):
        # if humidity value are between rh_gul and rh_roed at given hour
        if hr_data.iloc[i,0] > rh_gul[hr_data.iloc[i,2]] * 100 and hr_data.iloc[i,0] < rh_roed[hr_data.iloc[i,2]] * 100:
            rh_dict['middleValue'] += 1
        # If humidity is less than rh_gul
        elif hr_data.iloc[i,0] < rh_gul[hr_data.iloc[i,2]] * 100:
            rh_dict['lowValue'] += 1
        # If humidity is greater than rh_roed 
        elif hr_data.iloc[i,0] > rh_roed[hr_data.iloc[i,2]] * 100:
            rh_dict['highValue'] += 1
        else:
            print 'Something fails'
        # For each value, set the  limits...
        hr_data.iloc[i,6] = rh_gul[hr_data.iloc[i,2]]
        hr_data.iloc[i,7] = rh_roed[hr_data.iloc[i,2]]
    
    room     = room_n.encode("ascii", "ignore").replace("/", "")
    print room
    room_id = ""
    if livingroom_pattern.match(room):
        room_id = "livingroom"
    elif bedroom_pattern.match(room):
        room_id = "bedroom"
    else:
        room_id = "other"

    #Save HR data for room to file - bedroom/livingroom as extension to match room in house
    #saveDataframeToPath(hr_data[['Date', 'Time', 'Humidity', 'Temperature', 'rh_gul', 'rh_roed']], location_n + '-RH-' + room_id, netpath+"/HR")
    #chdir(viz_path)

    # Save plot to proper location
    plotType = '-RH-'           # type: relative humidity
    room     = room_n.encode("ascii", "ignore").replace("/", "")
    filen    = location_n + plotType + room + ".png"
    filen    = filen.replace(" ", "")

    # Plot over fresh air 
    fig = {
        'data': [{'labels': ['Under anbefaling', 'Indenfor anbefaling', 'Over anbefaling'],
                  'values': [rh_dict['lowValue'],
                             rh_dict['middleValue'],
                             rh_dict['highValue']],
                  'type': 'pie', 
                  'marker': {'colors': [col1,
                                        col2,
                                        col3]},
                  'textinfo': 'none'}],
        'layout': { 'autosize': False,
                    'width': 350,
                    'height': 350,
                    "paper_bgcolor": "rgba(0, 0, 0, 0)",
                    "plot_bgcolor": "rgba(0, 0, 0, 0)",
                    'showlegend': False}
         }
    print rh_dict['lowValue'], rh_dict['middleValue'], rh_dict['highValue'] ,  len(hr_data)

    # Save to folder
    py.image.save_as(fig, filename=filen)
    print "Plot created " + filen
    # Plot result
    #Image(fullPathToPlot) # Display a static image
    #py.iplot(fig)

# ------------------------------------------------------------------------------------------- #





# ------------------------------------------------------------------------------------------- #
