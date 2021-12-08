# Geoportal Hamburg IR Trafficcount
Collection of data extracted from Geoportal Hamburg and the script to scrape the data.

# About the Data

- sensor_ids_hamburg.csv includes the iot_id, lat/lon values and observation type of each station.
- sensor_data_hamburg.csv includes the iot_id, result and date (extracted from phenomenonTime) of each observation day.

# Outliers
IMAGE Outliers
Please pay close attention to the values returned by the stations with the ids 15776, 15780 and 15784. These stations have reported counts of around 20K usually, but have results ranging from 4-20 Million starting on january 27th ending February 5th 2021. Filtering these technical failures out is probably the most reasonable measure.

# Growing Station Network
IMAGE Station Network
Also consider that, judging from the available data, it seems that the network of sensors has been expanded gradually.

The first bike sensor results are from December 2019, where only 20 bike count sensors provide data. This number has been increasing until December 2020, where it reached the state of today (December 2021), which is a total of 127 stations. This increase in sample size and local distribution has to be considered when working with aggregations over all stations.

For the car sensors the first available data is from November 2020, starting with 490 stations already. This has been increasing to the 602 stations of today (December 2021).

# About the script

We decided to share the script we used to scrape the data from the web, so that interested users can download their own updated data. It is rudimentary and there is no error handling in the script, but it did the job for us. When scraping we ran across the following error sometimes: 
'''
Failed to establish a new connection: [Errno 113] No route to host'))
'''

Just rerun the script and eventually it will work and complete the write to csv. Of course this can be changed to your liking. If you wish to export the data otherwise, export the two dataframes accordingly.


