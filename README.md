# Geoportal Hamburg IR Trafficcount
This is a collection of data extracted from [Geoportal Hamburg](https://geoportal-hamburg.de/verkehrsportal/#) and the script to scrape the data.

More specifically this is an extract of the data that can be visualized under: 

Issues & Topics -> Subject Data -> Planungsdaten -> Verkehrsdaten (Infrarotdetektoren) Hamburg -> Rad & Kfz. 

This data is generated from infrared-sensors that count each passing car or bicycle. Sensors only count one type of vehicle per sensor.

# About the Data

## Content
- sensor_ids_hamburg.csv includes the iot_id, lat/lon values and observation type of each sensor.
- sensor_data_hamburg.csv includes the iot_id, phenomenonTime, result and date of the daily observations.

## Outliers
![Outliers](https://i.ibb.co/V9qFSb6/outliers.png)

Please pay close attention to the values returned by the stations with the ids 15776, 15780 and 15784. These stations have daily result sums of around 20K usually, but have results ranging from 4-20 Million starting on January 27th ending February 5th 2021. Filtering these technical failures out is probably the most reasonable measure.

## Growing Station Network
![Station Network Count](https://i.ibb.co/zFtqF2r/Stationcount.png)

Also consider that, judging from the available data, the network of sensors has been expanded gradually.

The first bike sensor results are from December 2019, where only 20 bike sensors provide data. This number has been increasing until December 2020, where it reached the state of today (December 2021), which is a total of 127 stations. This increase in sample size and local distribution has to be considered when working with aggregations over all stations.

For the car sensors the first available data is from November 2020, starting with 490 stations already. This has been increasing to the 602 stations of today (December 2021).

# About the script

We decided to share the script that we used to scrape the data from the web, so that interested users can download their own updated data. It is rudimentary and there is no error handling in the script, but it did the job for us.

The necessary modules must be installed for the script to work. Look at the import statements at the beginning of the script, to see which modules are required.

When scraping we ran across the following error sometimes: 
```
Failed to establish a new connection: [Errno 113] No route to host
```
Just rerun the script and eventually it will work and complete the write to csv. Of course this can be changed to your liking. If you wish to export the data otherwise, export the two dataframes accordingly.


