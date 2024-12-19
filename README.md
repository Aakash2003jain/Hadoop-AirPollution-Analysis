# Hadoop Air Pollution Analysis

## Overview
This project analyzes air pollution data using Hadoop MapReduce. It processes a large dataset of air pollution measurements, specifically focusing on air quality index (AQI) and CO concentration, and calculates various insights such as the average AQI per state, highest CO concentration, and AQI values above a certain threshold.

The dataset contains air pollution data for various local site names of a state, and the analysis aims to provide useful information for environmental studies and policy-making.

## Features
- **Average AQI Calculation**: Computes the average AQI for each state.
- **Highest CO Concentration**: Finds the highest CO concentration for each city or state.
- **AQI Above Threshold**: Filters cities or states with AQI values above a predefined threshold.

## Dataset 
The dataset includes detailed air pollution data, with the following key columns:

- **Date**: The date of measurement.
- **City/State**: The location where air quality data was collected.
- **Daily Max 8-hour CO Concentration**: Maximum 8-hour CO concentration measured in parts per million (ppm).
- **Daily AQI Value**: The calculated Air Quality Index for the day.
- **County/CBSA Name**: County and metropolitan area names.
- **Site Latitude and Longitude**: Geographic coordinates of the measurement site,
and more.
The dataset covers a wide range of local site names of a state, with high granularity for air pollution trends.

## Technologies Used
- **Hadoop**: For distributed processing using MapReduce.
- **Java**: For implementing the MapReduce logic.
- **HDFS (Hadoop Distributed File System)**: For storing and processing large datasets.

## Getting Started
### Steps to Run the Project

1. **Clone the repository**:
   Clone this repository to your local machine:
   ```bash
   git clone https://github.com/Aakash2003jain/Hadoop-AirPollution-Analysis.git
   ```
