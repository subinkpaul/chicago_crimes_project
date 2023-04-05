# DataTalksClub's Data Engineering Zoomcamp Project

## Chicago City Crime Analysis
This is the final project as a part of the [Data Engineering Zoomcamp course](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/README.md). The goals of this project are to apply everything we learned in this course and build an end-to-end data pipeline that will help to organize data processing in a batch manner and to build analytical dashboard that will make it easy to discern the trends and digest the insights 


The period of the data processing will cover from 2001 to 2022.

## Problem Statement

The Chicago city police department has collected a large dataset of crimes that have occurred in the city over the past several years, and they are looking to analyze this data in order to better understand crime patterns and trends. However, the dataset is currently in a CSV format and is not optimized for efficient querying and analysis. Therefore, developing an end-to-end data pipeline that can transform the CSV data into a format that is more suitable for analysis.  The goal of this project is to create a streamlined and efficient process for analyzing crime data that can be used to inform decision-making and improve public safety in the city.


## Architecture diagram

![architecture chicago crimes](https://user-images.githubusercontent.com/88390708/230216468-ef38c0d0-0fc8-4394-99ce-8e2749eef9bc.jpg)

Visualization link [here](https://lookerstudio.google.com/s/lrQNEgBjkaE)

## Technologies


__Airflow__: To orchestrate the workflow
__Terraform__: As Infrastructure as code tool to build the resources efficiently
__Docker__: To containerize the code and infrastructure
__Google Cloud VM__: Machine instance where services like docker and airflow are hosted
__Google Cloud Storage__: As Data Lake
__Google BigQuery__: As Data Warehouse
__Apache Spark__: Run data transformation
__Google Dataproc cluster__: To run the Spark engine
__Google Looker Studio__: Visualization of the findings

