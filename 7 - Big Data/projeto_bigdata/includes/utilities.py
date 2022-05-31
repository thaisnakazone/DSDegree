# Databricks notebook source

from pyspark.sql.session import SparkSession
  
def _generate_file_handles(table: str, raw_path: str):

    file = f"northwind_{table}.csv"

    dbfsPath = raw_path
    dbfsPath += file

    driverPath = "file:/databricks/driver/" + file

    return file, dbfsPath, driverPath