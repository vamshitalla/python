'''
    File name: m6LiftAndShift.py
    Author: Vamshi Talla
    Date created: 4/03/2018
    Date last modified: 4/04/2018
    Python Version: 3.6
'''

import sys
import subprocess
from pyspark.sql import SparkSession
import datetime

print("Starting Python Script...", sys.argv[0])

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Process M6 Files") \
                    .getOrCreate()

print("Obtained Spark Session!")

#Command line arguments
m6SourcePath = sys.argv[1]
fileType = sys.argv[2]
division = sys.argv[3]
cycleDate = sys.argv[4]
arguments = len(sys.argv) - 1

m6TargetPath = "/user/v362809/target/"

#House keeping function to re(move) any tmp directories
def cleanUp(cmd):
    print("Cleaning up tmp directories/files...")
    subprocess.Popen(cmd, stdout=subprocess.PIPE)

# Generic function to read data from (source&target) HDFS directories using SparkSession
def readM6Data(arg1, arg2, arg3, arg4):
    if (str(arg1) == m6SourcePath):
        print("Reading M6 Source Data...")
        filename = "*" + str(arg2) + "_" + str(arg3) + "_" + str(arg4) + "*"
        absolutepath = str(arg1) + filename
    else:
        print("Reading M6 Target Data...")
        filename = "part*"
        absolutepath = str(arg1)+fileType+"/"+division+"/"+transDate+"/"+filename
    cmd = ["hdfs", "dfs", "-ls", absolutepath]
    file = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in file.stdout:
        filename = line.strip().split(None, 7)[-1].decode('utf-8')
    return (spark.read.option("header", "true").option("delimiter", "|").csv(filename))

# Function to save data in HDFS in target location
def saveM6Data(outputDF):
    outputPath = m6TargetPath+fileType+"/"+division
    deletePath = outputPath+"/"+transDate
    tmpPath = m6TargetPath+fileType+"/"+division+"/"+"/tmp/"+transDate

    outputDF.write\
            .option("header", "true")\
            .option("delimiter", "|")\
            .save(path=tmpPath, format='csv', mode='overwrite')

    hdfsMoveCmd = ["hdfs", "dfs", "-mv", deletePath, "/user/v362809/"]
    cleanUp(hdfsMoveCmd)
    print("Temp Move completed!!")

    hdfsCopyCmd = ["hdfs", "dfs", "-cp", tmpPath, outputPath]
    cleanUp(hdfsCopyCmd)
    print("Copy Completed!!")

#Function to update data by joining source and target data
def updateM6Data():
    print("Joining Source and Target DFs")
    cond = [m6SourceDF.DT_KEY == m6TargetDF.DT_KEY, m6SourceDF.STO_KEY == m6TargetDF.STO_KEY, m6SourceDF.ITM_SCN_CD == m6TargetDF.ITM_SCN_CD]

    outputDF = m6SourceDF.join(m6TargetDF, cond, 'inner').select(m6SourceDF.DT_KEY, m6SourceDF.STO_KEY, m6SourceDF.ITM_SCN_CD, m6SourceDF.SALES_UNITS,
                                                   m6SourceDF.SALES_MEAS, m6SourceDF.SALES_AMT, m6SourceDF.SALES_MEAS_UOM, m6SourceDF.GM_AMT,
                                                   m6SourceDF.AVE_UNIT_PRC, m6SourceDF.AVE_MEAS_PRC, m6SourceDF.INNER_SALES_UNITS,
                                                   m6SourceDF.EST_SALES_CD, m6SourceDF.DT_EXCP_CD, m6SourceDF.UPD_DTM)
    saveM6Data(outputDF)

# Script starts here!!
if(arguments == 4 and (len(cycleDate) != 0)):
    print("CAUTION: Processing historical data!!!")
    print("Processing fileType %s for division %s for historical cycleDate %s at inputPath %s" % (fileType, division, cycleDate, m6SourcePath))
    m6SourceDF = readM6Data(m6SourcePath, fileType, division, cycleDate)
    transDatesDF = m6SourceDF.select("DT_KEY").distinct().orderBy("DT_KEY")
    for row in transDatesDF.collect():
        transDate = str(row.DT_KEY).replace('-', '')
        m6TargetDF = readM6Data(m6TargetPath, fileType, division, transDate).cache()
        updateM6Data()

elif(arguments == 4 and (len(cycleDate) == 0)):
    print("NOTE: Processing BAU data!!!")
    cycleDate = datetime.date.today().strftime('%Y%m%d')
    print("Processing fileType %s for division %s for BAU cycleDate %s" %(fileType, division, cycleDate))

else:
    print("please check the arguments passed!")
    exit(1)