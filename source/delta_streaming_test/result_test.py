# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
from datetime import date, timedelta, datetime
import os
import json

if __name__ == '__main__':
    spark = SparkSession.builder.appName('zeun' + '_' + os.path.abspath(__file__)).getOrCreate()
    sc = spark.sparkContext
    df = spark.read.format('delta').load('jieun/test2')  # 같은 delta format으로 읽어야 데이터 처리가 옳게 됨
    df.where(df.ctlg_seq < 5000).show()
    df.where(df.ctlg_seq >= 5000).where(df.ctlg_seq < 10000).where(df.svc_yn == 'Y').show()
    df.where(df.ctlg_seq >= 10000).where(df.ctlg_seq < 50000).where(df.svc_yn == 'N').show()
    # df.show()
    exit(0)