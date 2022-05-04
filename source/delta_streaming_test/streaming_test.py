# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as window
from datetime import date, timedelta, datetime
import os
import json
from delta.tables import *

kafka_brokers = "localhost:9092"

if __name__ == "__main__":
    spark = SparkSession.builder.appName('zeun' + '_' + os.path.abspath(__file__)).getOrCreate()
    sc = spark.sparkContext

    df = spark.readStream.format("kafka").options(
        **{
            "kafka.bootstrap.servers": kafka_brokers,
            "subscribe": "cdc_mysql_index.test.ctlg",
            "startingOffsets": "earliest",
            "maxOffsetsPerTrigger": 10000,  # maxOffsetsPerTrigger / partition 해서 각 파티션에 offset으로 들어감
            "enable.auto.commit": False
        }
    ).load()

    @F.udf(returnType=T.StructType([
        T.StructField('ctlg_seq', T.StringType()),
        T.StructField('svc_yn', T.StringType()),
        T.StructField('op', T.StringType())
    ]))
    def get_ctlg_info(value):
        if not value:
            return None
        op_manager = json.loads(value)
        if op_manager['op'] == 'd':
            return [op_manager['ctlg_seq'], op_manager['svc_yn'], op_manager['op']]
        return [op_manager['ctlg_seq'], op_manager['svc_yn'], op_manager['op']]

    df2 = df.where(F.col('value').isNotNull()).withColumn('ctlg_info', get_ctlg_info(F.col('value')))

    df2 = df2.select(
        F.col('timestamp'),
        F.col('ctlg_info.ctlg_seq'),
        F.col('ctlg_info.svc_yn'),
        F.col('ctlg_info.op')
    )

    def write_batch(df, _id):
        """
        :param df: dataframe
        :param _id:
        :return:
        데이터가 없으면 더이상 File이 써지지 않음
        오래된 파일 삭제 test => vaccume() 7days data
        check_point/commits, offsets 안에 오래된 offset 지워도 되는듯
        """
        df = df.withColumn('max_timestamp', F.max(F.col('timestamp')).over(window.Window.partitionBy(F.col('ctlg_seq'))))
        write_df = df.where(F.col('timestamp') == F.col('max_timestamp')).drop('max_timestamp')

        try:
            deltaTable = DeltaTable.forPath(spark, 'jieun/test2')
            deltaTable.vacuum()
            deltaTable.alias('old').merge(
                write_df.alias('new'),
                "old.ctlg_seq = new.ctlg_seq"
            ).whenMatchedUpdate(
                set={"svc_yn": F.col("new.svc_yn"), "op": F.col('new.op')}
            ).whenNotMatchedInsertAll().execute()

            deltaTable.delete(condition=F.col('op') == 'd')
            new_df = deltaTable.toDF()

            new_df.coalesce(10).write.format('delta').mode('overwrite').save('jieun/test2')
        except Exception as e:
            write_df.write.format('delta').mode('overwrite').save('jieun/test2')

    termination = df2.writeStream.foreachBatch(write_batch).trigger(processingTime="2 minutes").option('checkpointLocation', 'jieun/test2_check').start()
    termination.awaitTermination()

    exit(0)

