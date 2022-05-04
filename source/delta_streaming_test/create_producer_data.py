from kafka import KafkaProducer
from json import dumps
from datetime import date, timedelta, datetime
import logging
import json

TOPIC = 'cdc_mysql_index.test.ctlg'


def create_sample_data(producer):
    for idx in range(50000):
        if idx < 10000:
            data = {'timestamp': datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f"), 'ctlg_seq': idx, 'svc_yn': 'Y', 'op': 'c'}
        else:
            data = {'timestamp': datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f"), 'ctlg_seq': idx, 'svc_yn': 'N', 'op': 'c'}
        producer.send(TOPIC, value=data)
        producer.flush()
    for idx in range(50000):
        if idx < 5000:
            data = {'timestamp': datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f"), 'ctlg_seq': idx, 'svc_yn': 'Y', 'op': 'd'}
        elif idx < 10000:
            data = {'timestamp': datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f"), 'ctlg_seq': idx, 'svc_yn': 'N', 'op': 'u'}
        else:
            data = {'timestamp': datetime.today().strftime("%Y-%m-%d %H:%M:%S.%f"), 'ctlg_seq': idx, 'svc_yn': 'Y',
                    'op': 'u'}
        producer.send(TOPIC, value=data)
        producer.flush()
    """
    예상 결과
    0-4999: 데이터 X
    5000-9999: svc_yn : n
    10000-50000: svc_yn : y
    """


if __name__ == '__main__':
    producer = KafkaProducer(acks=0, compression_type='gzip', bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    try:
        create_sample_data(producer)
    except Exception as e:
        logging.error(e)
        exit(1)
    exit(0)

