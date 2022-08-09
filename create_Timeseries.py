import statistics

from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor

ip = "127.0.0.1"
port_ = "6661"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
# for i in range(3,129):
session.create_time_series(f"root.murr.channel.111",TSDataType.INT64,TSEncoding.PLAIN, Compressor.SNAPPY)
exit(1)