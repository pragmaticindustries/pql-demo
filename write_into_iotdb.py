from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from iotdb.utils.NumpyTablet import NumpyTablet

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"

def create_session():
    session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
    session.open(False)
    return session

def insert_tablet(tablet:Tablet):
    session:Session = create_session()
    session.insert_tablet(tablet)
    session.close()

