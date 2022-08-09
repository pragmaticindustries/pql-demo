# Uncomment the following line to use apache-iotdb module installed by pip3
from datetime import datetime, timedelta
from random import random, randint

import numpy as np
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.RowRecord import RowRecord
from iotdb.utils.SessionDataSet import SessionDataSet

ip = "127.0.0.1"
port_ = "6668"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
name_array = ["current_position","current_velocity","current_torque","point_position","point_velocity","point_velocity_additive","point_torque","following_distance","checkvalue","dcbusvoltage","current_position_drive","current_velocity_drive","current_torque_drive","point_position_drive","point_velocity_drive","point_velocity_additive_drive","point_torque_drive","current_following_distance_drive1","current_following_distance_drive2","target_position"]
# ts = 1657290757019
# ts = 1657300030000
# ts = 1657322009000
ts = 1657542009000
for i in range(0,10000):
    print(i)
    first:bool = True
    with open("/home/simonm/Documents/Marco_Bosch_daten/Test_Config3_28062022_164915.csv","r") as infile:
        for line in infile:
    # #         # print(line)
            line_splitted = line.split(",")
    # #         # 03 ist der Ts in ms
    # #         # von 4 bis 23 alles WErte
    #         measurements_ = ["s_01", "s_02", "s_03", "s_04", "s_05", "s_06"]
    #         # values_ = [False, 10, 11, 1.1, 10011.1, "test_record"]
            values_ = line_splitted[4:24]
            # data_types_ = [
            #     TSDataType.BOOLEAN,
            #     TSDataType.INT32,
            #     TSDataType.INT64,
            #     TSDataType.FLOAT,
            #     TSDataType.DOUBLE,
            #     TSDataType.TEXT,
            # ]
            data_types_ = [
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT
            ]
            if not first:
                #for i in range(0,len(name_array)):
                session.insert_record("root.murr",ts,name_array, data_types_,values_)
                ts += 1
                # session.insert_record("root.murr",1657116325+int(line_splitted[3]),name_array[i],[TSDataType.TEXT],line_splitted[i+4])
            first:bool = False
print(ts)
print("fertig")
# def connect_and_fetch_iotdb(device_id,timestamppara, measurements, data_types, values):
#     try :
#         session = Session(ip, port_, username_, password_)
#         session.open(False)
#         zone = session.get_time_zone()
#         result = session.insert_record(device_id,timestamppara,measurements,data_types,values)
#         session.close()
#         return result
#     except Exception as error :
#         session.close ()
#         print("Fehleriotdb")
#         print(error)
#         return error
# #
# timestamp_pointer =  1648735366
# timestamp = 1635692566
# while timestamp < timestamp_pointer:
#     print("oben")
#     motor: int = randint(-5, 20)
#     #motor
#     connect_and_fetch_iotdb("root.plcadpater.1",timestamp,"metric",[TSDataType.INT64],motor)
#     connect_and_fetch_iotdb("root.plcadpater.2",timestamp,"metric",[TSDataType.INT64],motor)
#
#     position: int = randint(-400, 400)
#     #position
#     connect_and_fetch_iotdb("root.plcadpater.3",timestamp,"metric",[TSDataType.INT64],position)
#     connect_and_fetch_iotdb("root.plcadpater.4",timestamp,"metric",[TSDataType.INT64],position)
#
#     randval: int = randint(-2, 4)
#     #randval
#     connect_and_fetch_iotdb("root.plcadpater.5",timestamp,"metric",[TSDataType.INT64],randval)
#     connect_and_fetch_iotdb("root.plcadpater.6",timestamp,"metric",[TSDataType.INT64],randval)
#
#     print("unten")
# result: SessionDataSet = session.execute_query_statement("SELECT * FROM root.murr")
# print(result.get_column_names())
# print(result.next())