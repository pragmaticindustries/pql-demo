from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType

ip = "127.0.0.1"
port_ = "6662"
username_ = 'root'
password_ = 'root'
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
name_array = []
data_types = []
for i in range(1,129):
    name_array.append(f"channel_{i}")
    data_types.append(TSDataType.TEXT)
# name_array = ["current_position","current_velocity","current_torque","point_position","point_velocity","point_velocity_additive","point_torque","following_distance","checkvalue","dcbusvoltage","current_position_drive","current_velocity_drive","current_torque_drive","point_position_drive","point_velocity_drive","point_velocity_additive_drive","point_torque_drive","current_following_distance_drive1","current_following_distance_drive2","target_position"]
# ts = 1657290757019
# ts = 1657300030000
# ts = 1657322009000
# ts = 1657542009000
first:bool = True
with open("/home/simonm/Downloads/Data_Config2_21042022_113006.csv","r") as infile:
    for line in infile:
        line_splited = line.replace(";",",")
        line_splited = line_splited.split(",")
        cleaned = line_splited[4:132]
        if not first:
                # #for i in range(0,len(name_array)):
            session.insert_record("root.murr",int(line_splited[3]),name_array, data_types,cleaned)
                # session.insert_record("root.murr",1657116325+int(line_splitted[3]),name_array[i],[TSDataType.TEXT],line_splitted[i+4])
        first:bool = False
print("fertig")