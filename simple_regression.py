# import numpy as np
# import math
# import random
# data_points_X=[0,1,2,3,4,5,6,7]
# data_points_y=[-1,-0.5,0,1,2,1,0.5,-1]
#
import json
# data_points_X_t=[0,1,2,3,4,5,6,7,8]
# data_points_y_t=[-1,-0.5,0,1,2,1,0.5,-1,-1.5]
# #
# # PHI=np.array([[math.sin(1),0],[math.sin(1),1],[math.sin(1),2],[math.sin(1),3],[math.sin(1),4],[math.sin(1),5],
# #               [math.sin(1),6],[math.sin(1),7],[math.sin(1),8],[math.sin(1),9],[math.sin(1),10],[math.sin(1),11],
# #               [math.sin(1),12],[math.sin(1),13],[math.sin(1),14]])
# # # PHI=np.array([[1,0],[1,1],[1,2],[1,3],[1,4],[1,5],[1,6],[1,7],[1,8],[1,9]])
# # t=np.array([[-1],[-0.5],[0],[1],[2],[1],[0.5],[-1],[-1.5],[-2],[-1.5],[-1],[-0.5],[0],[1]])
# PHI = []
# t = []
# last_phi = []
# last_t = []
# for i in np.arange(0,1000,0.1):
#     if i < 999.8:
#         PHI.append([math.sin(1),i])
#         # t.append([random.uniform(-10.0,10.0)*math.sin((i*random.uniform(-2.0,4.0)))])
#         t.append([math.sin(i)])
#     else:
#         last_phi.append([math.sin(1),i])
#         # last_t.append([random.uniform(-10.0,10.0)*math.sin((i*random.uniform(-2.0,4.0)))])
#         last_t.append([math.sin(i)])
#
#
# w_ls=np.dot(np.linalg.pinv(PHI),t)
# w_0 = w_ls[0]
# w_1 = w_ls[1]
# last_time= last_phi[0][1]
#
# # y = ((math.sin((w_0) + w_1) * 8))
# # y = ((math.sin(w_0) + w_1 * last_time))
# # y = math.sin((w_0) + (w_1*last_time))
# # y = math.sin(w_0) + math.sin(w_1*last_time)
# # y = math.sin(w_0) + math.sin(w_1*last_time)
# y =  w_0 + (w_1*last_time)
# print(f"Predicted:{y}")
# print(last_t)
all_lines_ts_normal:[dict] = []
all_lines_ts_as_string:[dict] = []
y_werte = []
x_werte= []
x_werte2= []
from datetime import datetime, timedelta
x = datetime.now()
# with open("/home/simonm/Documents/Marco_Bosch_daten/ALL0004/F0004CH1Clean.csv","r") as infile:
with open("/home/simonm/Documents/Marco_Bosch_daten/Test_Config3_28062022_164915.csv","r") as infile:
    for line in infile:
        line_splitted = line.split(",")
        x_werte.append(line_splitted[3])
        y_werte.append(line_splitted[4])
        # x_werte.append(line_splitted[3])
        # x_werte2.append(line_splitted[4])
        # y_werte.append(x)
        # x += timedelta(milliseconds=1)
#periode circa 7 Tage bei 0470c4f4 minus cosinus nehmen
# with open("out.csv", "w") as outfile:
#     outfile.write("parameter,value,timestamp\n")
# with open("/home/simonm/Downloads/77065804out.csv") as infile:
#     for line in infile:
#         # print(line)
#         line = line.split("\t")
#         line_dict_origin:dict = line[0]
#         line_dict = json.loads(line_dict_origin)
#         line_dict2 = json.loads(line_dict_origin)
#         clean_time = line[-1].replace("\n","")
#         #1 februar bis 1 märz
#         if clean_time > "2021-02-07 0:00:00.000+00"\
#                 and clean_time < "2021-02-14 00:00:00.000+00":
#             y_werte.append(line_dict.get("0470c4f4"))
#             x_werte.append(clean_time)
#             line_dict.update({"time":clean_time})
#             line_dict2.update({"time":f"{clean_time}"})
#             all_lines_ts_normal.append(line_dict)
#             all_lines_ts_as_string.append(line_dict2)
# print(x_werte)
# print(y_werte)
        # line[0]=line[0].replace("{", "").replace("}","").replace(" ","").replace(":",",")
        # with open("out.csv","a") as outfile:
        #     outfile.write(f"{line[0]},{line[1]}")
# file = open("sample.txt", "w+")
# file2= open("sample2.txt","w+")
# # Saving the array in a text file
# # file.write(all_lines_ts_normal)
# file.close()
# file2.write(str(all_lines_ts_normal))
# file2.close()
# file3 = open("sample_str.txt", "w+")
# file4= open("sample_str2.txt","w+")
# # file3.write(all_lines_ts_as_string)
# file3.close()
# file4.write(str(all_lines_ts_as_string))
# file4.close()
print("dict fertig")
import matplotlib.pyplot as plt
# plt.plot(all_lines_ts_normal)
# plt.ylabel('some numbers')
# plt.xscale("log")
plt.scatter(x=x_werte,y=y_werte)
# plt.scatter(x=x_werte2,y=y_werte)
# plt.figure(dpi=5000)
plt.show()
print("plot fertig")
# import pandas as pd
# df = pd.read_csv('https://raw.githubusercontent.com/alod83/data-science/master/DataAnalysis/tourism/data/eurostat.csv')
# df = df[df['geo'] == 'IT']
# df = df[df['unit'] == 'NR']
# df = df[df['c_resid'] == 'TOTAL']
# df.drop(['c_resid', 'unit', 'nace_r2', '2019M11', 'geo'],axis=1, inplace=True)
# df = df.reset_index()
# df.drop(['index'],axis=1, inplace=True)
#
# columns = df.columns[::-1]
# for column in columns:
#     for row in range(1, len(df[column])):
#         if "b" in df[column][row]:
#             df[column] = df[column][row][:-2]
#         if ":" in df[column][row]:
#             df[column][row] = "0"
#
#
# y = []
# for column in columns:
#     df[column] = df[column].astype(str).astype(int)
#     y.append(df[column].sum())
#
# import matplotlib.pyplot as plt
# import numpy as np
#
#
# X = np.arange(0, len(columns))
#
# step = 5
# x_ticks = np.arange(0, len(columns)+step, step=12*step)
# x_labels = []
# x_labels.append(1990)
# for i in range(1, len(x_ticks)):
#     x_labels.append(x_labels[i-1]+step)
# plt.xticks(x_ticks, x_labels)
# plt.plot(X, y, color="red", linewidth=1)
#
# plt.grid()
# plt.xlabel('Months')
# plt.ylabel('Number of arrivals')
#
# plt.show()
# skip = 22*12
# X = X[skip:]
# y = y[skip:]
# def sinusoid(x,A,offset,omega,phase):
#     return A*np.sin(omega*x+phase) + offset
#
# T = 12
# def get_p0(y):
#
#     A0 = (max(y[0:T]) - min(y[0:T]))/2
#     offset0 = y[0]
#     phase0 = 0
#     omega0 = 2.*np.pi/T
#     return [A0, offset0,omega0, phase0]
#
#
#
# from scipy.optimize import curve_fit
# import math
#
# param, covariance = curve_fit(sinusoid, X, y, p0=get_p0(y))
#
# step = 1
# x_ticks = np.arange(skip, skip+len(X)+step+12, step=12*step)
# x_labels = []
# x_labels.append(2012)
# for i in range(1, len(x_ticks)):
#     x_labels.append(x_labels[i-1]+step)
# plt.xticks(x_ticks, x_labels)
# plt.ylabel('Number of arrivals')
# plt.plot(X, y, color="red", linewidth=1,linestyle='dashed')
# plt.plot(X, sinusoid(X, *param), color="blue", linewidth=1)
# plt.show()
#
#
#
# def get_peaks(y, metrics):
#     n = int(math.ceil(len(y)/T))
#     step = 0
#     x_peaks = []
#     y_peaks = []
#     for i in range(0,n):
#         peak_index = y.index(metrics(y[step:step+T]))
#         x_peaks.append(peak_index + skip)
#         y_peaks.append(y[peak_index])
#         step = step+T
#     return [x_peaks,y_peaks]
#
#
#
# # approximate curve of peaks with
# min_peaks = get_peaks(y,min)
# max_peaks = get_peaks(y,max)
#
#
#
# #calculate variable amplitude and variable offset
# A = []
# offset = []
# for i in range(0, len(min_peaks[1])):
#     c_a = (max_peaks[1][i] - min_peaks[1][i])/2
#     c_offset = min_peaks[1][i] + c_a
#     for j in range(0,T):
#         A.append(c_a)
#         offset.append(c_offset)
# # last two months of 2019 are not available
# A = A[:-2]
# offset = offset[:-2]
#
#
#
# features = [X, A, offset]
#
# def variable_sinusoid(features,omega,phase):
#     x = features[0]
#     A = features[1]
#     offset = features[2]
#     return A*np.sin(omega*x+phase) + offset
#
# def variable_get_p0(x, y):
#     phase0 = 0
#     omega0 = 2.*np.pi/T
#     return [omega0, phase0]
#
#
#
# param, covariance = curve_fit(variable_sinusoid, features, y, p0=variable_get_p0(X,y))
#
# plt.xticks(x_ticks, x_labels)
# plt.ylabel('Number of arrivals')
# plt.plot(X, y, color="red", linewidth=1,linestyle='dashed')
# plt.plot(X, variable_sinusoid(features, *param), color="blue", linewidth=1)
# plt.show()
#
#
#
# from sklearn import linear_model
#
# # reshape x_peaks
# x_min_peaks = list(map(lambda el:[el], min_peaks[0]))
# x_max_peaks = list(map(lambda el:[el], max_peaks[0]))
#
# # min model
# model_min = linear_model.LinearRegression()
# model_min.fit(x_min_peaks,min_peaks[1])
#
# # max model
# model_max = linear_model.LinearRegression()
# model_max.fit(x_max_peaks,max_peaks[1])
#
#
#
# x_min_peaks.append([x_min_peaks[len(x_min_peaks) -1][0] + T])
# x_max_peaks.append([x_max_peaks[len(x_max_peaks) -1][0] + T])
# y_pred_min = model_min.predict(x_min_peaks)
# y_pred_max = model_max.predict(x_max_peaks)
# # print(y_pred_max)
# # print(y_pred_min)
# plt.xticks(x_ticks, x_labels)
# plt.plot(X, y, color="red", linewidth=1,linestyle='dashed')
# plt.scatter(x_min_peaks, y_pred_min, color="green", linewidth=1,linestyle='dashed')
# plt.scatter(x_max_peaks, y_pred_max, color="green", linewidth=1,linestyle='dashed')
# plt.ylabel('Number of arrivals')
# plt.grid()
# plt.show()
#
#
# X_pred = np.array(X)
# month = X_pred[len(X_pred)-1]
# for i in range(0,T):
#     X_pred = np.append(X_pred,month)
#     month = month + 1
#
#
#
# index = len(max_peaks[0])-1
# c_a = (max_peaks[1][index] - min_peaks[1][index])/2
# c_offset = min_peaks[1][index] + c_a
# for j in range(0,T):
#     A.append(c_a)
#     offset.append(c_offset)
#
# features_pred = [X_pred,A,offset]
#
# plt.xticks(x_ticks, x_labels)
# plt.plot(X, y, color="red", linewidth=1,linestyle='dashed')
# plt.plot(X_pred, variable_sinusoid(features_pred, *param), color="blue", linewidth=1)
# plt.grid()
# plt.ylabel('Number of arrivals')
# # plt.savefig("data/sinusoid.png")
# plt.show()



# import csv
#
# table = []
# y_pred = variable_sinusoid(features_pred, *param)
# for i in range(0, len(X_pred)):
#     row = { 'x' : X_pred[i], 'y' : y_pred[i]}
#     table.append(row)
#
# names = table[0].keys()
# with open('data/arrivals_prediction.csv', 'w') as output_file:
#     dict_writer = csv.DictWriter(output_file, names)
#     dict_writer.writeheader()
#     dict_writer.writerows(table)

