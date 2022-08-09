import csv

with open('clean_data.csv', 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile)
    with open("/home/simonm/Downloads/Data_Config2_21042022_113006.csv","r") as infile:
        for line in infile:
            line_splited = line.replace(";",",")
            line_splited = line_splited.split(",")
            cleaned = line_splited[3:132]
            spamwriter.writerow(cleaned)
            # 3 index ist ms und dann 4 bis 132
            # spamwriter.writerow()
# import csv
# with open('innovators.csv', 'w', newline='') as file:
#     writer = csv.writer(file)
#     writer.writerow(["SN", "Name", "Contribution"])
#     writer.writerow([1, "Linus Torvalds", "Linux Kernel"])
#     writer.writerow([2, "Tim Berners-Lee", "World Wide Web"])
#     writer.writerow([3, "Guido van Rossum", "Python Programming"])
# import csv
#
# with open('newfilename.csv', 'w') as f2:
#     with open('home/simonm/Downloads/Data_Config2_21042022_113006.csv', mode='r') as infile:
#         csv_reader = csv.reader(infile, delimiter=',')
#         for row in csv_reader:
#             print(row)


