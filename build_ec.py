import datetime
import functies as functie
import os
import time

soort = 'mta'
jaar = '2020'
folder_path = "Y:\\comverse\\" + soort + "\\new\\"
folder_path_processed = "Y:\\comverse\\" + soort + "\\processed\\"
destination_path = "F:\\python\\processing\\" + soort + "\\"
EC = 'Comverse (mta) 2020'
mode = 'full'

functie.cleanDBFarm()

print("start fetching files: "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
prepare = functie.prepareFilesEC(soort, folder_path, jaar)
print("done fetching files: "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
if prepare == 'done':
    print("start building: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    build = functie.buildEC(EC, mode)
    print("done building: " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    if build == 'True':
        print("start moving files: "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        movefiles = functie.moveFilesEC(soort, destination_path, folder_path_processed, jaar)
        print("done moving files: "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        print("Something went wrong at build level")
else:
    print("Something went wrong at preparing the files")