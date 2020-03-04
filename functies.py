import os
import os.path
import glob
import psycopg2
import shutil
import sys
import fnmatch
import time
import subprocess
from ftplib import FTP, error_perm

def connect(jaar):
    db_conn = \
        "host = '' dbname = '" + jaar + "' user = '' password = ''"
    return psycopg2.connect(db_conn)

def copytree2(source, dest, ):
    # os.mkdir(dest)
    print(source, ' --- ', dest)
    dest_dir = os.path.join(dest, os.path.basename(source))
    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    shutil.copytree(source, dest_dir)


def buildEC(ECube, mode):
    cmdline = ["cmd", "/q", "/k", "echo off"]
    cmd = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    cmdInfo = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    batchInfo = b"""\
    C:
    cd C:\Program Files\Sisense\Prism
    psm ecube info name="ElasticubeToImport"
    exit
    """

    batch = b"""\
    C:
    cd C:\Program Files\Sisense\Prism
    psm ecube build name="ElasticubeToImport" mode=type_mode
    exit
    """

    print('start building cube')
    ECubeBytes = bytes(ECube, 'utf-8')
    modeBytes = bytes(mode, 'utf-8')
    batch = batch.replace(rb'ElasticubeToImport', ECubeBytes)
    batch = batch.replace(rb'type_mode', modeBytes)
    # print(batch)
    cmd.stdin.write(batch)
    cmd.stdin.flush()
    result = cmd.stdout.readlines()
    # print(result)

    print('build done: fetching cube status')
    ECubeBytes = bytes(ECube, 'utf-8')
    batchInfo = batchInfo.replace(rb'ElasticubeToImport', ECubeBytes)
    cmdInfo.stdin.write(batchInfo)
    cmdInfo.stdin.flush()
    result = cmdInfo.stdout.readlines()

    for line in result:
        if 'LastBuildSucceeded'.encode() in line.rstrip():
            string = line.decode("utf-8")
            removestring = string.replace("LastBuildSucceeded: ", "")
            finalstring = removestring.strip()

    return finalstring

def exportEC(ECube):
    check_file = 'F:\\python\\' + ECube + '.ecdata'
    cmdline = ["cmd", "/q", "/k", "echo off"]
    cmdInfo = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    cmdStop = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    cmdExport = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    cmdRemoteAttach = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    cmdCopy = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    batchCopy = b"""\
        C:
        ncftpput -u cubedata -p 3cub3data -R ip / "ElasticubeToImport"
        exit
        """
    batchStop = b"""\
    C:
    cd C:\Program Files\Sisense\Prism
    psm ecube stop name="ElasticubeToImport" serverAddress="localhost"
    exit
    """

    batchInfo = b"""\
        C:
        cd C:\Program Files\Sisense\Prism
        psm ecube info name="ElasticubeToImport"
        exit
        """

    batchExport = b"""\
        C:
        cd C:\Program Files\Sisense\Prism
        psm ecube export name="ElasticubeToImport" force=true path="F:\python\ElasticubeToImport
        exit
        """

    batchRemoteAttach = b"""\
        C:
        cd C:\PSTools
        psexec -i -s -d \\\\ip -u username -p password "C:\Python\python.exe" "C:\Python\remote_import.py" "ElasticubeToImport"
        exit
        """

    print('get cube info for path')
    ECubeBytes = bytes(ECube, 'utf-8')
    batchInfo = batchInfo.replace(rb'ElasticubeToImport', ECubeBytes)
    # print(batchInfo)
    cmdInfo.stdin.write(batchInfo)
    cmdInfo.stdin.flush()
    result = cmdInfo.stdout.readlines()
    # print(result)

    for line in result:
        if 'DBFarmDirectory'.encode() in line.rstrip():
            string = line.decode("utf-8")
            removestring = string.replace("DBFarmDirectory: ", "")
            finalstring = removestring.strip()

            if os.path.exists(check_file):
                os.remove(check_file)
            else:
                print('File does not exists')

            print('stop cube')
            ECubeBytes = bytes(ECube, 'utf-8')
            batchStop = batchStop.replace(rb'ElasticubeToImport', ECubeBytes)
            # print(batchExport)
            cmdStop.stdin.write(batchStop)
            cmdStop.stdin.flush()

            print('sleep 10 seconds')

            print('start export')
            ECubeBytes = bytes(ECube, 'utf-8')
            batchExport = batchExport.replace(rb'ElasticubeToImport', ECubeBytes)
            # print(batchExport)
            cmdExport.stdin.write(batchExport)
            cmdExport.stdin.flush()

            print('sleep 10 seconds')
            time.sleep(10)

            while not os.path.exists(check_file):
                print("waiting for file")
                time.sleep(1)

            if os.path.isfile(check_file):
                print('File created: start copying cube to production')
                batchCopy = batchCopy.replace(rb'ElasticubeToImport', bytes(check_file, 'utf-8'))
                # print(batchCopy)
                cmdCopy.stdin.write(batchCopy)
                cmdCopy.stdin.flush()
                result = cmdCopy.stdout.readlines()
            else:
                raise ValueError("%s isn't a file!" % check_file)

            print('start importing cube in production')
            ECubeBytesAttach = bytes(ECube, 'utf-8')
            batchRemoteAttach = batchRemoteAttach.replace(rb'ElasticubeToImport', ECubeBytesAttach)
            #print(batchRemoteAttach)
            cmdRemoteAttach.stdin.write(batchRemoteAttach)
            time.sleep(5)
            cmdRemoteAttach.stdin.flush()

            time.sleep(5)
            exit()

def prepareACSFilesEC(soort, destination_path, jaar):
    for filename in glob.glob(os.path.join(destination_path, jaar + '*.txt')):

        connection = connect(jaar)
        cursor = connection.cursor()
        postgreSQL_select_Query = \
            'select * from ubuntulog where filename = %s'
        result = cursor.execute(postgreSQL_select_Query, (filename,))

        if cursor.fetchone() is not None:

            mobile_records_one = cursor.fetchone()

        else:

            print('Insert record in db  ' + filename)

            postgres_insert_query = \
                """ INSERT INTO ubuntulog (filename, status,type,datetime) VALUES (%s,%s,%s,now())"""
            record_to_insert = (filename, 'Processing', soort)
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            connection.close()

    return 'done'


def prepareFilesEC(soort, destination_path, jaar):
	
    for filename in glob.glob(os.path.join(destination_path, jaar + '*.txt')):

        connection = connect(jaar)
        cursor = connection.cursor()
        postgreSQL_select_Query = \
            'select * from ubuntulog where filename = %s'
        result = cursor.execute(postgreSQL_select_Query, (filename,))

        if cursor.fetchone() is not None:

            mobile_records_one = cursor.fetchone()

        else:

            print('Insert record in db  ' + filename)

            postgres_insert_query = \
                """ INSERT INTO ubuntulog (filename, status,type,datetime) VALUES (%s,%s,%s,now())"""
            record_to_insert = (filename, 'Processing', soort)
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
            connection.close()
            end_path = "F:\\python\\processing\\" + soort + "\\"
            file = os.path.basename(filename)
            shutil.move(filename, end_path + file)

    return 'done'


def moveFilesEC(soort, destination_path, folder_path_processed, jaar):
    for filename in glob.glob(os.path.join(destination_path, jaar + '*.txt')):
        connection = connect(jaar)
        cursor = connection.cursor()

        file = os.path.basename(filename)
        sql_update_query = \
            """Update ubuntulog set status = %s , end_datetime=now() where filename like %s and type=%s"""
        cursor.execute(sql_update_query, ('Done', '%' + file + '%', soort))
        print(sql_update_query)
        connection.commit()

        connection.close()

        shutil.move(filename, folder_path_processed + file)
    return 'done'

def cleanDBFarm():

    cmdline = ["cmd", "/q", "/k", "echo off"]
    cmdInfo = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    batchInfo = b"""\
    C:
    cd C:\Scripts\DBCleaner
    elasticube_data_cleaner.exe
    exit
    """


    print('clean dbfarm')
    # print(batchInfo)
    cmdInfo.stdin.write(batchInfo)
    cmdInfo.stdin.flush()
    result = cmdInfo.stdout.readlines()
