import os
import os.path
import glob
import psycopg2
import shutil
import sys
import fnmatch
import time
import subprocess

EC=sys.argv[1]

cmdline = ["cmd", "/q", "/k", "echo off"]
cmdRemoteAttach = subprocess.Popen(cmdline, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
batchRemoteAttach = b"""\
        C:
        cd C:\Program Files\Sisense\Prism
        psm ecube import path="F:\SiSense\PrismServer\ElastiCubeData\ElasticubeToImport.ecdata"
        exit
        """
print('start importing cube in production')
ECubeBytesAttach = bytes(EC, 'utf-8')
batchRemoteAttach = batchRemoteAttach.replace(rb'ElasticubeToImport', ECubeBytesAttach)
print(batchRemoteAttach)
cmdRemoteAttach.stdin.write(batchRemoteAttach)
time.sleep(5)
cmdRemoteAttach.stdin.flush()