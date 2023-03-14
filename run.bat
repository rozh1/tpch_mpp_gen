echo off
rem gendb.py csv 10 2 6 4 3
rem     csv - тип хранилища [csv, bin, bin_compressed]
rem     10 - DB size in GB
rem     2 - split count for node
rem     6 - node count [N]
rem     4 - node number [1,N]
rem     3 - working thread count

gendb\gendb.exe bin_compressed 10 1 1 1 4