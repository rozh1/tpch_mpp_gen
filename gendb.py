import multiprocessing
import subprocess

import tbl_to_csv
import tbl_to_bin
import os
import sys

DBGEN_DIST_PATH = "tpch3_dist"
DBGEN_PATH = os.path.join(DBGEN_DIST_PATH, "dbgen.exe")
HELP_MSG = '''
Usage: gendb.py csv 10 2 6 4 3
    csv - тип хранилища [csv, bin, bin_compressed]
    10 - DB size in GB
    2 - split count for node
    6 - node count [N]
    4 - node number [1,N]
    3 - working thread count
    '''


def GenTpchDb(size, count, nodes=1, node=1) -> bool:
    processes = []
    start_num = (node-1) * count + 1
    end_num = node * count + 1
    for i in range(start_num, end_num):
        process = subprocess.Popen(
            [DBGEN_PATH, "-s", str(size), "-C",
             str(count*nodes), "-S", str(i), "-f"],
            cwd=DBGEN_DIST_PATH)
        processes.append(process)

    result = True
    for process in processes:
        if (process.wait() != 0):
            result = False

    return result


if __name__ == "__main__":
    if sys.platform.startswith('win'):
        # On Windows calling this function is necessary.
        multiprocessing.freeze_support()

    if len(sys.argv) <= 5:
        print(HELP_MSG)
        os._exit(0)

    mode = sys.argv[1]
    size = int(sys.argv[2]) if len(sys.argv) > 4 else 1
    split_count = int(sys.argv[3]) if len(sys.argv) > 4 else 4
    nodes_count = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    node_number = int(sys.argv[5]) if len(sys.argv) > 4 else 1
    thread_count = int(sys.argv[6]) if len(sys.argv) > 5 else 2

    print(
        f'Starting generating DB [{mode}] with size={size} GB, split={split_count}, nodes={nodes_count}, node={node_number}, threads={thread_count}')

    if (GenTpchDb(size, split_count, nodes_count, node_number)):
        match mode:
            case 'csv':
                converter = tbl_to_csv.TblToCsvConverter(
                    "tpch3_dist", r"tpch_data\csv", thread_count)
                converter.Run()
            case 'bin':
                converter = tbl_to_bin.TblToBinConverter(
                    "tpch3_dist", r"tpch_data\bin", thread_count)
                converter.Run()
            case 'bin_compressed':
                converter = tbl_to_bin.TblToBinConverter(
                    "tpch3_dist", r"tpch_data\bin_compressed", thread_count, True)
                converter.Run()

        files = os.listdir(DBGEN_DIST_PATH)
        for item in files:
            if item.find(".tbl") > 0:
                os.remove(os.path.join(DBGEN_DIST_PATH, item))
