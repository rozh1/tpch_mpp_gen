import os
import io
import sys
from multiprocessing import Queue
from multiprocessing import Process
import time
import tbl_to_csv_transformer

class TblToCsvConverter:
    def __init__(self, input_dir, output_dir, threads=4):
        self.data = []
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.q = Queue(threads)
        self.__init_data()

    def __init_data(self):
        self.file_list = {
            "customer.tbl": {
                "name": "customer.csv",
                "header": "C_CUSTKEY:long,C_NAME:string,C_ADDRESS:string,C_NATIONKEY:long,C_PHONE:string,C_ACCTBAL:float,C_MKTSEGMENT:string,C_COMMENT:string"
            },
            "lineitem.tbl": {
                "name": "lineitem.csv",
                "header": "L_ORDERKEY:long,L_PARTKEY:long,L_SUPPKEY:long,L_LINENUMBER:int,L_QUANTITY:float,L_EXTENDEDPRICE:float,L_DISCOUNT:float,L_TAX:float,L_RETURNFLAG:string,L_LINESTATUS:string,L_SHIPDATE:date,L_COMMITDATE:date,L_RECEIPTDATE:date,L_SHIPINSTRUCT:string,L_SHIPMODE:string,L_COMMENT:string"
            },
            "nation.tbl": {
                "name": "nation.csv",
                "header": "N_NATIONKEY:long,N_NAME:string,N_REGIONKEY:long,N_COMMENT:string"
            },
            "orders.tbl": {
                "name": "orders.csv",
                "header": "O_ORDERKEY:long,O_CUSTKEY:long,O_ORDERSTATUS:string,O_TOTALPRICE:float,O_ORDERDATE:date,O_ORDERPRIORITY:string,O_CLERK:string,O_SHIPPRIORITY:int,O_COMMENT:string"
            },
            "part.tbl": {
                "name": "part.csv",
                "header": "P_PARTKEY:long,P_NAME:string,P_MFGR:string,P_BRAND:string,P_TYPE:string,P_SIZE:int,P_CONTAINER:string,P_RETAILPRICE:float,P_COMMENT:string"
            },
            "partsupp.tbl": {
                "name": "partsupp.csv",
                "header": "PS_PARTKEY:long,PS_SUPPKEY:long,PS_AVAILQTY:int,PS_SUPPLYCOST:float,PS_COMMENT:string"
            },
            "region.tbl": {
                "name": "region.csv",
                "header": "R_REGIONKEY:long,R_NAME:string,R_COMMENT:string"
            },
            "supplier.tbl": {
                "name": "supplier.csv",
                "header": "S_SUPPKEY:long,S_NAME:string,S_ADDRESS:string,S_NATIONKEY:long,S_PHONE:string,S_ACCTBAL:float,S_COMMENT:string"
            },
        }

    def transform_file_thread(self, input_file_path: str, ouput_file_path: str, header: str):
        transformer = tbl_to_csv_transformer.TblToCsvTransformer()
        transformer.transform_file(input_file_path, ouput_file_path, header)
        task = self.q.get()
       # self.q.task_done()
        print(task + " done")

    def transform_file_async(self, input_file_path: str, ouput_file_path: str, header: str):
        self.q.put(input_file_path)
        t = Process(target=self.transform_file_thread, args=[
                   input_file_path, ouput_file_path, header])
        t.start()

    def get_split_keys(self, input_dir:str) -> list:
        l = []
        for key, value in self.file_list.items():
            for dir_file in os.listdir(input_dir):
                if (not dir_file.startswith(key)):
                    continue

                if (not dir_file.endswith(".tbl") and not dir_file.endswith(".tmp")):
                    start_ind = dir_file.find(".tbl")
                    split_key = dir_file[start_ind+5:len(dir_file)]
                    l.append(split_key) if split_key not in l else l
        return l

    def Run(self):
        if (not os.path.isdir(self.output_dir)):
            os.makedirs(self.output_dir)

        split_key_set = self.get_split_keys(self.input_dir)
        transformer = tbl_to_csv_transformer.TblToCsvTransformer()

        for key, value in self.file_list.items():
            header = value["header"]
            for dir_file in os.listdir(self.input_dir):
                if (not dir_file.startswith(key)):
                    continue

                if (not dir_file.endswith(".tbl")):
                    start_ind = dir_file.find(".tbl")
                    split_key = dir_file[start_ind+5:len(dir_file)]
                    input_file_path = os.path.join(self.input_dir, dir_file)
                    split_output_dir = os.path.join(self.output_dir, split_key)
                    ouput_file_path = os.path.join(
                        split_output_dir, value["name"])

                    if (not os.path.isdir(split_output_dir)):
                        os.mkdir(split_output_dir)
                else:
                    if (len(split_key_set) == 0):
                        input_file_path = os.path.join(self.input_dir, key)
                        ouput_file_path = os.path.join(
                            self.output_dir, value["name"])
                    else:
                        input_file_path = os.path.join(self.input_dir, key)
                        split_output_dir = os.path.join(
                            self.output_dir, split_key_set[0])
                        ouput_file_path = os.path.join(
                            split_output_dir, value["name"])
                        for split_key in split_key_set:
                            if (split_key == split_key_set[0]):
                                continue
                            temp_file = os.path.join(
                                self.input_dir, key+".tmp")
                            open(temp_file, 'a').close()
                            split_output_temp_dir = os.path.join(
                                self.output_dir, split_key, value["name"])
                            transformer.transform_file(
                                temp_file, split_output_temp_dir, header)

                self.transform_file_async(
                #transformer.transform_file(
                    input_file_path, ouput_file_path, header)

        while not self.q.empty():
            time.sleep(1)
            
        self.q.close()
        self.q.join_thread()


if __name__ == "__main__":
    input_dir = r"D:\Clusterix\tpch\1G\tbl_x4"
    output_dir = sys.argv[1] if len(
        sys.argv) > 1 else r"D:\Clusterix\tpch\1G\csv_x4"

    converter = TblToCsvConverter(input_dir, output_dir, 2)
    converter.Run()
