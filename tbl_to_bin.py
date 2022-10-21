import os
import sys
from multiprocessing import Queue
from multiprocessing import Process
import time
import subprocess


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
                "name": "customer",
                "csv": "C_CUSTKEY:long,C_NAME:string,C_ADDRESS:string,C_NATIONKEY:long,C_PHONE:string,C_ACCTBAL:float,C_MKTSEGMENT:string,C_COMMENT:string",
                "json": """{
  "Columns": [
    "C_CUSTKEY",
    "C_NAME",
    "C_ADDRESS",
    "C_NATIONKEY",
    "C_PHONE",
    "C_ACCTBAL",
    "C_MKTSEGMENT",
    "C_COMMENT"
  ],
  "Types": [ LONG, STRING, STRING, LONG, STRING, FLOAT, STRING, STRING  ]
}"""
            },
            "lineitem.tbl": {
                "name": "lineitem",
                "csv": "L_ORDERKEY:long,L_PARTKEY:long,L_SUPPKEY:long,L_LINENUMBER:int,L_QUANTITY:float,L_EXTENDEDPRICE:float,L_DISCOUNT:float,L_TAX:float,L_RETURNFLAG:string,L_LINESTATUS:string,L_SHIPDATE:date,L_COMMITDATE:date,L_RECEIPTDATE:date,L_SHIPINSTRUCT:string,L_SHIPMODE:string,L_COMMENT:string",
                "json": """{
  "Columns": [
    "L_ORDERKEY",
    "L_PARTKEY",
    "L_SUPPKEY",
    "L_LINENUMBER",
    "L_QUANTITY",
    "L_EXTENDEDPRICE",
    "L_DISCOUNT",
    "L_TAX",
    "L_RETURNFLAG",
    "L_LINESTATUS",
    "L_SHIPDATE",
    "L_COMMITDATE",
    "L_RECEIPTDATE",
    "L_SHIPINSTRUCT",
    "L_SHIPMODE",
    "L_COMMENT"
  ],
  "Types": [ LONG, LONG, LONG, INTEGER, FLOAT, FLOAT, FLOAT, FLOAT, STRING, STRING, DATE, DATE, DATE, STRING, STRING, STRING   ]
}"""
            },
            "nation.tbl": {
                "name": "nation",
                "csv": "N_NATIONKEY:long,N_NAME:string,N_REGIONKEY:long,N_COMMENT:string",
                "json": """{
  "Columns": [
    "N_NATIONKEY",
    "N_NAME",
    "N_REGIONKEY",
    "N_COMMENT"
  ],
  "Types": [ LONG, STRING, LONG, STRING ]
}
                """
            },
            "orders.tbl": {
                "name": "orders",
                "csv": "O_ORDERKEY:long,O_CUSTKEY:long,O_ORDERSTATUS:string,O_TOTALPRICE:float,O_ORDERDATE:date,O_ORDERPRIORITY:string,O_CLERK:string,O_SHIPPRIORITY:int,O_COMMENT:string",
                "json": """{
  "Columns": [
    "O_ORDERKEY",
    "O_CUSTKEY",
    "O_ORDERSTATUS",
    "O_TOTALPRICE",
    "O_ORDERDATE",
    "O_ORDERPRIORITY",
    "O_CLERK",
    "O_SHIPPRIORITY",
    "O_COMMENT"
  ],
  "Types": [ LONG, LONG, STRING, FLOAT, DATE, STRING, STRING, INTEGER, STRING ]
}
                """
            },
            "part.tbl": {
                "name": "part",
                "csv": "P_PARTKEY:long,P_NAME:string,P_MFGR:string,P_BRAND:string,P_TYPE:string,P_SIZE:int,P_CONTAINER:string,P_RETAILPRICE:float,P_COMMENT:string",
                "json": """{
  "Columns": [
    "P_PARTKEY",
    "P_NAME",
    "P_MFGR",
    "P_BRAND",
    "P_TYPE",
    "P_SIZE",
    "P_CONTAINER",
    "P_RETAILPRICE",
    "P_COMMENT"
  ],
  "Types": [ LONG, STRING, STRING, STRING, STRING, INTEGER, STRING, FLOAT, STRING ]
}
                """
            },
            "partsupp.tbl": {
                "name": "partsupp",
                "csv": "PS_PARTKEY:long,PS_SUPPKEY:long,PS_AVAILQTY:int,PS_SUPPLYCOST:float,PS_COMMENT:string",
                "json": """{
  "Columns": [
    "PS_PARTKEY",
    "PS_SUPPKEY",
    "PS_AVAILQTY",
    "PS_SUPPLYCOST",
    "PS_COMMENT"
  ],
  "Types": [ LONG, LONG, INTEGER, FLOAT, STRING ]
}
                """
            },
            "region.tbl": {
                "name": "region",
                "csv": "R_REGIONKEY:long,R_NAME:string,R_COMMENT:string",
                "json": """{
  "Columns": [
    "R_REGIONKEY",
    "R_NAME",
    "R_COMMENT"
  ],
  "Types": [ LONG, STRING, STRING ]
}
                """
            },
            "supplier.tbl": {
                "name": "supplier",
                "csv": "S_SUPPKEY:long,S_NAME:string,S_ADDRESS:string,S_NATIONKEY:long,S_PHONE:string,S_ACCTBAL:float,S_COMMENT:string",
                "json": """{
  "Columns": [
    "S_SUPPKEY",
    "S_NAME",
    "S_ADDRESS",
    "S_NATIONKEY",
    "S_PHONE",
    "S_ACCTBAL",
    "S_COMMENT"
  ],
  "Types": [ LONG, STRING, STRING, INTEGER, STRING, FLOAT ,STRING ]
}
                """
            },
        }

    def transform_file(self, input_file_path: str, ouput_file_path: str, csv: str, json: str):
        with open(ouput_file_path + ".csv", "w") as f:
            f.write(csv)

        with open(ouput_file_path + "_meta.json", "w") as f:
            f.write(json)

        process = subprocess.Popen(
            ["java", "-jar", "JBinaryTransformer.jar", input_file_path, ouput_file_path])
        process.wait()
        pass

    def transform_file_thread(self, input_file_path: str, ouput_file_path: str, csv: str, json: str):
        self.transform_file(input_file_path, ouput_file_path, csv, json)
        task = self.q.get()
        print(task + " done")

    def transform_file_async(self, input_file_path: str, ouput_file_path: str, csv: str, json: str):
        self.q.put(input_file_path)
        t = Process(target=self.transform_file_thread, args=[
            input_file_path, ouput_file_path, csv, json])
        t.start()

    def get_split_keys(self, input_dir: str) -> list:
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

        for key, value in self.file_list.items():
            csv = value["csv"]
            json = value["json"]
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
                            self.transform_file(
                                temp_file, split_output_temp_dir, csv, json)

                self.transform_file_async(
                    input_file_path, ouput_file_path, csv, json)

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
