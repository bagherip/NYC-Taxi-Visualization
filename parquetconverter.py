import glob, os
import time
import dask.dataframe as dd
import numpy
import pandas as pd
import pyarrow as pa
import numpy as np
import pyarrow.parquet as pq

path = "//sbs2003/Daten-CME/"
os.chdir(path)


### Converting with pyarrow engine
def parquet_converter(file):
    chunksize = 500000
    i = 0
    data = pd.DataFrame()  # creates a new dataframe that's empty
    for chunk in pd.read_csv(file, chunksize=chunksize, usecols=["End_Lon", "End_Lat"],
                             dtype={"End_Lon": np.float32, "End_Lat": np.float32},
                             delimiter=' *, *', engine="python"):
        # chunk = chunk.rename(columns={"dropoff_latitude": "End_Lat", "dropoff_longitude": "End_Lon"})
        table = pa.Table.from_pandas(chunk)
        # for the first chunk of records
        if i == 0:
            # create a parquet write object giving it an output file
            pqwriter = pq.ParquetWriter(target, table.schema, compression='snappy')
            pqwriter.write_table(table)
        # subsequent chunks can be written to the same file
        else:
            pqwriter.write_table(table)
        i += 1
    # close the parquet writer
    if pqwriter:
        pqwriter.close()


### Get all files in directory ###
for file in glob.glob("*.csv"):
    t1 = time.time()
    if not os.path.exists(file + ".parquet"):  # checks if the file is already converted
        target = path + file + ".parquet"
        print(f"In process: {file} ...")
        parquet_converter(file)
        print(f"{file} got converted to parquet \n")
        print("Time needed:" + str(time.time() - t1))
    else:
        print(file + "skipped available files")

### Converting with pandas
for file in glob.glob("*.csv"):
    t1 = time.time()
    if not os.path.exists(file + ".parquet"):  # checks if hte file is already converted
        df = pd.read_csv(file, usecols=["End_Lat", "End_Lon"],
                         dtype={"End_Lat": np.float32, "End_Lon": np.float32},
                         delimiter=' *, *', engine="python")
        chunk = chunk.rename(columns={"dropoff_latitude": "End_Lat", "dropoff_longitude": "End_Lon"})
        print("opened " + file)
        df.to_parquet((file + ".parquet"))
        print("converted {} in {} seconds".format(file, time.time() - t1))
        del df

### Converting with Dask
for file in glob.glob("*.csv"):
    t1 = time.time()
    dask_csv = dd.read_csv(file)
    print("opened " + file)

    dask_csv.to_parquet((file + ".parquet"))
    print("converted {} in {} seconds".format(file, time.time() - t1))
    print(dask_csv)
