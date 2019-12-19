import datetime
import glob
import os
import time
from multiprocessing import Pool

import datashader as ds
from colorcet import *
from dask import dataframe as dd
from datashader import transfer_functions as tf
from datashader import utils

os.chdir("//sbs2003/Daten-CME/")

t1 = time.time()

def data_pool(file):
    df = dd.read_parquet(file)
    print(file + " loaded")
    return df

data = None

if __name__ == '__main__':
    print(datetime.datetime.now())
    t1 = time.time()
    files = glob.iglob('*.csv_2_.parquet')
    p = Pool(os.cpu_count())
    data = dd.concat(p.map(data_pool, files)) # reset_index(drop=True))
    canvas = ds.Canvas(x_range=(-74.25, -73.7), y_range=(40.5, 41),plot_width=8000, plot_height=8000)
    agg = canvas.points(data, 'End_Lon', 'End_Lat')
    pic = tf.set_background(tf.shade(agg, cmap=reversed(blues)), color="#364564")#364564
    utils.export_image(pic, "NYCPlot fn1", fmt=".png")
    print("time needed", time.time() - t1)
