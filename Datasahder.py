import os
import time
import glob
import datashader as ds
import pandas as pd
from colorcet import fire
from datashader import transfer_functions as tf
from datashader import utils

os.chdir("//sbs2003/Daten-CME/")

t1 = time.time()

data = pd.DataFrame()
# for file in glob.glob("*2013-10*.parquet"): # Depending ov the file this line can be activated or deactivated
for file in glob.glob("*2009-06.csv"):
    df = pd.read_csv(
        file,
        usecols=[
            "End_Lon",
            "End_Lat"
        ]
    )
    print(df)
    print("Loaded {}".format(file))
    data = data.append(df)
    del df
    print("{} added to dataframe".format(file))
print("loading done")

canvas = ds.Canvas(x_range=(-74.25, -73.7),
                   y_range=(40.5, 41),
                   plot_width=4000,
                   plot_height=4000)

agg = canvas.points(data,
                    'End_Lon',
                    'End_Lat')

pic = tf.set_background(
    tf.shade(
        agg,
        min_alpha=200,
        cmap=fire
    ),
    "black"
)

utils.export_image(pic,
                   "Onefile END",
                   fmt=".png"
                   )

print("Image saved!")
print(
    "Time required:",
    time.time() - t1)
