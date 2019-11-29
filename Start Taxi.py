# import pandas as pd
# import glob, os
# import matplotlib.pyplot as plt
# import matplotlib.colors as mcolors
# import matplotlib.patches as mpatches
# from matplotlib.patches import Shadow
#
# os.chdir("C:/Users/aba/PycharmProjects/Taxi/Uber")
#
# newDF = pd.DataFrame() #creates a new dataframe that's empty
#
# for file in glob.glob("*.csv"):
#     df = pd.read_csv(file, sep=',')
#     newDF=newDF.append(df)
# print(newDF)
#
#
#
# ax = plt.axes()
# ax.set_aspect('auto')
# size=0.08
# quality=1000
# plt.style.use('dark_background')
# # plt.figure()
# plt.xlim(-74.3,-73.6)
# plt.ylim(40.5,41.0)
# plt.axis("off")
#
# i=15000
# a=0
# colors=["#481860","#ff6666","#ffcc99","#ffffcc","#ffffff"]
# # for x in colors:
# #     i=i-4000
# #     a=a+0.001
# #     plt.scatter(newDF["Lon"][::3000], newDF["Lat"][::3000], marker=".", s=i, c=x, alpha=0.006 + a)
#
# plt.scatter(newDF["Lon"], newDF["Lat"], marker=".", s=size, c="#e9f56b", linewidths=0.008, edgecolors="#e97f1c")
# plt.tight_layout()
#
# plt.savefig('C:/Users/aba/PycharmProjects/Taxi/Result.png',dpi=quality) #'+str(size)+'_'+str(quality)+'.png', )
# # plt.show()
#

# libraries
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import kde
import csv
import pandas as pd
import glob, os
import time

start=time.time()
os.chdir("//sbs2003/Daten-CME/")

data = pd.DataFrame()

for file in glob.glob("*_1_.parquet"):
    try:
        print(file)
        df = pd.read_parquet(file, columns=["End_Lon", "End_Lat"])
        print("Loaded {}".format(file))
        data=data.append(df,ignore_index=True)
        del df
    except KeyError:
        pass
    print("{} added to dataframe".format(file))
print(data)

# create data for mesh
y= data['End_Lat'].sample(1000).to_numpy()
x= data['End_Lon'].sample(1000).to_numpy()

# Evaluate a gaussian kde on a regular grid of nbins x nbins over data extents
nbins = 700
k = kde.gaussian_kde([x, y])
xi, yi = np.mgrid[x.min():x.max():nbins * 1j, y.min():y.max():nbins * 1j]
zi = k(np.vstack([xi.flatten(), yi.flatten()]))

# Parameters and Settings
size=0.002
quality=2000
a=0.1
plt.style.use('dark_background')
plt.xlim(-74.25,-73.7)
plt.ylim(40.5,41.0)
plt.axis("off")
ax = plt.axes()
ax.set_aspect('equal')
plt.tight_layout()

# Make the plot
plt.pcolormesh(xi, yi, zi.reshape(xi.shape),cmap="inferno",alpha=a)
plt.scatter(data['End_Lon'],data['End_Lat'], marker=".", s=size, c="#ffcc66", linewidths=0, edgecolors="#e97f1c")
plt.savefig('C:/Users/aba/PycharmProjects/Taxi/Results/Result_EndLocs.png',dpi=quality)

dauer=time.time()-start

if dauer>60:
    print(dauer/60,"minute")
else:
    print(dauer,"second")

