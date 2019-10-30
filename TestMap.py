from mpl_toolkits.basemap import Basemap, cm
import numpy as np
import matplotlib.pyplot as plt
from numpy import max

# # create figure and axes instances
fig = plt.figure(figsize=(12,12))
# create polar stereographic Basemap instance.
# width and height are in meters
m = Basemap(width=1000000,height=1200000,projection='lcc',\
            resolution='i',lon_0=-119,lat_0=37.20)

lon, lat = -119.7956341,36.0784807 # Location of Boulder
xpt,ypt = m(lon,lat)
x=[int(xpt),10111]
y=[int(ypt),10111]
z=[1000.4,2000.05]
#m.drawlsmask(land_color='green',ocean_color='aqua',lakes=True)
# draw coastlines, state and country boundaries, edge of map.
#m.bluemarble()
#m.shadedrelief()
#m.plot(xpt,ypt,'bo')  # plot a blue dot there
# x = linspace(0, map.urcrnrx, data.shape[1])
# y = linspace(0, map.urcrnry, data.shape[0])
#
# xx, yy = meshgrid(x, y)

m.pcolormesh(x, y, z)
#m.hexbin(np.array(x), np.array(y), C = np.array(z), reduce_C_function = max, gridsize=1, mincnt=0, cmap='YlOrBr', linewidths=0.5, edgecolors='k', zorder=10)
#m.colorbar(location='bottom', label='Mean amplitude (kA)')
#m.etopo()
m.drawcoastlines()
m.drawstates()
m.drawcountries()
plt.show()
