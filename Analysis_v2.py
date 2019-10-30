######################################
#   Spark Payroll Analysis           #
#   Author: Jinzhong Zhang, Xu Lian  #
#   Mar, 2016                        #
######################################

# Plot colors
# b: blue
# g: green
# r: red
# c: cyan
# m: magenta
# y: yellow
# k: black
# w: white

from pyspark import SparkContext, SparkConf
from StringIO import StringIO
import matplotlib.pyplot as plt
import numpy as np
import sys,re,csv,os,math
from geopy.geocoders import Nominatim
geolocator = Nominatim()

locations={}
if os.path.isfile('location_lon_lat.csv'):
    with open('location_lon_lat.csv', 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for line in reader:
            if len(line)==3:
                locations[line[0]]=(line[1],line[2])
        csvfile.close()

Fragments_Cells=None
def parse(line):
    global Fragments_Cells

    #some cells have return keys inside, the line will be broken. csv reader cannot handle this case
    if Fragments_Cells!=None:
        line=Fragments_Cells+line
        Fragments_Cells=None
        #print>>sys.stdout, line
    elif len(line.split(','))<11:
        Fragments_Cells=line#[:-1]
        return None

    try:
        #line=line.encode('utf-8').decode('utf-8','ignore').encode("utf-8") #This code might be safer
        line=line.encode('utf-8','ignore')
        x=csv.reader(StringIO(line),strict=True, skipinitialspace = True, quoting = csv.QUOTE_ALL, quotechar = '"', delimiter = ',', lineterminator = '\n').next()
    except Exception as e:
        e.args += (line.split(','),)
        raise

    if x[0]=='Employee Name':# remove the title tow
        return None
    else:
        for i,xi in enumerate(x):
            if re.match('^[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?$', xi) is None: #if the cell is not a number
                if xi=="Not Provided":#if the cell is empty
                    x[i]=None
            else:
                if '.' in xi:#if the cell is an float
                    x[i]=float(xi)
                else:#if the cell is an integer
                    x[i]=int(xi)
    return x

#'Employee Name', u'Job Title', u'Base Pay', u'Overtime Pay', u'Other Pay',
#'Benefits', u'Total Pay', u'Total Pay & Benefits', u'Year', u'Notes', u'Agency', u'Status'
def histo_wage(rdd,years):
    binwidth=0.8/len(years)
    bins=[0,10000,20000,50000,100000,150000,200000,float('inf')]
    colors=['b','y','g','c','r','m']
    idx=np.arange(len(bins)-1)
    fig, ax = plt.subplots()
    handlers=[]
    years_filled=[]
    for i, year in enumerate(years):
        data=rdd.lookup(year)
        n=np.histogram(data, bins=bins)[0]
        counts=len(data)
        if counts>0:
            n_fills=[ x/float(counts)*100 for x in n ] #normalize to percentage
            n_Std=[ math.sqrt(x)/float(counts)*100 if x>0 else 0 for x in n ]
            handlers.append( ax.bar(idx+(binwidth+0.02)*i, n_fills, binwidth, color=colors[i], yerr=n_Std, ecolor='k', edgecolor = "none") )
            years_filled.append(year)
    ax.set_ylabel('percents of records per bin (%)', fontsize=20)
    ax.set_xlabel('Total Pay & Benefits', fontsize=20)
    ax.set_yscale("log", nonposy='clip')
    ax.set_xticklabels(([str(x/1000)+' k' for x in bins[:-1]]+['inf']))
    ax.legend((handlers),(years_filled))
    #plt.show()
    plt.savefig('Histo_Wage.png')
    return years_filled

def convert_name_to_location(record):
    if record[0] in locations:
        return ( locations[record[0]],record[1] )
    try:
        location = geolocator.geocode(record[0]+', California')
    except:
        print>>sys.stdout, "\033[91m"+record[0]+". Service request declined.\033[0m"
        return (None,None)
    if location==None:
        print>>sys.stdout, "\033[91m"+record[0]+" is not found.\033[0m"
        return (None,None)
    else:
        print>>sys.stdout, "\n\033[92m"+location.address.encode('utf-8','ignore')+": "+str((location.latitude, location.longitude))+". >>"+'location_lon_lat.csv'+"\033[0m"
        with open('location_lon_lat.csv', 'a') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow([record[0],location.latitude, location.longitude])
            csvfile.close()
        return ( (location.latitude, location.longitude),record[1] )

def location_wage(rdd,title,lon_0,lat_0,width,height,gridsize):
    location_wage_yr=rdd.map( lambda x:(x[10],(x[7],1,x[7])) )
    #[0]: key(location); [1][0]: sumover all records; [1][1]: number of records; [1][2]: max value

    location_wage_stat=location_wage_yr.reduceByKey( lambda x,y:(x[0]+y[0],x[1]+y[1],max(x[2],y[2])) )
    location_wage_stat=location_wage_stat.map(convert_name_to_location)

    result=location_wage_stat.collect()

    from mpl_toolkits.basemap import Basemap

    # # create figure and axes instances
    fig = plt.figure(figsize=(14,14))
    # create polar stereographic Basemap instance.
    # width and height are in meters
    m = Basemap(width=width,height=height,projection='lcc',\
                resolution='f',lon_0=lon_0,lat_0=lat_0)
    #m.drawlsmask(land_color='green',ocean_color='aqua',lakes=True)
    # draw coastlines, state and country boundaries, edge of map.
    #m.bluemarble()
    m.shadedrelief()
    #m.etopo()
    x=[]
    y=[]
    z=[]

    for record in result:
        if record[0]!=None:
            xpt,ypt=m(record[0][1],record[0][0])
            # print>>sys.stdout, record
            # print>>sys.stdout, xpt
            # print>>sys.stdout, ypt
            x.append(int(xpt))
            y.append(int(ypt))
            z.append(record[1][0]/float(record[1][1])/1000)

    m.drawcoastlines()
    m.drawstates()
    m.drawcountries()
    binplot=m.hexbin(np.array(x), np.array(y), C = np.array(z), reduce_C_function = np.mean, gridsize=gridsize, mincnt=0, linewidths=0.5, edgecolors='k', zorder=10)
    #m.scatter(x,y,40,z,cmap=plt.cm.jet,marker='o',edgecolors='none',alpha=.5, zorder=10)
    # plot colorbar for markers.
    m.colorbar(binplot,pad='8%', label='Mean Total Income (k$)')
    plt.title(title, fontsize=20)
    binplot.set_clim(vmin=0, vmax=150)
    #plt.show()
    #print>>sys.stdout, location_wage_stat.take(10)
    plt.savefig(title)
    return location_wage_stat.keys().count()

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext()

    raw = sc.textFile("2012-payroll.csv") #test file
    #raw = sc.textFile("*payroll.csv")
    payrolls=raw.map(parse).filter(lambda x:x!=None)
    print>>sys.stdout, "\n\033[92m"+str(payrolls.count())+" payroll records are found.\033[0m"
    #print>>sys.stdout, payrolls.take(12)
    #print>>sys.stdout, histo_wage( payrolls.map(lambda x:(x[8],x[7])), [2011,2012,2013,2014])
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls,\
                                                      '2012 California Average Income',\
                                                      width=1000000,height=1200000,lon_0=-119,lat_0=37.20, gridsize=250))+" locations found.\033[0m"
    # print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2011),\
    #                                                   '2011 Bay Area Average Income',\
    #                                                   width=120000,height=120000,lon_0=-122.16,lat_0=37.72, gridsize=150))+" locations found.\033[0m"
