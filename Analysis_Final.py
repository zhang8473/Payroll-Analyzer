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
import sys,re,csv,os,math,time
try:
    from geopy.geocoders import Nominatim
    geolocator = Nominatim()
except:
    pass

bins=[0,10000,20000,50000,100000,150000,200000,float('inf')]

locations={}
if os.path.isfile('location_lon_lat.csv'):
    with open('location_lon_lat.csv', 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for line in reader:
            if len(line)==3:
                if line[1]==0:
                    locations[line[0]]=(None,None)
                else:
                    locations[line[0]]=(line[1],line[2])
        csvfile.close()

Fragments_Cells=None
def parse(line):
    global Fragments_Cells

    #some cells have return keys inside, the line will be broken. csv reader cannot handle this case
    if Fragments_Cells!=None:
        line=Fragments_Cells+line
        Fragments_Cells=None
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

def Statistics(line):
    data=[ i for i in line[1] ]
    counts=len(data)
    histo=np.histogram(data, bins=bins)[0]
    histo_normalized=[ x/float(counts)*100 for x in histo ]
    histo_barstd=[ math.sqrt(x)/float(counts)*100 if x>0 else 0 for x in histo ]
    #year,(
    #     0. normalized histogram
    #     1. standard deviation of each bar
    #     2. mean of all values
    #     3. standard deviation of all values
    #     4. total counts)
    return (line[0], (histo_normalized,histo_barstd,np.mean(data),np.std(data),counts) )

def histo_wage(rdd):
    colors=['b','y','g','c','r','m']
    idx=np.arange(len(bins)-1)
    handlers=[]
    years_filled=[]
    mean=[]
    std=[]
    plt.close('all')
    fig1, ax = plt.subplots()
    results=rdd.groupByKey().map(Statistics).collect()
    binwidth=0.8/len(results)
    for i, res in enumerate(results):
        if res[1][-1]>0:
            mean.append(res[1][2])
            std.append(res[1][3])
            handlers.append( ax.bar(idx+(binwidth+0.02)*i, res[1][0], binwidth, color=colors[i], yerr=res[1][1], ecolor='k', edgecolor = "none") )
            years_filled.append(res[0])
    ax.set_ylabel('percents of records per bin (%)', fontsize=20)
    ax.set_xlabel('Pay & Benefits (Total Income)', fontsize=20)
    ax.set_yscale("log", nonposy='clip')
    ax.set_xticklabels(([str(x/1000)+' k' for x in bins[:-1]]+['inf']))
    ax.legend((handlers),(years_filled))
    fig1.savefig('Histo_Wage.png')
    fig2, ax2 = plt.subplots()
    ax2.errorbar(years_filled, mean, xerr=0, yerr=std, fmt='o')
    ax2.set_ylabel("Average Pay & Benefits")
    ax2.set_xlabel("Years")
    plt.xticks(years_filled, [str(year) for year in years_filled], rotation='vertical')
    plt.xlim([np.amin(years_filled)-1,np.amax(years_filled)+1])
    fig2.savefig('Wage_vs_years.png')
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
        print>>sys.stdout, "\033[93m"+record[0]+" is not found.\033[0m"
        with open('location_lon_lat.csv', 'a') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow([record[0],0, 0])
            csvfile.close()
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
    # draw coastlines, state and country boundaries, edge of map.
    m.shadedrelief()
    x=[]
    y=[]
    z=[]

    for record in result:
        if record[0]!=None:
            xpt,ypt=m(record[0][1],record[0][0])
            x.append(int(xpt))
            y.append(int(ypt))
            z.append(record[1][0]/float(record[1][1])/1000)

    m.drawcoastlines()
    m.drawstates()
    m.drawcountries()
    binplot=m.hexbin(np.array(x), np.array(y), C = np.array(z), reduce_C_function = np.mean, gridsize=gridsize, mincnt=0, linewidths=0.5, edgecolors='k', zorder=10)
    # plot colorbar for markers.
    m.colorbar(binplot,pad='8%', label='Mean Total Income (k$)')
    plt.title(title, fontsize=20)
    binplot.set_clim(vmin=0, vmax=150)
    plt.savefig(title)
    return location_wage_stat.keys().count()

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext()

    #'Employee Name', u'Job Title', u'Base Pay', u'Overtime Pay', u'Other Pay',
    #'Benefits', u'Total Pay', u'Total Pay & Benefits', u'Year', u'Notes', u'Agency', u'Status'
    #raw = sc.textFile("2010-payroll.csv") #test file
    raw = sc.textFile("201[1-4]-payroll.csv")
    payrolls=raw.map(parse).filter(lambda x:x!=None)
    print>>sys.stdout, "\n\033[92m"+str(payrolls.count())+" payroll records are found.\033[0m"

    start_time=time.time()
    print>>sys.stdout, histo_wage( payrolls.map(lambda x:(x[8],x[7])) )
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #52.9s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2014),\
                                                      '2014 California Average Income',\
                                                      width=1000000,height=1200000,lon_0=-119,lat_0=37.20, gridsize=1000))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #106s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2013),\
                                                      '2013 California Average Income',\
                                                      width=1000000,height=1200000,lon_0=-119,lat_0=37.20, gridsize=1000))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #112.6s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2012),\
                                                      '2012 California Average Income',\
                                                      width=1000000,height=1200000,lon_0=-119,lat_0=37.20, gridsize=1000))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #116.7s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2011),\
                                                      '2011 California Average Income',\
                                                      width=1000000,height=1200000,lon_0=-119,lat_0=37.20, gridsize=1000))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #103s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2014),\
                                                      '2014 Bay Area Average Income',\
                                                      width=120000,height=120000,lon_0=-122.16,lat_0=37.72, gridsize=1600))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #103s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2013),\
                                                      '2013 Bay Area Average Income',\
                                                      width=120000,height=120000,lon_0=-122.16,lat_0=37.72, gridsize=1600))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #99.4s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2012),\
                                                      '2012 Bay Area Average Income',\
                                                      width=120000,height=120000,lon_0=-122.16,lat_0=37.72, gridsize=1600))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #97.7s
    start_time=time.time()
    print>>sys.stdout, "\n\033[92m"+str(location_wage(payrolls.filter(lambda x:x[8]==2011),\
                                                      '2011 Bay Area Average Income',\
                                                      width=120000,height=120000,lon_0=-122.16,lat_0=37.72, gridsize=2500))+" locations found.\033[0m"
    print>>sys.stdout, "\n\033[92mIt takes %.1fs.\033[0m"%(time.time()-start_time) #97.5s
