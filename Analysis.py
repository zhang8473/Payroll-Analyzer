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
import sys,re,csv,math
# reload(sys)
# sys.setdefaultencoding('utf-8')


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
#'Benefits', u'Total Pay', u'Total Pay & Benefits', u'Year', u'Notes', u'Agency'
def histo_wage(rdd,years):
    binwidth=0.8/len(years)
    bins=[0,10000,20000,50000,100000,150000,200000,float('inf')]
    colors=['r','b','y','g','c','m']
    idx=np.arange(len(bins)-1)
    fig, ax = plt.subplots()
    handlers=[]
    years_filled=[]
    for i, year in enumerate(years):
        year_wage=rdd.filter(lambda x:x[8]==year).map(lambda x:x[7])
        counts=year_wage.count()  #total number of records
        n=year_wage.histogram(bins)[1]
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

def location_wage(rdd,year):
    location_wage_yr=rdd.filter(lambda x:x[8]==year)

if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext()

    raw = sc.textFile("20*-payroll.csv") #test file
    #raw = sc.textFile("*payroll.csv")
    payrolls=raw.map(parse).filter(lambda x:x!=None)
    print>>sys.stdout, "\n\033[92m"+str(payrolls.count())+" payroll records are found\033[0m"
    print>>sys.stdout, payrolls.take(12)
    print>>sys.stdout, histo_wage(payrolls,[2011,2012,2013,2014])
    #print>>sys.stdout, location_wage(payrolls,2010)
