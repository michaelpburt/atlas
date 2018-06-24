# Atlas Data Scraping Library

This module provides a set of collectors for energy data. The primary focus of 
the data collection effort is ISO market data, but other sources are also 
included such as EPA, FERC, and EIA.

The module is written in Python 2.7. It is recommended that you install this 
module within a Virtual Environment to avoid clashing dependencies. The module 
was written using as few 3rd party modules as possible.

## Getting Started

Download the modules and copy it to your home folder. On a Debian-flavored 
system, use the following as a guide:
```
~$ sudo apt-get upgrade -y && update -y
~$ sudo apt-get install python-pip python-dev build-essential virtualenv -y
~$ sudo pip install --upgrade pip
~$ sudo pip install --upgrade virtualenv
~$ sudo apt-get install subversion -y

~$ cd ~/

~$ svn checkout https://github.com/michaelpburt/atlas/trunk/atlas

~$ virtualenv ATLAS
~$ source ATLAS/bin/activate
~$ pip install -r atlas/requirements.txt
```

## Examples

### Scrape some MISO prices
The following shows how to download some MISO LMP data into a Pandas DataFrame.

Start by entering the interpreter:
```
~$ cd ~/
~$ source ATLAS/bin/activate
~$ python
```
Now download some data and inspect it:

```
>>> import atlas.energy.miso as miso
>>> import atlas.energy.caiso as caiso
>>> import datetime
>>> 
>>> dts = datetime.datetime(2018,6,19)
>>> dte = datetime.datetime(2018,6,20)
>>> 
>>> miso_rt = miso.MisoLmp(
...     datatype='RTLMP_PRELIM',
...     startdate=dts,
...     enddate=dte)
>>> 
>>> df = miso_rt.get_data()
>>> df

            datatype                    dt_utc   iso lmp_type         node  node_type  price
0       RTLMP_PRELIM 2018-06-19 04:00:00+00:00  MISO      LMP          AEC  INTERFACE  22.76
1       RTLMP_PRELIM 2018-06-19 05:00:00+00:00  MISO      LMP          AEC  INTERFACE  22.20
2       RTLMP_PRELIM 2018-06-19 06:00:00+00:00  MISO      LMP          AEC  INTERFACE  21.76
...              ...                       ...   ...      ...          ...        ...    ...
157965  RTLMP_PRELIM 2018-06-20 01:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.19
157966  RTLMP_PRELIM 2018-06-20 02:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.04
157967  RTLMP_PRELIM 2018-06-20 03:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.02

[157968 rows x 7 columns]
>>> 
```

### Scrape some CAISO prices and look at DA/RT returns
The following shows how to download some CAISO LMP data into a Pandas DataFrame
and compare different datatypes to see DA/RT returns.

Start by entering the interpreter:
```
~$ cd ~/
~$ source ATLAS/bin/activate
~$ python
```
Now download some data and play around with it:

```
>>> import atlas.energy.caiso as caiso
>>> import datetime
>>> 
>>> dt = datetime.datetime(2018,06,10)
>>> 
```
If you want to inspect the different CAISO datatypes, use the following code
snippet:
```
>>> dtypes = caiso.CaisoLmp.datatype_config()
>>> for type in dtypes:
...   print type['atlas_datatype']
HALMP_PRC
RTLMP_RTPD
DALMP_PRC
```
Let's take a look at the DART spread at MIDC by using two of the above datatypes:
```
>>> 
>>> caiso_rt = caiso.CaisoLmp(
...     datatype='RTLMP_RTPD',
...     startdate=dts,
...     enddate=dte,
...     pnode='CGAP_CHPD_MIDC-APND')
>>> caiso_da = caiso.CaisoLmp(
...     datatype='DALMP_PRC',
...     startdate=dts,
...     enddate=dte,
...     pnode='CGAP_CHPD_MIDC-APND')
>>> 
>>> caiso_da_data = caiso_da.get_data()
>>> caiso_rt_data = caiso_rt.get_data()
>>> 
>>> midc_da = (caiso_da_data[caiso_da_data.node == 'CGAP_CHPD_MIDC-APND'] 
...     .rename(columns={'price': 'dalmp_prc'})
...     .set_index('dt_utc')
...     .drop(['datatype','iso','lmp_type','node_type','node'], axis=1))
>>> 
>>> midc_rt = (caiso_rt_data[caiso_rt_data.node == 'CGAP_CHPD_MIDC-APND']
...     .rename(columns={'price': 'rtlmp_rtpd'})
...     .set_index('dt_utc') 
...     .drop(['datatype','iso','lmp_type','node_type','node'], axis=1)     
...     .resample('H')
...     .mean())
>>> 
>>> midc = midc_da.join(midc_rt)
>>> midc
                     dalmp_prc  rtlmp_rtpd
dt_utc                                    
2018-06-10 07:00:00   13.08213    8.759810
2018-06-10 08:00:00   14.16261    9.356127
2018-06-10 09:00:00   14.75799   10.517850
...                   ...        ...
2018-06-11 04:00:00   34.71625   11.336900
2018-06-11 05:00:00   19.25525   11.990675
2018-06-11 06:00:00   19.45879   17.930340
>>> 

[24 rows x 2 columns]
>>> 
```
It looks like on this particular day RT prices were well below DA prices.

## Next steps

* Add in PJM, SPP, ERCOT, NYISO, NEISO LMP's
* Write collectors for load, ancillary services
* Write NCDC collector

## Authors

* **[Michael Burt](http://mpburt.com/resume/)**
