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
(ATLAS) ~$ pip install -r atlas/requirements.txt
```

## Examples

### Scrape some MISO prices
The following shows how to download some MISO LMP data into a Pandas DataFrame.

Start by entering the interpreter:
```
~$ cd ~/
~$ source ATLAS/bin/activate
(ATLAS) ~$ python
```
Now download some data and inspect it:

```
>>> import datetime
>>> 
>>> import atlas.energy.miso as miso
>>> 
>>> dts = datetime.datetime(2018,6,19)
>>> dte = datetime.datetime(2018,6,20)
>>> 
>>> # use the class method datatype_config to see the Atlas datatypes
>>> for dtype in miso.MisoLmp.datatype_config():
...     print dtype['atlas_datatype']
... 
DALMP_EXPOST
DALMP_EXANTE
RTLMP_PRELIM
RTLMP
>>> # use the class method build_url to figure out where to get the data from
>>> m_url = miso.MisoLmp.build_url(datatype='RTLMP', startdate=dts, enddate=dte)
>>> print m_url
https://docs.misoenergy.org/marketreports/20180619_rt_lmp_final.csv
>>> 
>>> 
>>> miso_rt = miso.MisoLmp(url=m_url)
>>> df = miso_rt.get_data()
>>> df
      datatype   iso         node                    dt_utc  energy  cong  loss    lmp
0        RTLMP  MISO          AEC 2018-06-19 04:00:00+00:00   22.81  0.00 -0.05  22.76
1        RTLMP  MISO          AEC 2018-06-19 05:00:00+00:00   22.27  0.00 -0.07  22.20
2        RTLMP  MISO          AEC 2018-06-19 06:00:00+00:00   21.84  0.00 -0.08  21.76
...        ...   ...          ...                       ...     ...   ...   ...    ...
52653    RTLMP  MISO          YAD 2018-06-20 01:00:00+00:00   24.41  0.00  0.19  24.60
52654    RTLMP  MISO          YAD 2018-06-20 02:00:00+00:00   22.95  0.00  0.04  22.99
52655    RTLMP  MISO          YAD 2018-06-20 03:00:00+00:00   21.63  0.00  0.02  21.65

[52656 rows x 8 columns]
>>> 

```

### Scrape some CAISO prices and look at DA/RT returns
The following shows how to download some CAISO LMP data into a Pandas DataFrame
and compare different datatypes to see DA/RT returns.

Start by entering the interpreter:
```
~$ cd ~/
~$ source ATLAS/bin/activate
(ATLAS) ~$ python
```
Now download some data and play around with it:

```
>>> import datetime
>>> 
>>> import atlas.energy.caiso as caiso
>>> 
>>> dts = datetime.datetime(2018,6,10)
>>> dte = datetime.datetime(2018,6,18)
>>> 
>>> c_url_rt = caiso.CaisoLmp.build_url(
...     datatype='RTLMP_RTPD', 
...     pnode='CGAP_CHPD_MIDC-APND', 
...     startdate=dts,
...     enddate=dte)
>>> c_url_da = caiso.CaisoLmp.build_url(
...     datatype='DALMP_PRC', 
...     pnode='CGAP_CHPD_MIDC-APND', 
...     startdate=dts,
...     enddate=dte)
>>> 
>>> caiso_rt = caiso.CaisoLmp(url=c_url_rt)
>>> caiso_da = caiso.CaisoLmp(url=c_url_da)
>>> 
>>> caiso_rt_data = caiso_rt.get_data()
>>> caiso_da_data = caiso_da.get_data()
>>> 
>>> midc_rt = (caiso_rt_data[caiso_rt_data.node == 'CGAP_CHPD_MIDC-APND']
...     .rename(columns={'lmp': 'rtlmp_rtpd'})
...     .set_index('dt_utc') 
...     .drop(['datatype','iso','node','energy','cong','loss'], axis=1)  
...     .resample('H')
...     .mean())
>>> midc_da = (caiso_da_data[caiso_da_data.node == 'CGAP_CHPD_MIDC-APND'] 
...     .rename(columns={'lmp': 'dalmp'})
...     .set_index('dt_utc')
...     .drop(['datatype','iso','node','energy','cong','loss'], axis=1))
>>> 
>>> midc = midc_da.join(midc_rt)
>>> midc
                        dalmp  rtlmp_rtpd
dt_utc                                   
2018-06-10 07:00:00  13.08213    8.759810
2018-06-10 08:00:00  14.16261    9.356127
2018-06-10 09:00:00  14.75799   10.517850
...                       ...         ...
2018-06-18 04:00:00  34.73098   21.800213
2018-06-18 05:00:00  27.78227   20.840042
2018-06-18 06:00:00  23.63532   20.902700

[192 rows x 2 columns]
>>> 
```

## Next steps

* Add in PJM, ERCOT, NYISO, NEISO LMP's
* Write collectors for load, ancillary services
* Write NCDC collector

## Authors

* **[Michael Burt](http://mpburt.com/resume/)**
