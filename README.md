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

The following shows how to download some MISO LMP data into a Pandas DataFrame.

Start by entering the interpreter:
```
~$ cd ~/
~$ source ATLAS/bin/activate
~$ python
```
Now download some data and play around with it:

```
>>> import atlas.energy.miso as miso
>>> 
>>> url_pre = 'https://docs.misoenergy.org/marketreports/20180619_rt_lmp_prelim.csv'
>>> 
>>> miso_rt_lmp_pre = miso.MisoRtLmpPrelim(url=url_pre)
>>> 
>>> lmp_pre_df = miso_rt_lmp_pre.getData()
>>> lmp_pre_df
            datatype                    dt_utc   iso lmp_type         node  node_type  price
0       RTLMP_PRELIM 2018-06-19 04:00:00+00:00  MISO      LMP          AEC  INTERFACE  22.76
1       RTLMP_PRELIM 2018-06-19 05:00:00+00:00  MISO      LMP          AEC  INTERFACE  22.20
2       RTLMP_PRELIM 2018-06-19 06:00:00+00:00  MISO      LMP          AEC  INTERFACE  21.76
3       RTLMP_PRELIM 2018-06-19 07:00:00+00:00  MISO      LMP          AEC  INTERFACE  21.69
4       RTLMP_PRELIM 2018-06-19 08:00:00+00:00  MISO      LMP          AEC  INTERFACE  21.96
...              ...                       ...   ...      ...          ...        ...    ...
157963  RTLMP_PRELIM 2018-06-19 23:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.08
157964  RTLMP_PRELIM 2018-06-20 00:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.13
157965  RTLMP_PRELIM 2018-06-20 01:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.19
157966  RTLMP_PRELIM 2018-06-20 02:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.04
157967  RTLMP_PRELIM 2018-06-20 03:00:00+00:00  MISO      MLC          YAD  INTERFACE   0.02

[157968 rows x 7 columns]
>>> 
```

## Next steps

* Add in CAISO, PJM, SPP, ERCOT, NYISO, NEISO LMP's
* Write collectors for load, ancillary services
* Write NCDC collector

## Authors

* **[Michael Burt](http://mpburt.com/resume/)**
