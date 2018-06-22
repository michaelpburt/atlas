"""
This file provides classes for handling MISO LMP data. The loadData method 
will return a Pandas DataFrame with the associated LMP data. 

All datetimes are converted to UTC.

MISO data portal:
  https://www.misoenergy.org/markets-and-operations/market-reports/

Current supported datatypes include:
  RTLMP_PRELIM  - MisoRtLmpPrelim
  DALMP_EXPOST  - MisoDaExPostLmp
  DALMP_EXANTE  - MisoDaExAnteLmp
  RTLMP         - MisoRtLmpFinalMarket

Additional datatypes for collection:
  DALMP         - MisoDaLmpArchive < These are the archived DALMP prices
  RTLMP         - MisoRtLmpArchive < These are the archived RTLMP prices
"""

import pandas
import zipfile
import pytz
import datetime

from atlas import BaseCollectEvent

"""This is the Super Class for all MISO LMP collector classes"""
class BaseMisoLmp(BaseCollectEvent):
  def __init__(self, **kwargs):
    BaseCollectEvent.__init__(self)
    self.rows_rejected = 0
    self.rows_accepted = 0

  def parseCsvFile(self, i_csv_string):
    """Accepts a string and returns list of lists"""
    output = []
    for x in i_csv_string.split('\n'):
      output.append(x.split(','))
    return output
  
  def extractFile(self, i_filename, i_filedata):
    """Open zipfile and return file-like object"""
    input_zip=zipfile.ZipFile(i_filedata)
    return input_zip.read(i_filename)
  
  def loadData(self, i_csv_list):
    """
    Accepts:  list of lists representing the csv file
    Returns:  Pandas DataFrame
    
    - All files have been EST; we need to double check on DST issues
    """
    localtz = pytz.timezone('America/New_York')
    
    """
    - Find actual header row (lots of non-data rows in top of csv file)
    - 'clean' is the input csv without the junk at the top
    """
    r = [x for x in i_csv_list if x[0] == 'Node'][0]
    clean = i_csv_list[i_csv_list.index(r):]
    headers = [x.strip().upper().replace('HE ','') for x in clean[0]]
    date = datetime.datetime.strptime(self.filename[0:8], '%Y%m%d')
    
    """
    - Loop through rows and build dictionary for each row with cleaned values
    - Notice the DT conversions
    - This part looks complicated because we are pivoting the LMP data from 
      wide to long format
    """
    output = []
    for row in clean[1:]: 
      try:
        d = dict(zip(headers, [x.upper().replace('\r','') for x in row]))
        d['data'] = [{
          'datatype':self.datatype,
          'iso':'MISO',
          'node':d['NODE'],
          'node_type':d['TYPE'],
          'dt_utc':localtz.localize(date + datetime.timedelta(hours=int(x)-1))
            .astimezone(pytz.timezone('UTC')),
          'price':float(d[str(x)])
          } for x in range(1,25)]
        for i in d['data']:
          try:
            i['lmp_type'] = d['VALUE']
          except:
            i['lmp_type'] = 'Unknown'
        output.extend(d['data'])
      except Exception, er:
        """
        No logging implemented, but this is where we would handle errors 
        from failed rows and log it
        """
        self.rows_rejected += 1
        pass
    self.data = pandas.DataFrame(output)
    self.rows_accepted = len(self.data)
    return self.data

"""
The following classes are subclassed from BaseMisoLmp. These represent 
individual MISO LMP scrapers.
  
- You must supply a url constructor using a keyword arg
- Get a Pandas DataFrame with the data by calling getData() method on the object
"""
class MisoRtLmpFinalMarket(BaseMisoLmp):
  """
  The url convention for this data is:
  https://docs.misoenergy.org/marketreports/{year}{month}{day}_rt_lmp_final.csv
  """
  def __init__(self, **kwargs):
    self.url = kwargs.get('url')
    BaseMisoLmp.__init__(self)
    self.filename = self.url.split('/')[-1]
    self.datatype = 'RTLMP'
    self.collector = 'MisoRtLmpFinalMarket'
    
  def getData(self):
    self.getFile()
    csv_list = self.parseCsvFile(self.fileobject.read())
    self.loadData(csv_list)
    return self.data

class MisoRtLmpPrelim(BaseMisoLmp):
  """
  The url convention for this data is:
  https://docs.misoenergy.org/marketreports/{year}{month}{day}_rt_lmp_prelim.csv
  """
    
  def __init__(self, **kwargs):
    self.url = kwargs.get('url')
    BaseMisoLmp.__init__(self)
    self.filename = self.url.split('/')[-1]
    self.datatype = 'RTLMP_PRELIM'
    self.collector = 'MisoRtLmpPrelimMarket'
    
  def getData(self):
    self.getFile()
    csv_list = self.parseCsvFile(self.fileobject.read())
    self.loadData(csv_list)
    return self.data

class MisoDaExAnteLmp(BaseMisoLmp):
  """
  The url convention for this data is:
  https://docs.misoenergy.org/marketreports/{year}{month}{day}_da_exante_lmp.csv
  """
    
  def __init__(self, **kwargs):
    self.url = kwargs.get('url')
    BaseMisoLmp.__init__(self)
    self.filename = self.url.split('/')[-1]
    self.datatype = 'DALMP_EXANTE'
    self.collector = 'MisoDaExAnteLmp'
    
  def getData(self):
    self.getFile()
    csv_list = self.parseCsvFile(self.fileobject.read())
    self.loadData(csv_list)
    return self.data

class MisoDaExPostLmp(BaseMisoLmp):
  """
  The url convention for this data is:
  https://docs.misoenergy.org/marketreports/{year}{month}{day}_da_expost_lmp.csv
  """
    
  def __init__(self, **kwargs):
    self.url = kwargs.get('url')
    BaseMisoLmp.__init__(self)
    self.filename = self.url.split('/')[-1]
    self.datatype = 'DALMP_EXPOST'
    self.collector = 'MisoDaExPostLmp'
    
  def getData(self):
    self.getFile()
    csv_list = self.parseCsvFile(self.fileobject.read())
    self.loadData(csv_list)
    return self.data

