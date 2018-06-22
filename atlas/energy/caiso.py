"""
This file provides classes for handling CAISO LMP data. The getData method 
will return a Pandas DataFrame with the associated LMP data. 

All datetimes are converted to UTC.

CAISO data portal:
  http://oasis.caiso.com/oasisapi/SingleZip?queryname={DATA_TYPE}&version=1 \
    &startdatetime={YYYYMMDD}T07:00-0000                                    \
    &enddatetime={YYYYMMDD}T07:00-0000                                      \
    &market_run_id={MARKET}                                                 \
    &resultformat=6                             << specifies a csv file

CAISO includes the following markets: ['RTM','DAM','HASP']

Current supported datatypes include:
  DALMP_PRC     - CaisoDaLmpPrc

Additional datatypes for collection:
"""

import datetime
import zipfile
import pandas

from atlas import BaseCollectEvent

class BaseCaisoLmp(BaseCollectEvent):
  """This is the Super Class for all CAISO LMP collector classes."""
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
    
    - All files have been GMT; we need to double check on DST issues
    """
    output = []
    for row in i_csv_list:
      try:
        d = {'datatype':self.datatype,
          'iso':'CAISO',
          'node':row[6],
          'node_type':'',
          'dt_utc':datetime.datetime.strptime(row[0]
            ,'%Y-%m-%dT%H:%M:%S-00:00'),
          'price':float(row[14]),
          'lmp_type':row[9]}
        output.append(d)
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
  
  def getFileName(self,i_data_type,i_lmp_type,i_market):
    ulist = self.url.split('&')
    udict = {}
    for i in ulist:
      udict[i.split('=')[0]] = i.split('=')[1]
    return '{0}_{0}_{1}_{2}_{3}_v1.csv'.format(
      udict['startdatetime'][:8],
      i_data_type,
      i_market,
      i_lmp_type)
    

class CaisoDaLmpPrc(BaseCaisoLmp):
  """
  The url convention for this data is:
    http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&version=1   \
      &startdatetime={YYYYMMDD}T07:00-0000                                  \
      &enddatetime={YYYYMMDD}T07:00-0000                                    \
      &market_run_id=DAM                                                    \
      &resultformat=6                             << specifies a csv file
      
  - By default, just return LMP data. You can override this with the lmp_type 
    kwarg and get MCC, MCE, or MCL LMP prices.
  """
  def __init__(self, lmp_type='LMP', **kwargs):
    self.url = kwargs.get('url')
    BaseCaisoLmp.__init__(self)
    self.filename = self.getFileName('PRC_LMP',lmp_type,'DAM')
    self.datatype = 'DALMP_PRC'
    self.collector = 'CaisoDaLmpPrc'
    
  def getData(self, **kwargs):
    self.getFile()
    csv_str = self.extractFile(self.filename, self.fileobject)
    csv_list = self.parseCsvFile(csv_str)
    self.loadData(csv_list)
    return self.data
    

class CaisoGenericLmp(BaseCaisoLmp):
  """
  This is a generic LMP class for CAISO. The available LMP series are stored 
  in cls.dataTypeMap(). 
  
  The constructor must include: datatype (str), date (datetime ob)
  Output is a Pandas DataFrame
  
  You will get back LMP type data, which can be overridden with:
    MCC, MCE, MCL LMP types
  """
  def __init__(self, lmp_type='LMP', **kwargs):
    self.url = self.buildUrl()
    self.datatype = kwargs.get('datatype')
    self.date = kwargs.get('date').strftime('%Y%m%d')
    BaseCaisoLmp.__init__(self)
    self.filename = self.getFileName('PRC_LMP',lmp_type,'DAM')
  
  def buildUrl(self):
    meta = cls.dataTypeMap()
    base = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname='
    url = base + '{0}&version=1&startdatetime={1}'.format(
      self.datatype,
      self.date)
    url = url + '&enddatetime={0}&market_run_id={1}&resultformat=6'.format(
      self.date + datetime.timedelta(days=1),
      # add the appropriate market for the datatype
      [d['market'] for d in meta if 
        meta['atlas_datatype'] == self.datatype][0])
    self.url = url
    return self.url
    
  def dataTypeMap(cls):
    datatypes = [{
        'atlas_datatype':'HALMP_PRC',
        'xml_name':'PRC_HASP_LMP',
        'market':'HASP',
        'xml_data_items':
          ['LMP_CONG_PRC','LMP_ENE_PRC','LMP_LOSS_PRC','LMP_PRC','LMP_GHG_PRC']
      },{
        'atlas_datatype':'DALMP_PRC',
        'xml_name':'PRC_LMP',
        'market':'DAM',
        'xml_data_items':
          ['LMP_CONG_PRC','LMP_ENE_PRC','LMP_LOSS_PRC','LMP_PRC']
      }]
    
  def getData(self, **kwargs):
    self.getFile()
    csv_str = self.extractFile(self.filename, self.fileobject)
    csv_list = self.parseCsvFile(csv_str)
    self.loadData(csv_list)
    return self.data