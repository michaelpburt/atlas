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

CAISO includes the following markets: ['RTM','DAM','HASP','RUC']

It is recommended to review the CAISO API reference:
  http://www.caiso.com/Documents/                                           \
    OASIS-InterfaceSpecification_v4_2_6Clean_Independent2015Release.pdf
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
    """
    Accepts: csv string
    Returns: list of lists
    """
    output = []
    for x in i_csv_string.split('\n'):
      output.append(x.split(','))
    return output
  
  def extractFile(self, i_filename, i_filedata):
    """Open zipfile and return file-like object"""
    input_zip=zipfile.ZipFile(i_filedata)
    
    # override filename attr if only one file in archive
    if len(input_zip.namelist()) == 1:
      self.filename = input_zip.namelist()[0]
      return input_zip.read(self.filename)
    
    return input_zip.read(i_filename)

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
    self.datatype = kwargs.get('datatype')
    self.date = kwargs.get('date')
    self.pnode = kwargs.get('pnode')
    self.url = self.buildUrl()
    self.lmp_type = lmp_type
    BaseCaisoLmp.__init__(self)
    
    # build filename
    meta = CaisoGenericLmp.dataTypeMap()
    i_xml_name = [d['xml_name'] for d in meta if 
      d['atlas_datatype'] == self.datatype][0]
    i_market = [d['market'] for d in meta if 
      d['atlas_datatype'] == self.datatype][0]
    self.filename = self.getFileName(i_xml_name,lmp_type,i_market)
  
  def getFileName(self,i_data_type,i_lmp_type,i_market):
    """
    Accepts: i_data_type (str), i_lmp_type (str), i_market (str)
    Returns filename (str)
    
    This method is used to construct the filename. When only one file exists
    in the archive, the self.filename attr is overridden in self.extractFile().
    Note that this method does not assign attr self.filename.
    """
    
    # Build a dict of url args because we need startdatetime for filename
    ulist = self.url.split('&')
    udict = {}
    for i in ulist:
      udict[i.split('=')[0]] = i.split('=')[1]
      
    # some files don't break out the MCE, MCC, MLC; treat accordingly
    meta = CaisoGenericLmp.dataTypeMap()
    if [d['lmp_component_split'] for d in meta if 
      d['atlas_datatype'] == self.datatype][0]:
      return '{0}_{0}_{1}_{2}_{3}_v1.csv'.format(
        udict['startdatetime'][:8],
        i_data_type,
        i_market,
        i_lmp_type)
    else:
      return '{0}_{0}_{1}_{2}_v1.csv'.format(
        udict['startdatetime'][:8],
        i_data_type,
        i_market)

  def buildUrl(self):
    """
    Accepts:  self
    Returns:  url (str)
    
    Relies on following attr: self.datatype, self.date, self.pnode (opt.)
    """
    meta = CaisoGenericLmp.dataTypeMap()
    base = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname='
    url = base + '{0}&version=1&startdatetime={1}T07:00-0000'.format(
      # add the appropriate xml_name for the datatype
      [d['xml_name'] for d in meta if 
        d['atlas_datatype'] == self.datatype][0],
      self.date.strftime('%Y%m%d'))
    url = url + '&enddatetime={0}T07:00-0000&market_run_id={1}&resultformat=6'\
      .format((self.date + datetime.timedelta(days=1)).strftime('%Y%m%d'),
        # add the appropriate market for the datatype
        [d['market'] for d in meta if 
          d['atlas_datatype'] == self.datatype][0])
      
    # if a pnode arg has been supplied in __init__, then add to url
    if self.pnode:
      url = url + '&node={0}'.format(self.pnode)
    return url
  
  @classmethod
  def dataTypeMap(cls):
    """
    This class method is a map for atlas datatypes to CAISO OASIS datatypes.
    Additional metadata is included to help with descrepencies between files.
    """
    datatypes = [{
        'atlas_datatype':'HALMP_PRC',
        'xml_name':'PRC_HASP_LMP',
        'market':'HASP',
        'xml_data_items':
          ['LMP_CONG_PRC','LMP_ENE_PRC','LMP_LOSS_PRC','LMP_PRC','LMP_GHG_PRC'],
        'lmp_component_split':False,
        'col_offset':0
      },{
        'atlas_datatype':'RTLMP_RTPD',
        'xml_name':'PRC_RTPD_LMP',
        'market':'RTPD',
        'xml_data_items':
          ['LMP_CONG_PRC','LMP_ENE_PRC','LMP_LOSS_PRC','LMP_PRC','LMP_GHG_PRC'],
        'lmp_component_split':False,
        'col_offset':-1
      },{
        'atlas_datatype':'DALMP_PRC',
        'xml_name':'PRC_LMP',
        'market':'DAM',
        'xml_data_items':
          ['LMP_CONG_PRC','LMP_ENE_PRC','LMP_LOSS_PRC','LMP_PRC'],
        'lmp_component_split':True,
        'col_offset':0
      }]
    return datatypes
  
  def loadData(self, i_csv_list):
    """
    Accepts:  i_csv_list (list) < list of lists representing the csv file
    Returns:  Pandas DataFrame
    
    - All files have been GMT; we need to double check on DST issues
    """
    meta = CaisoGenericLmp.dataTypeMap()
    offset = [d['col_offset'] for d in meta 
      if d['atlas_datatype'] == self.datatype][0]
    output = []
    for row in i_csv_list:
      try:
        d = {'datatype':self.datatype,
          'iso':'CAISO',
          'node':row[6 + offset],
          'node_type':'',
          'dt_utc':datetime.datetime.strptime(row[0]
            ,'%Y-%m-%dT%H:%M:%S-00:00'),
          'price':float(row[14 + offset]),
          'lmp_type':row[9 + offset]}
        output.append(d)
      except Exception, er:
        """
        No logging implemented, but this is where we would handle errors 
        from failed rows and log it
        """
        self.rows_rejected += 1
        pass
    output = list(filter(lambda d: d['lmp_type'] in [self.lmp_type], output))
    self.data = pandas.DataFrame(sorted(output, key=lambda k: k['dt_utc']))
    self.rows_accepted = len(self.data)
    return self.data
    
  def getData(self, **kwargs):
    """
    Accepts:  self
    Returns:  Pandas DataFrame
    """
    self.getFile()
    csv_str = self.extractFile(self.filename, self.fileobject)
    csv_list = self.parseCsvFile(csv_str)
    self.loadData(csv_list)
    return self.data