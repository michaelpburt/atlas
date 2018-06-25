# -*- coding: utf-8 -*-
"""
        atlas.energy.ercot
        ~~~~~~~~~~~~~~
        This file provides classes for handling ERCOT LMP data.
        
        :copyright: © 2018 by Veridex
        :license: MIT, see LICENSE for more details.
        
        ERCOT data portal:
        http://www.ercot.com/mktinfo/prices
"""

import sys
import datetime
import zipfile
import StringIO

import pandas
import requests
import pytz

from atlas import BaseCollectEvent


class BaseErcotLmp(BaseCollectEvent):
    """This is the Super Class for all ERCOT LMP collector classes."""
    
    def __init__(self, **kwargs):
        BaseCollectEvent.__init__(self)
        
    def get_file(self):
        """This method overrides the superclass method. This method 
        generates a GET request on the self.url resource. It returns a 
        ZipFile file object.
        """
        r = requests.get(self.url, stream=True)
        return zipfile.ZipFile(StringIO.StringIO(r.content))
    
    def extract_file(self, i_filedata):
        """Overrides Superclass method. Open zipfile and return 
        StringIO file-like object.
        """
        output = StringIO.StringIO()
        # override filename attr if only one file in archive
        self.filename = i_filedata.namelist()[0][:-3] + 'zip'
        output.write(i_filedata.read(self.filename[:-3] + 'csv'))
        output.seek(0)
        return output
    
    def get_data(self):
        """This method overrides the superclass method. This method 
        generates a GET request on the self.url resource, unzips the 
        file, and parses it into a Pandas DataFrame.
        """
        self.fileobject = self.get_file()
        unzipped = self.extract_file(self.fileobject)
        csvstr = unzipped.read()
        payload = self.load_data(self.get_csv_list_from_str(csvstr))
        del unzipped
        return payload
        

class ErcotDaLmp(BaseErcotLmp):
    """This is the generic LMP Class for ERCOT. Right now we only 
    collect the ERCOT LMP data in daily increments."""
    
    def __init__(self, **kwargs):
        BaseErcotLmp.__init__(self)
        self.url = kwargs.get('url')
        self.datatype = 'DALMP'
    
    def load_data(self, i_csv_list):
        output = []
        headers = i_csv_list[0]
        print headers
        localtz = pytz.timezone('America/Chicago')
        for row in i_csv_list[1:]:
            _d = dict(zip(
                [h.lower() for h in headers], [x.upper() for x in row]))
            try:
                d = {
                    'datatype':     self.datatype,
                    'iso':          'ERCOT',
                    'node':         _d['settlementpoint'],
                    'dt_utc':       (datetime.datetime
                                        .strptime(
                                            '{0} {1}'.format(
                                                _d['deliverydate'],
                                                _d['hourending']),
                                            '%m/%d/%Y %H:%M')
                                        + datetime.timedelta(hours=-1)),
                    'energy':       '',
                    'cong':         '',
                    'loss':         '',
                    'lmp':          float(_d['settlementpointprice']),
                }
                output.append(d)
            except Exception, er:
                print er
                pass
        cols_ordered = [
            'datatype','iso','node','dt_utc'
            ,'energy','cong','loss','lmp',
        ]
        self.data = pandas.DataFrame(output)[cols_ordered]
        return self.data


class ErcotRtLmp(BaseErcotLmp):
    """This is the generic LMP Class for ERCOT. Right now we only 
    collect the ERCOT LMP data in daily increments."""
    
    def __init__(self, **kwargs):
        BaseErcotLmp.__init__(self)
        self.url = kwargs.get('url')
        self.datatype = 'RTLMP'
    
    def load_data(self, i_csv_list):
        output = []
        headers = i_csv_list[0]
        print [h.lower() for h in headers]
        localtz = pytz.timezone('America/Chicago')
        for row in i_csv_list[1:]:
            _d = dict(zip(
                [h.lower() for h in headers], [x.upper() for x in row]))
            try:
                dt = (localtz.localize(datetime.datetime
                    .strptime(
                        '{0} {1}:{2}'.format(
                            _d['deliverydate'],
                            _d['deliveryhour'],
                            (int(_d['deliveryinterval'])%4)*15),
                        '%m/%d/%Y %H:%M')).astimezone(pytz.timezone('UTC'))
                    + datetime.timedelta(hours=-1))
                d = {
                    'datatype':     self.datatype,
                    'iso':          'ERCOT',
                    'node':         _d['settlementpointname'],
                    'dt_utc':       dt,
                    'energy':       '',
                    'cong':         '',
                    'loss':         '',
                    'lmp':          float(_d['settlementpointprice']),
                }
                output.append(d)
            except Exception, er:
                pass
        cols_ordered = [
            'datatype','iso','node','dt_utc'
            ,'energy','cong','loss','lmp',
        ]
        self.data = pandas.DataFrame(output)[cols_ordered]
        return self.data