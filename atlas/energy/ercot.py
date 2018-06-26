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


class BaseErcot(BaseCollectEvent):
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
        self.filename = i_filedata.namelist()[0] + '.zip'
        output.write(i_filedata.read(self.filename[:-4]))
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
    
    @classmethod
    def get_const_cols(cls):
        return [
            'datatype','iso','dt_utc','constraint_id','constraint_name',
            'contingency_name','shadow_price','max_shadow_price',
            'constraint_limit','constraint_value','violation_amount',
            'from_station','to_station','from_station_kv','to_station_kv',
        ]
        

class ErcotDaLmp(BaseErcot):
    """This is the generic LMP Class for ERCOT. Right now we only 
    collect the ERCOT LMP data in daily increments."""
    
    def __init__(self, **kwargs):
        BaseErcot.__init__(self)
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
                dt = (localtz.localize(datetime.datetime
                    .strptime(
                        '{0} {1}'.format(
                            _d['deliverydate'],
                            _d['deliveryhour']),
                        '%m/%d/%Y %H:%M')).astimezone(pytz.timezone('UTC'))
                + datetime.timedelta(hours=-1))
                d = {
                    'datatype':     self.datatype,
                    'iso':          'ERCOT',
                    'node':         _d['settlementpoint'],
                    'dt_utc':       dt,
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


class ErcotRtLmp(BaseErcot):
    """This is the generic LMP Class for ERCOT. Right now we only 
    collect the ERCOT LMP data in daily increments."""
    
    def __init__(self, **kwargs):
        BaseErcot.__init__(self)
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


class ErcotSced(BaseErcot):
    """This is the generic LMP Class for ERCOT. Right now we only 
    collect the ERCOT LMP data in daily increments."""
    
    def __init__(self, **kwargs):
        BaseErcot.__init__(self)
        self.url = kwargs.get('url')
        self.datatype = 'SCED_GEN'
    
    def extract_file(self, i_filedata):
        """Overrides Superclass method. Open zipfile and return 
        StringIO file-like object.
        """
        output = StringIO.StringIO()
        # override filename attr if only one file in archive
        self.filename = [i for i in i_filedata.namelist() if 
            '60d_SCED_Gen_Resource_Data-' in i][0]
        output.write(i_filedata.read(self.filename))
        output.seek(0)
        return output
    
    @classmethod
    def _proc_sced_curves(cls, i_dict, o_dict):
        """Helper method so we don't have to write the same thing 
        70 times.
        """
        for sced in [1,2]:
            for i in range(1,36):
                key_mw = 'sced{0}_mw{1}'.format(sced, i)
                key_price = 'sced{0}_price{1}'.format(sced, i)
                o_dict[key_mw] = i_dict['sced{0} curve-mw{1}'.format(sced, i)]
                o_dict[key_price] = i_dict[
                    'sced{0} curve-price{1}'.format(sced, i)
                ]
        for tpo in range(1,11):
            key_mw = 'tpo_mw{0}'.format(tpo)
            key_price = 'tpo_price{0}'.format(tpo)
            o_dict[key_mw] = i_dict['submitted tpo-mw{0}'.format(tpo)]
            o_dict[key_price] = i_dict['submitted tpo-price{0}'.format(tpo)]
        return o_dict
            
    def load_data(self, i_csv_list):
        output = []
        headers = [i.lower().strip().replace('"','') for i in i_csv_list[0]]
        localtz = pytz.timezone('America/Chicago')
        for row in i_csv_list[1:]:
            _d = dict(
                zip(
                    [h.lower().strip().replace('"','') for h in headers],
                    [x.upper().strip().replace('"','') for x in row]
                )
            )
            try:
                dt = (localtz.localize(datetime.datetime
                    .strptime(_d['sced time stamp'],'%m/%d/%Y %H:%M:%S'))
                    .astimezone(pytz.timezone('UTC')))
                    
                d = {
                    'datatype':             self.datatype,
                    'iso':                  'ERCOT',
                    'dt_utc':               dt,
                    'resource_name':        _d['resource name'],
                    'resource_type':        _d['resource type'],
                    'output_schedule':      _d['output schedule'],
                    'hsl':                  _d['hsl'],
                    'hasl':                 _d['hasl'],
                    'hdl':                  _d['hdl'],
                    'lsl':                  _d['lsl'],
                    'lasl':                 _d['lasl'],
                    'ldl':                  _d['ldl'],
                    'tele_resource_status': _d['telemetered resource status'],
                    'base_point':           _d['base point'],
                    'tele_net_output':      _d['telemetered net output'],
                    'as_regup':             _d['ancillary service regup'],
                    'as_regdown':           _d['ancillary service regdn'],
                    'as_rrs':               _d['ancillary service rrs'],
                    'as_nsrs':              _d['ancillary service nsrs'],
                    'bid_type':             _d['bid_type'],
                    'startup_cold_offer':   _d['start up cold offer'],
                    'startup_hot_offer':    _d['start up hot offer'],
                    'startup_inter_offer':  _d['start up inter offer'],
                    'min_gen_cost':         _d['min gen cost'],
                    'proxy_ext':            _d['proxy extension'],
                }
                d = ErcotSced._proc_sced_curves(_d, d)
                output.append(d)
            except Exception, er:
                print er
                pass
        self.data = pandas.DataFrame(output)
        return self.data


class ErcotDaConstraint(BaseErcot):
    """This class is for ERCOT DA constraint and shadow price data."""
    
    def __init__(self, **kwargs):
        BaseErcot.__init__(self)
        self.url = kwargs.get('url')
        self.datatype = 'DA_CONSTRAINT'
        self.fileobject = self.get_file()
    
    def load_data(self, i_csv_list):
        output = []
        headers = [i.lower().strip().replace('"','') for i in i_csv_list[0]]
        localtz = pytz.timezone('America/Chicago')
        for row in i_csv_list[1:]:
            _d = dict(
                zip(
                    [h.lower().strip().replace('"','') for h in headers],
                    [x.upper().strip().replace('"','') for x in row]
                )
            )
            try:
                dt = (localtz.localize(datetime.datetime
                    .strptime(_d['deliverytime'],'%m/%d/%Y %H:%M:%S'))
                    .astimezone(pytz.timezone('UTC')))
                d = {
                    'datatype':             self.datatype,
                    'iso':                  'ERCOT',
                    'dt_utc':               dt,
                    'constraint_id':        _d['constraintid'],
                    'constraint_name':      _d['constraintname'],
                    'contingency_name':     _d['contingencyname'],
                    'shadow_price':         _d['shadowprice'],
                    'max_shadow_price':     '',
                    'constraint_limit':     _d['constraintlimit'],
                    'constraint_value':     _d['constraintvalue'],
                    'violation_amount':     _d['violationamount'],
                    'from_station':         _d['fromstation'],
                    'to_station':           _d['tostation'],
                    'from_station_kv':      _d['fromstationkv'],
                    'to_station_kv':        _d['tostationkv'],
                }
                output.append(d)
            except Exception, er:
                print er
                pass
        self.data = pandas.DataFrame(output)[BaseErcot.get_const_cols()]
        return self.data


class ErcotRtConstraint(BaseErcot):
    """This class is for ERCOT RT constraint and shadow price data."""
    
    def __init__(self, **kwargs):
        BaseErcot.__init__(self)
        self.url = kwargs.get('url')
        self.datatype = 'RT_CONSTRAINT'
        self.fileobject = self.get_file()
    
    def load_data(self, i_csv_list):
        output = []
        headers = [i.lower().strip().replace('"','') for i in i_csv_list[0]]
        localtz = pytz.timezone('America/Chicago')
        for row in i_csv_list[1:]:
            _d = dict(
                zip(
                    [h.lower().strip().replace('"','') for h in headers],
                    [x.upper().strip().replace('"','') for x in row]
                )
            )
            try:
                dt = (localtz.localize(datetime.datetime
                    .strptime(_d['scedtimestamp'],'%m/%d/%Y %H:%M:%S'))
                    .astimezone(pytz.timezone('UTC')))
                d = {
                    'datatype':             self.datatype,
                    'iso':                  'ERCOT',
                    'dt_utc':               dt,
                    'constraint_id':        _d['constraintid'],
                    'constraint_name':      _d['constraintname'],
                    'contingency_name':     _d['contingencyname'],
                    'shadow_price':         _d['shadowprice'],
                    'max_shadow_price':     _d['maxshadowprice'],
                    'constraint_limit':     _d['limit'],
                    'constraint_value':     _d['value'],
                    'violation_amount':     _d['violatedmw'],
                    'from_station':         _d['fromstation'],
                    'to_station':           _d['tostation'],
                    'from_station_kv':      _d['fromstationkv'],
                    'to_station_kv':        _d['tostationkv'],
                }
                output.append(d)
            except Exception, er:
                print er
                pass
        self.data = pandas.DataFrame(output)[BaseErcot.get_const_cols()]
        return self.data