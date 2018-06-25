# -*- coding: utf-8 -*-
"""
        atlas.energy.spp
        ~~~~~~~~~~~~~~
        This file provides classes for handling SPP LMP data.
        
        :copyright: © 2018 by Veridex
        :license: MIT, see LICENSE for more details.
        
        SPP data portal:
        https://marketplace.spp.org/
"""

import sys
import datetime

import pandas
import pytz

from atlas import BaseCollectEvent


class BaseSppLmp(BaseCollectEvent):
    """This is the Super Class for all SPP LMP collector classes."""
    
    def __init__(self, **kwargs):
        BaseCollectEvent.__init__(self)
        

class SppDaLmp(BaseSppLmp):
    """This is the generic LMP Class for SPP. Right now we only 
    collect the SPP LMP data in daily increments."""
    
    def __init__(self, **kwargs):
        BaseSppLmp.__init__(self)
        self.url = kwargs.get('url')
        self.filename = self.url[-25:]
        self.datatype = 'DALMP'
    
    def load_data(self, i_csv_list):
        output = []
        headers = i_csv_list[0]
        localtz = pytz.timezone('America/Chicago')
        gmt_col = False
        if 'GMT' in ','.join(headers).upper():
            gmt_col = True
        for row in i_csv_list[1:]:
            _d = dict(zip(
                [h.lower() for h in headers], [x.upper() for x in row]))
            try:
                if gmt_col:
                    dt_utc = (datetime.datetime
                        .strptime(_d['gmtintervalend'],'%m/%d/%Y %H:%M:%S')
                        + datetime.timedelta(hours=-1))
                else:
                    dt_utc = (localtz.localize(datetime.datetime
                        .strptime(_d['interval'],'%m/%d/%Y %H:%M:%S'))
                        .astimezone(pytz.timezone('UTC'))
                        + datetime.timedelta(hours=-1))
                d = {
                    'datatype':     self.datatype,
                    'iso':          'SPP',
                    'node':         _d['pnode'],
                    'dt_utc':       dt_utc,
                    'energy':       float(_d['mec']),
                    'cong':         float(_d['mcc']),
                    'loss':         float(_d['mlc']),
                    'lmp':          float(_d['lmp']),
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
    
    @classmethod
    def build_url(cls, **kwargs):
        """This class method builds a url from the date arg."""
        base = 'https://marketplace.spp.org/file-api/download/da-lmp-by-bus?'
        url = base + 'path=/{0}/{1}/By_Day/DA-LMP-B-{0}{1}{2}0100.csv'.format(
            kwargs.get('date').strftime('%Y'),
            kwargs.get('date').strftime('%m'),
            kwargs.get('date').strftime('%d'))
        return url