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
import StringIO
import zipfile

import requests
import pandas as pd
import pytz

from atlas import BaseCollectEvent


class BaseSppLmp(BaseCollectEvent):
    """This is the Super Class for all MISO LMP collector classes."""
    
    def __init__(self, **kwargs):
        BaseCollectEvent.__init__(self)
    
    def load_data(self, i_csv_list):
        output = []
        headers = i_csv_list[0]
        offset = 0
        if 'GMT' in ','.join(headers):
            offset = 1
        for row in i_csv_list[1:]:
            try:
                output = self._proc_row('LMP', 2, output, row, offset)
                output = self._proc_row('MLC', 3, output, row, offset)
                output = self._proc_row('MCC', 4, output, row, offset)
                output = self._proc_row('MEC', 5, output, row, offset)
            except Exception, er:
                print er
                pass
        self.data = pd.DataFrame(output)
        return self.data

    def _proc_row(self, i_lmp_type, i_offset, i_list, row, i_gmt_offset):
        """Helper method to process each row for each LMP type."""
        localtz = pytz.timezone('America/Chicago')
        d = {
            'datatype':     self.datatype,
            'iso':          'SPP',
            'node':         row[1+i_gmt_offset],
            'node_type':    '',
            'dt_utc':       localtz.localize(datetime.datetime
                                .strptime(row[0],'%m/%d/%Y %H:%M:%S'))
                                .astimezone(pytz.timezone('UTC')),
            'price':        float(row[i_offset+i_gmt_offset]),
            'lmp_type':     i_lmp_type,
        }
        i_list.append(d)
        return i_list


class SppDaLmp(BaseSppLmp):
    """This is the generic LMP Class for SPP. Right now we only 
    collect the SPP LMP data in daily increments."""
    
    def __init__(self, **kwargs):
        BaseSppLmp.__init__(self)
        self.url = kwargs.get('url')
        self.filename = self.url[-25:]
        self.datatype = 'DALMP'
        

