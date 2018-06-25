# -*- coding: utf-8 -*-
"""
        atlas.energy.miso
        ~~~~~~~~~~~~~~
        This file provides classes for handling MISO LMP data.
        
        :copyright: © 2018 by Veridex
        :license: MIT, see LICENSE for more details.
        
        MISO data portal:
        https://www.misoenergy.org/markets-and-operations/market-reports/
"""


import zipfile
import datetime

import pandas
import pytz

from atlas import BaseCollectEvent


class MisoLmp(BaseCollectEvent):
    """This is the Super Class for all MISO LMP collector classes."""
    
    def __init__(self, lmp_type='LMP', **kwargs):
        BaseCollectEvent.__init__(self)
        self.url = kwargs.get('url')
        self.filename = self.url.split('/')[-1]
        self.lmp_type = lmp_type
        
        self.rows_rejected = 0
        self.rows_accepted = 0

    def load_data(self, i_csv_list):
        """This method accepts a list of lists representing the csv
        file and it returns a Pandas DataFrame. 
        """
        # all times are in EPT but watch out for DST issues
        localtz = pytz.timezone('America/New_York')
        
        # find actual header row
        r = [x for x in i_csv_list if x[0] == 'Node'][0]
        # clean is the csv file without the top few rows of fluff
        clean = i_csv_list[i_csv_list.index(r):]
        headers = [x.strip().upper().replace('HE ','') for x in clean[0]]
        
        # find the datatype form the url 
        datatype = MisoLmp._get_datatype_from_url(url=self.url)
        
        # loop through clean and build a list of dicts
        # make EPT/UTC conversion on datetime columns
        # pivot table form wide to long format
        output = []
        # initialize a date for datetime columns
        date = datetime.datetime.strptime(self.filename[0:8], '%Y%m%d')
        for row in clean[1:]: 
            try:
                d = dict(zip(headers, 
                        [x.upper().replace('\r','') for x in row]))
                d['data'] = [{
                    'datatype':     datatype,
                    'iso':          'MISO',
                    'node':         d['NODE'],
                    'dt_utc':       localtz.localize(date 
                                        + datetime.timedelta(hours=int(x)-1))
                                        .astimezone(pytz.timezone('UTC')),
                    'price':        float(d[str(x)]),
                    'lmptype':      d['VALUE'],
                } for x in range(1,25)]
                output.extend(d['data'])
            except Exception, er:
                """
                No logging implemented, but this is where we would 
                handle errors from failed rows and log it.
                """
                self.rows_rejected += 1
                pass
        raw = pandas.DataFrame(output)
        lmp = (raw[raw.lmptype == 'LMP']
               .rename(columns={'price': 'lmp'})
               .set_index(['dt_utc','node'])
               .drop(['lmptype'], axis=1))
        mcc = (raw[raw.lmptype == 'MCC']
               .rename(columns={'price': 'cong'})
               .set_index(['dt_utc','node'])
               .drop(['datatype', 'iso', 'lmptype'], axis=1))
        mlc = (raw[raw.lmptype == 'MLC']
               .rename(columns={'price': 'loss'})
               .set_index(['dt_utc','node'])
               .drop(['datatype', 'iso', 'lmptype'], axis=1))
        
        joined = lmp.join(mcc).join(mlc).reset_index(level=['dt_utc', 'node'])
        joined['energy'] = joined['lmp'] - joined['cong'] - joined['loss']
        
        self.rows_accepted = len(joined)
        cols_ordered = [
            'datatype','iso','node','dt_utc'
            ,'energy','cong','loss','lmp',
        ]
        self.data = joined[cols_ordered]
        return self.data
        
    @classmethod
    def build_url(cls, **kwargs):
        """This class method builds  a url for the datatype 
        and date arguments.
        """
        meta = MisoLmp.datatype_config()
        base = 'https://docs.misoenergy.org/marketreports/'
        try:
            startdate = kwargs.get('date').strftime('%Y%m%d')
        except Exception, er:
            startdate = kwargs.get('startdate').strftime('%Y%m%d')
            pass
        url = base + '{0}{1}'.format(
            startdate,
            [d['url_suffix'] for d in meta if 
                d['atlas_datatype'] == kwargs.get('datatype')][0])
        return url
    
    @classmethod
    def _get_datatype_from_url(cls, **kwargs):
        """This class method finds the datatype for a given url."""
        meta = MisoLmp.datatype_config()
        url = kwargs.get('url')
        conf = [i for i in meta if i['url_suffix'] in url][0]
        return conf['atlas_datatype']
    
    @classmethod
    def datatype_config(cls):
        """This class method maps the Atlas datatype to a URL suffix."""
        config = [
            {
                'atlas_datatype':   'DALMP_EXPOST',
                'url_suffix':       '_da_expost_lmp.csv'
            },{
                'atlas_datatype':   'DALMP_EXANTE',
                'url_suffix':       '_da_exante_lmp.csv',
            },{
                'atlas_datatype':   'RTLMP_PRELIM',
                'url_suffix':       '_rt_lmp_prelim.csv'
            },{
                'atlas_datatype':   'RTLMP',
                'url_suffix':       '_rt_lmp_final.csv'
            }
        ]
        return config