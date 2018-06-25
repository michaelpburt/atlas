# -*- coding: utf-8 -*-
"""
        atlas.energy.caiso
        ~~~~~~~~~~~~~~
        This file provides classes for handling CAISO LMP data.
        
        :copyright: © 2018 by Veridex
        :license: MIT, see LICENSE for more details.
        
        CAISO data portal:
        http://oasis.caiso.com/oasisapi/SingleZip
            ?queryname={DATA_TYPE}
            &startdatetime={YYYYMMDD}T07:00-0000                                    
            &enddatetime={YYYYMMDD}T07:00-0000                                      
            &market_run_id={MARKET}    
            &version=1                                              
            &resultformat=6                 << specifies a csv file
            
        CAISO API Reference:
        http://www.caiso.com/Documents/OASIS-
            InterfaceSpecification_v4_2_6Clean_Independent2015Release.pdf
"""


import datetime
import zipfile
import StringIO

import pandas
import requests

from atlas import BaseCollectEvent


class CaisoLmp(BaseCollectEvent):
    """This is the generic LMP Class for CAISO."""
    
    def __init__(self, **kwargs):
        BaseCollectEvent.__init__(self)
        self.rows_rejected = 0
        self.rows_accepted = 0
        self.url = kwargs.get('url')
        self.datatype = CaisoLmp._get_datatype_from_url(url=self.url)
        self.filename = self.get_file_name_from_url(self.url)
        
    def get_data(self):
        """This method overrides the superclass method. This method 
        generates a GET request on the self.url resource. It returns a 
        StringIO file object.
        """
        self.fileobject = self.get_file()
        unzipped = self.extract_file(self.fileobject)
        csvstr = unzipped.read()
        payload = self.load_data(self.get_csv_list_from_str(csvstr))
        del unzipped
        return payload
        
    def get_file(self):
        """This method overrides the superclass method. This method 
        generates a GET request on the self.url resource. It returns a 
        StringIO file object.
        """
        r = requests.get(self.url, stream=True)
        return zipfile.ZipFile(StringIO.StringIO(r.content))
        
    def extract_file(self, i_filedata):
        """Overrides Superclass method. Open zipfile and return 
        file-like object.
        """
        output = StringIO.StringIO()
        # override filename attr if only one file in archive
        if len(i_filedata.namelist()) == 1:
            self.filename = i_filedata.namelist()[0][:-3] + 'zip'
            output.write(i_filedata.read(self.filename[:-3] + 'csv'))
            output.seek(0)
            return output
        else:
            for f in i_filedata.namelist():
                print f
                if f == i_filedata.namelist()[0]:
                    output.write(i_filedata.read(f))
                else:
                    otemp = StringIO.StringIO()
                    otemp.write(i_filedata.read(f))
                    otemp.seek(0)
                    output.write('\n'.join(otemp.readlines()[1:]))
            output.seek(0)
            return output
    
    @classmethod
    def get_file_name_from_url(cls, url):
        """This method is used to construct the filename. When only 
        one file exists in the zip archive, the self.filename attr 
        is overridden in self.extract_file().
        """
        meta = CaisoLmp.datatype_config()
        # assign datatype
        filename_ext = [d['filename'] for d in meta 
            if d['xml_name'] in url][0]
        # Build a dict of url args because we need startdatetime for filename
        ulist = url.split('&')
        udict = {}
        for i in ulist:
            udict[i.split('=')[0]] = i.split('=')[1]
        # some files don't break out the MCE, MCC, MLC; treat accordingly
        return '{0}_{1}_{2}'.format(
            udict['startdatetime'][:8]
            ,udict['enddatetime'][:8]
            ,filename_ext)
    
    def load_data(self, i_csv_list):
        """This method accepts a list of lists representing the csv
        file and it returns a Pandas DataFrame. 
        """
        meta = CaisoLmp.datatype_config()
        price_col = [d['price_col'] for d in meta 
            if d['atlas_datatype'] == self.datatype][0]
        headers = [x.strip().upper() for x in i_csv_list[0]]
        output = []
        for row in i_csv_list[1:]:
            _d = dict(zip(
                [h.lower() for h in headers], [x.upper() for x in row]))
            try:
                d = {
                    'datatype':     self.datatype,
                    'iso':          'CAISO',
                    'node':         _d['node'],
                    'node_type':    '',
                    'dt_utc':       datetime.datetime.strptime(
                                        _d['intervalstarttime_gmt'],
                                        '%Y-%m-%dT%H:%M:%S-00:00'),
                    'price':        float(_d[price_col]),
                    'lmp_type':     _d['lmp_type'],
                }
                output.append(d)
            except Exception, er:
                """No logging implemented, but this is where we 
                would handle errors from failed rows and log it.
                """
                self.rows_rejected += 1
                pass
        raw = pandas.DataFrame(output)
        lmp = (raw[raw.lmp_type == 'LMP']
               .rename(columns={'price': 'lmp'})
               .set_index(['dt_utc','node'])
               .drop(['lmp_type'], axis=1))
        mcc = (raw[raw.lmp_type == 'MCC']
               .rename(columns={'price': 'cong'})
               .set_index(['dt_utc','node'])
               .drop(['datatype', 'iso', 'lmp_type'], axis=1))
        mlc = (raw[raw.lmp_type == 'MCL']
               .rename(columns={'price': 'loss'})
               .set_index(['dt_utc','node'])
               .drop(['datatype', 'iso', 'lmp_type'], axis=1))
        mce = (raw[raw.lmp_type == 'MCE']
               .rename(columns={'price': 'energy'})
               .set_index(['dt_utc','node'])
               .drop(['datatype', 'iso', 'lmp_type'], axis=1))
        
        joined = (lmp
            .join(mcc, how='left', lsuffix='_left', rsuffix='_right')
            .join(mlc, how='left', lsuffix='_left', rsuffix='_right')
            .join(mce, how='left', lsuffix='_left', rsuffix='_right')
            .reset_index(level=['dt_utc', 'node'])
            .sort_values(by=['node','dt_utc'])
            .reset_index())
        
        self.rows_accepted = len(joined)
        cols_ordered = [
            'datatype','iso','node','dt_utc'
            ,'energy','cong','loss','lmp',
        ]
        self.data = joined[cols_ordered]
        return self.data

    @classmethod
    def build_url(cls, **kwargs):
        """This class method builds a url from the startdate, 
        enddate, pnode, datatype arg."""
        meta = CaisoLmp.datatype_config()
        config_dict = [d for d in meta if 
            d['atlas_datatype'] == kwargs.get('datatype')][0]
        try:
            startdate = kwargs.get('date').strftime('%Y%m%d')
        except Exception, er:
            startdate = kwargs.get('startdate').strftime('%Y%m%d')
            pass
        # filter pnode constructor
        if kwargs.get('pnode'):
            try:
                pnode_url = '&node={0}'.format(kwargs.get('pnode'))
                enddate = kwargs.get('enddate').strftime('%Y%m%d')
            except AttributeError:
                enddate = (datetime.datetime.strptime(startdate, '%Y%m%d') 
                    + datetime.timedelta(days=1)).strftime('%Y%m%d')
                print 'enddate set to {0}'.format(enddate)
        else:
            pnode_url = ''
            enddate = (datetime.strptime(startdate, '%Y%m%d') 
                + datetime.timedelta(days=1)).strftime('%Y%m%d')
        url = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname='
        url += '{0}&startdatetime={1}T07:00-0000'.format(
            # add the appropriate xml_name for the datatype
            config_dict['xml_name'],
            startdate)
        url += '&enddatetime={0}T07:00-0000&market_run_id={1}'.format(
            enddate,
            config_dict['market'])
        url += '&resultformat=6&version=1' + pnode_url
        return url
    
    @classmethod
    def _get_datatype_from_url(cls, **kwargs):
        """This class method finds the datatype for a given url."""
        meta = CaisoLmp.datatype_config()
        url = kwargs.get('url')
        conf = [i for i in meta if i['xml_name'] in url][0]
        return conf['atlas_datatype']
    
    @classmethod
    def datatype_config(cls):
        """This class method maps the Atlas datatype to CAISO 
        API fields."""
        config = [
            {
                'atlas_datatype':       'HALMP_PRC', # failing multi-day test
                'xml_name':             'PRC_HASP_LMP',
                'market':               'HASP',
                'xml_data_items':       [
                                            'LMP_CONG_PRC', 'LMP_ENE_PRC',
                                            'LMP_LOSS_PRC', 'LMP_PRC',
                                            'LMP_GHG_PRC',
                                        ],
                'lmp_component_split':  True,
                'price_col':           'mw',
                'singlezip':           True,
                'filename':             '_HASP_LMP_GRP_N_N_v1_csv.zip',
            },{
                'atlas_datatype':       'RTLMP_RTPD',
                'xml_name':             'PRC_RTPD_LMP',
                'market':               'RTPD',
                'xml_data_items':       [
                                            'LMP_CONG_PRC', 'LMP_ENE_PRC',
                                            'LMP_LOSS_PRC', 'LMP_PRC',
                                            'LMP_GHG_PRC',
                                        ],
                'lmp_component_split':  True,
                'price_col':           'prc',
                'singlezip':           True,
                'filename':             '_RTPD_LMP_GRP_N_N_v1_csv.zip',
            },{
                'atlas_datatype':       'DALMP_PRC',
                'xml_name':             'PRC_LMP',
                'market':               'DAM',
                'xml_data_items':       [
                                            'LMP_CONG_PRC', 'LMP_ENE_PRC',
                                            'LMP_LOSS_PRC', 'LMP_PRC',
                                        ],
                'lmp_component_split':  True,
                'price_col':            'mw',
                'singlezip':            False,
                'filename':             '_DAM_LMP_GRP_N_N_v1_csv.zip',
            }
        ]
        return config