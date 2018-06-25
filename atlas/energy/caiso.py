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
import urllib2

from atlas import BaseCollectEvent


class CaisoLmp(BaseCollectEvent):
    """This is the generic LMP Class for CAISO."""
    
    def __init__(self, lmp_type='LMP', **kwargs):
        BaseCollectEvent.__init__(self)
        self.rows_rejected = 0
        self.rows_accepted = 0
        self.startdate = kwargs.get('startdate')
        self.pnode = kwargs.get('pnode')
        self.lmp_type = lmp_type
        # default enddate to one day from startdate if pnode is not supplied
        if self.pnode is None:
            self.enddate = self.startdate + datetime.timedelta(days=1)
        else:
            self.enddate = kwargs.get('enddate')
        self.datatype = kwargs.get('datatype')
        self.url = self.build_url()
        BaseCaisoLmp.__init__(self)
        
        # build filename
        meta = CaisoLmp.datatype_config()
        i_xml_name = [d['xml_name'] for d in meta if 
            d['atlas_datatype'] == self.datatype][0]
        i_market = [d['market'] for d in meta if 
            d['atlas_datatype'] == self.datatype][0]
        self.filename = self.get_file_name(i_xml_name,lmp_type,i_market)
        
    def get_file(self):
        """This method overrides the superclass method. This method 
        generates a GET request on the self.url resource. It returns a 
        StringIO file object.
        """
        url = urllib2.urlopen(self.url)
        self.fileobject = self.extract_file(self.filename, 
                                            StringIO.StringIO(url.read()))
    
    def get_file_name(self,i_data_type,i_lmp_type,i_market):
        """This method is used to construct the filename. When only 
        one file exists in the zip archive, the self.filename attr 
        is overridden in self.extract_file().
        """
        # Build a dict of url args because we need startdatetime for filename
        ulist = self.url.split('&')
        udict = {}
        for i in ulist:
            udict[i.split('=')[0]] = i.split('=')[1]
            
        # some files don't break out the MCE, MCC, MLC; treat accordingly
        meta = CaisoLmp.datatype_config()
        if [d['lmp_component_split'] for d in meta if 
            d['atlas_datatype'] == self.datatype][0]:
            return '{0}_{1}_{2}_{3}_{4}_v1.csv'.format(
                udict['startdatetime'][:8]
                ,udict['enddatetime'][:8]
                ,i_data_type
                ,i_market
                ,i_lmp_type)
        else:
            return '{0}_{1}_{2}_{3}_v1.csv'.format(
                udict['startdatetime'][:8]
                ,udict['enddatetime'][:8]
                ,i_data_type
                ,i_market)

    def build_url(self):
        """This method builds the a url for the data source. It relies
        on the following attributes: self.startdate, self.datatype
        """
        meta = CaisoLmp.datatype_config()
        config_dict = [d for d in meta if 
            d['atlas_datatype'] == self.datatype][0]
        url = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname='
        url += '{0}&startdatetime={1}T07:00-0000'.format(
            # add the appropriate xml_name for the datatype
            config_dict['xml_name']
            ,self.startdate.strftime('%Y%m%d'))
        url += '&enddatetime={0}T07:00-0000&market_run_id={1}'.format(
            self.enddate.strftime('%Y%m%d')
            ,config_dict['market'])
        url += '&resultformat=6&version=1'
            
        # if a pnode arg has been supplied in __init__, then add to url
        if self.pnode:
            url += '&node={0}'.format(self.pnode)
        return url
    
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
                'lmp_component_split': False,
                'col_offset':          0,
            },{
                'atlas_datatype':       'RTLMP_RTPD',
                'xml_name':             'PRC_RTPD_LMP',
                'market':               'RTPD',
                'xml_data_items':       [
                                            'LMP_CONG_PRC', 'LMP_ENE_PRC',
                                            'LMP_LOSS_PRC', 'LMP_PRC',
                                            'LMP_GHG_PRC',
                                        ],
                'lmp_component_split':  False,
                'col_offset':           -1,
            },{
                'atlas_datatype':       'DALMP_PRC',
                'xml_name':             'PRC_LMP',
                'market':               'DAM',
                'xml_data_items':       [
                                            'LMP_CONG_PRC', 'LMP_ENE_PRC',
                                            'LMP_LOSS_PRC', 'LMP_PRC',
                                        ],
                'lmp_component_split': True,
                'col_offset':          0,
            }
        ]
        return config
    
    def load_data(self, i_csv_list):
        """This method accepts a list of lists representing the csv
        file and it returns a Pandas DataFrame. 
        """
        meta = CaisoLmp.datatype_config()
        offset = [d['col_offset'] for d in meta 
            if d['atlas_datatype'] == self.datatype][0]
        output = []
        for row in i_csv_list[1:]:
            try:
                d = {
                    'datatype':     self.datatype,
                    'iso':'         CAISO',
                    'node':         row[6 + offset],
                    'node_type':    '',
                    'dt_utc':       datetime.datetime.strptime(
                                        row[0]
                                        ,'%Y-%m-%dT%H:%M:%S-00:00'),
                    'price':        float(row[14 + offset]),
                    'lmp_type':     row[9 + offset],
                }
                output.append(d)
            except Exception, er:
                """No logging implemented, but this is where we 
                would handle errors from failed rows and log it.
                """
                print er
                self.rows_rejected += 1
                pass
        # filter output to LMP type
        output = list(filter(lambda d: d['lmp_type'] in 
            [self.lmp_type], output))
        # sort data by dt_utc and convert to Pandas DataFrame
        self.data = pandas.DataFrame(
            sorted(output, key=lambda k: k['dt_utc']))
        self.rows_accepted = len(self.data)
        return self.data