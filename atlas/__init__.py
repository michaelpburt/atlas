# -*- coding: utf-8 -*-
"""
        atlas.energy
        ~~~~~~~~~~~~~~
        This file provides a base class for handling AtlasCore Energy 
        data collection.
        
        :copyright: © 2018 by Veridex
        :license: MIT, see LICENSE for more details.
"""

import zipfile

import requests
import StringIO
import urllib2


class BaseCollectEvent():
    """This is the Super Class for all collection events."""
    
    def __init__(self, **kwargs):
        pass
        
    def get_file(self):
        """This method generates a GET request on the self.url 
        resource. It returns a StringIO file object. We cannot 
        use requests library on ftp server so we use urllib2 in 
        the case that our url ends with 'ftp'.
        """
        if self.url[0:3] == 'ftp':
            resp = urllib2.urlopen(self.url)
            f = StringIO.StringIO()
            f.write(resp.read())
            f.seek(0)
            self.fileobject = f
        elif self.filename[0:3] == 'zip':
            url = urllib2.urlopen(self.url)
            f = self.extract_file(self.filename, url)
            self.fileobject = f
        else:
            r = requests.get(self.url)
            f = StringIO.StringIO() 
            f.write(r.content)
            f.seek(0)
            self.fileobject = f
    
    def extract_file(self, i_filename, i_filedata):
        """Overrides Superclass method. Open zipfile and return 
        file-like object.
        """
        input_zip=zipfile.ZipFile(i_filedata)
        output = StringIO.StringIO()
        # override filename attr if only one file in archive
        if len(input_zip.namelist()) == 1:
            self.filename = input_zip.namelist()[0]
            output.write(input_zip.read(self.filename))
            output.seek(0)
            return output
        output.write(input_zip.read(i_filename))
        output.seek(0)
        return output
        
    def get_data(self):
        """This method returns a Pandas DataFrame of the data. It 
        executes the entire extract and transform workflow.
        """
        self.get_file()
        csv_list = self.get_csv_list_from_str(self.fileobject.read())
        payload = self.load_data(csv_list)
        del csv_list
        return payload
    
    def get_csv_list_from_str(self, i_csv_str):
        """This method returns a list of lists that represents 
        the csv data.
        """
        csv_list = []
        for x in i_csv_str.split('\n'):
            csv_list.append(x.split(','))
        return csv_list