"""
This file provides a base class for collect events.
"""

import StringIO
import requests
import urllib2

class BaseCollectEvent():
  def __init__(self, **kwargs):
    self.hi = 'hi'
    
  def getFile(self):
    """
    This method generates a GET request on the self.url resource. It returns a 
    StringIO file object.
    
    Cannot use requests library on ftp server so we use urllib2 in the case
    that our url ends with 'ftp'.
    """
    if self.url[0:3] == 'ftp':
      resp = urllib2.urlopen(self.url)
      f = StringIO.StringIO()
      f.write(resp.read())
      f.seek(0)
      self.fileobject = f
    else:
      r = requests.get(self.url)
      f = StringIO.StringIO() 
      f.write(r.content)
      f.seek(0)
      self.fileobject = f