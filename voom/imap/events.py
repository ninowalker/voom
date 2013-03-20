'''
Created on Mar 20, 2013

@author: nino
'''
from voom.events.base import Event

IMAPMessageDownloaded = Event.new("IMAPMessageDownloaded", "num mime_message connection")
