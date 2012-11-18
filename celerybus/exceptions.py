'''
Created on Nov 17, 2012

@author: nino
'''

class FlowControl(Exception):
    pass

class AbortProcessing(FlowControl):
    pass