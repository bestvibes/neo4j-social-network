from global_vars import *

class Networks(dict):
    def __init__(self, **input_networks):
        for network in networks:
            self[network] = no_network_value
        for network, value in input_networks.iteritems():
            if network in self.keys():
                self[network] = value
    def __setitem__(self, key, value):
        if key in networks:
            dict.__setitem__(self, key, value)