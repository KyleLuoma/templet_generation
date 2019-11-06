# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 12:47:39 2019
@author: LuomaKR
Module to load data as Pandas objects 
Sources include DRRSA UIC file and AOS tree export
"""

import pandas as pd

""" Retrieve the DRRSA UIC / Location file """
def load_drrsa_file():
    return pd.read_csv("./data/drrsa_uic_locations.csv")

""" Retrieve consolidated AOS UIC export file """
def load_aos_file():
    return pd.read_csv("./data/aos_uic_tree_ac_fy21.csv")