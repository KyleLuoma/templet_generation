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
    return pd.read_csv(
            "./data/aos_uic_tree_ac_fy21.csv",
            dtype = {
                    "HOGEO" : str,
                    "STACO" : str,
                    "PH_RSDNC_TXT" : str,
                    "PH_STREET_TXT" : str,
                    "PH_STREET_ADDTNL_TXT" : str,
                    "PH_POSTAL_BOX_TXT" : str,
                    "PH_POSTBOX_ID_TXT" : str,
                    "PH_GEO_TXT" : str,
                    "PH_POSTAL_CODE_TXT" : str,
                    "PH_CITY_TXT" : str,
                    "PH_COUNTRY_TXT" : str
                    }
            ).fillna("")

""" Retrieve FMS UIC Rollup with Military Authorizations """
def load_fms_file():
    return pd.read_csv("./data/FY21_AC_UIC_and_SUBCO_UIC_Rollup.csv")

""" Retrieve previous FY FMS UIC Rollup with Military Authorizations"""
def load_prev_fms_file():
    return pd.read_csv("./data/FY20_AC_UIC_and_SUBCO_UIC_Rollup.csv")

""" Retrieve FMS LDUIC Rollup with Military Authorizations """
def load_fms_lduic_file():
    return pd.read_csv("./data/FY21 AC LDUIC Rollup.csv")

""" Retrieve AA / SUBCODE to HD map """
def load_HD_map():
    return pd.read_csv("./data/UIC_HD_MAP.csv")

""" Retrieve EMILPO assignment rollup file """
def load_emilpo():
    return pd.read_csv("./data/emilpo_assigned_excess_01142020.csv")

""" Retrieve AOS UIC OUID Crosswalk """
def load_uic_ouids():
    return pd.read_csv(
            "./data/OUID_UIC_FY21.csv",
            dtype = {"UIC" : str, "OUID" : str}
            )