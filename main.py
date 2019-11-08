# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 13:31:32 2019

@author: LuomaKR

Templet auto generation scripts for AOS
"""

import pandas as pd
import numpy as np
import math
import load_data

TEMPLET_PERCENT = 0.15


def main():
    global aos_uic, drrsa_uic, fms_uic, HD_map, hduic_index, missing_aos_duic
    
    """ Load """
    aos_uic = load_data.load_aos_file()
    drrsa_uic = load_data.load_drrsa_file()
    fms_uic = load_data.load_fms_file()
    HD_map = load_data.load_HD_map()
    
    """ Transform """
    prepare_fms_file()
    prepare_HD_map()
    prepare_aos_uic_file()
    prepare_drrsa_uic_file()
    
    """ Analyze """
    hduic_index = make_drrsa_hduic_index(PUD_ONLY = False)
    missing_aos_duic = aos_drrsa_hduic_check()

"""
Relies on global dataframe fms_uic
Makes necessary transformations of fms export file. File should be exported
from fms personnel detail line report with the following selections:
    Compo = 1
    FY/Status = latest approved FY20 (or 21 when all are published)
    CMD = All
    UIC Name and SubCo Name boxes checked
    Summarize by Compo
    
"""
def prepare_fms_file():
    fms_uic.LOWEST_UIC = ""
    fms_uic.TEMPLET_QTY = 0
    
    """Consolidate UIC or sub code UIC into one column named LOWEST_UIC"""
    for row in fms_uic.itertuples():
        if (pd.isna(row.FULLSUBCO)):
            fms_uic.at[row.Index, 'LOWEST_UIC'] = row.UIC
        else:
            fms_uic.at[row.Index, 'LOWEST_UIC'] = row.FULLSUBCO
        
        """Calculate baseline templet quantity for each row"""
        fms_uic.at[row.Index, 'TEMPLET_QTY'] = math.ceil(row.AUTHMIL * TEMPLET_PERCENT)

""" 
Relies on global dataframe HD_map
HD_map implements HQDA G-3 direction for UIC to HDUIC mapping nomenclature
Creates a UIC -> HSDUIC code map 
"""
def prepare_HD_map():
    HD_map.set_index("UIC", drop = True, inplace = True)

""" 
Relies on global dataframe aos_uic  and HD_map
This file is a composite export of three root nodes in AOS:
    Army commands
    Army Staff
    Secretary of the Army
It is acquired by right clicking on the node in AOS and exporting the UIC tree
for each of these nodes. The files must be merged together in excel and the header
and footer and classification banners must be stripped. Then convert to .csv
"""
def prepare_aos_uic_file():
    aos_uic["UIC_PUD"] = ""
    aos_uic["EXPECTED_HDUIC"] = ""
    
    for row in aos_uic.itertuples():
        aos_uic.at[row.Index, 'UIC_PUD'] = row.UIC[0:4]
        if (HD_map.index.isin([row.UIC[4:6]]).any()):
            aos_uic.at[row.Index, 'EXPECTED_HDUIC'] = row.UIC[0:4] + HD_map.loc[row.UIC[4:6]].HDUIC
        
"""
Relies on global dataframe drrsa_uic
File provided by Jin Kang-Mo 
Export of all UICs for COMPO 1 stored in DRRS-A
"""
def prepare_drrsa_uic_file():
    drrsa_uic["UIC_PUD"] = drrsa_uic.UIC
    
    for row in drrsa_uic.itertuples():
        drrsa_uic.at[row.Index, 'UIC_PUD'] = row.UIC[0:4]

"""
Relies on global dataframe drrsa_uic
Can only be run after running prepare_drrsa_uic_file() function
Args: PUD_ONLY (boolean) to indicate that a four character PUD is desired
Returns: Series consisting of only HSUICs in the DRRS-A file, full HSUIC if PUD_ONLY is False
"""
def make_drrsa_hduic_index(PUD_ONLY):
    if(PUD_ONLY):
        return drrsa_uic.UIC_PUD.where(drrsa_uic.HSUIC_FLAG == "Y").dropna()
    else:
        return drrsa_uic.UIC.where(drrsa_uic.HSUIC_FLAG == "Y").dropna()

"""
Relies on global dataframs aos_uic and series hduic_index
Checks if the expected DUIC in AOS exists in the DRRS-A HDUIC index processed
in the make_drrsa_hduic_index(False) function
Must run after make_drrsa_hduic_index()
Returns: dataframe consisting of the AOS UICs where its HDUIC does not exist in DRRSA
"""
def aos_drrsa_hduic_check():
    aos_uic['HAS_DUIC'] = False
    aos_uic.HAS_DUIC = aos_uic.EXPECTED_HDUIC.isin(hduic_index)
    missing_uics = aos_uic[["UIC", "EXPECTED_HDUIC", "HAS_DUIC"]].where(aos_uic.HAS_DUIC == False).dropna()
    return missing_uics.where(missing_uics.EXPECTED_HDUIC != "").dropna()


if (__name__ == "__main__"): main()











