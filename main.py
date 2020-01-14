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
import utility
import aos_metrics

TEMPLET_PERCENT = 0.15
MIN_TEMPLETS = 3
TIMESTAMP = utility.get_file_timestamp()
EXPORT = True
LOCATION_EXEMPT_SUBCODES = ["95", "96", "99", "FF"]
NON_COMMAND_CODE = "99"
TEMPLET_GEN_RULE = "TEMPLET_QTY" #Options: TEMPLET_QTY, EMILPO_ADJUSTED_TEMPLET_QTY

def main():
    global aos_uic, drrsa_uic, fms_uic, HD_map, hduic_index, missing_aos_duic, \
    fms_uics_not_in_aos, aos_uics_not_in_fms, aos_hduic_templets, templet_rejects, \
    emilpo_uic, templet_short, fms_lduic, lduic_assignment_rollup, dq_metrics, uic_ouid
    
    """ Load """
    aos_uic = load_data.load_aos_file()
    uic_ouid = load_data.load_uic_ouids()
    drrsa_uic = load_data.load_drrsa_file()
    fms_uic = load_data.load_fms_file()
    fms_uic_prev = load_data.load_prev_fms_file()
    fms_lduic = load_data.load_fms_lduic_file()
    HD_map = load_data.load_HD_map()
    emilpo_uic = load_data.load_emilpo()
    
    """ Transform """
    fms_uic = prepare_fms_file(fms_uic, fms_uic_prev)
    prepare_HD_map()
    prepare_uic_ouid_map()
    aos_uic = prepare_aos_uic_file()
    prepare_drrsa_uic_file()
    prepare_emilpo_uic_file()
    
    """ Analyze """
    hduic_index = make_drrsa_hduic_index(PUD_ONLY = False)
    missing_aos_duic = aos_drrsa_hduic_check()
    fms_uics_not_in_aos = fms_uic_not_in_aos()
    fms_lduics_not_in_aos = fms_lduic_not_in_aos()
    aos_uics_not_in_drrsa = aos_uic_not_in_drrsa()
    hduics_not_in_aos = aos_aos_hduic_check()
    aos_uics_not_in_fms = aos_uic_not_in_fms()
    emilpo_uics_not_in_aos_fms_drrsa = emilpo_uic_not_in_aos_fms_drrsa()
    lduic_assignment_rollup = rollup_lduic_assignments()
    aos_hduic_templets = gen_aos_hduic_templets()
    templet_rejects = fms_uic_not_in_templet_file()
    templet_short = emilpo_assigned_delta()
    dq_metrics = aos_metrics.generate_dq_metrics(
            emilpo_uic, 
            fms_lduic, 
            aos_uic,
            "CMD",
            utility.get_local_time_as_string()
            )
    
    """ Export """
    if(EXPORT):
        missing_aos_duic.to_csv("./export/drrsa_duic_not_in_aos"       + TIMESTAMP + ".csv")
        fms_uics_not_in_aos.to_csv("./export/fms_uic_not_in_aos"       + TIMESTAMP + ".csv")
        fms_lduics_not_in_aos.to_csv("./export/fms_lduic_not_in_aos"   + TIMESTAMP + ".csv")
        aos_uics_not_in_drrsa.to_csv("./export/aos_uic_not_in_drrsa"   + TIMESTAMP + ".csv")
        hduics_not_in_aos.to_csv("./export/hduics_not_in_aos"          + TIMESTAMP + ".csv")
        aos_uics_not_in_fms.to_csv("./export/aos_uic_not_in_fms"       + TIMESTAMP + ".csv")
        emilpo_uics_not_in_aos_fms_drrsa.to_csv("./export/emilpo_uic_not_in_aos" + TIMESTAMP + ".csv")
        aos_hduic_templets.to_csv("./export/aos_hduic_templts"         + TIMESTAMP + ".csv")
        templet_rejects.to_csv("./export/templet_reject_report"        + TIMESTAMP + ".csv")
        fms_uic.to_csv("./export/fms_uic"                              + TIMESTAMP + ".csv")
        aos_uic.to_csv("./export/aos_uic"                              + TIMESTAMP + ".csv")
        drrsa_uic.to_csv("./export/drrsa_uic"                          + TIMESTAMP + ".csv")
        emilpo_uic.to_csv("./export/emilpo_uic"                        + TIMESTAMP + ".csv")
        lduic_assignment_rollup.to_csv("./export/lduic_assignments"    + TIMESTAMP + ".csv")
        dq_metrics.to_csv("./export/dq_metrics"                        + TIMESTAMP + ".csv")
        aos_hduic_templets[[
                "EXPECTED_HDUIC", 
                "HAS_DUIC", 
                "UIC", 
                "OUID", 
                "S_DATE", 
                "T_DATE",
                TEMPLET_GEN_RULE
                ]].where(aos_uic.HAS_DUIC == True).dropna().rename(
                    columns = {
                            "EXPECTED_HDUIC" : "HSDUIC",
                            "HAS_DUIC" : "HSDUIC_REGISTERED",
                            "UIC" : "PARENT_UIC",
                            "OUID" : "PARENT_OUID",
                            TEMPLET_GEN_RULE : "STANDARD_EXCESS"
                            }
                ).to_csv(
                "./export/hsduic_templet_generation"                            + TIMESTAMP + ".csv"
                )

"""
Relies on global dataframe fms_uic and fms_lduic
Makes necessary transformations of fms export file. File should be exported
fms_uic from fms personnel detail line report with the following selections:
    Compo = 1
    FY/Status = latest approved FY20 (or 21 when all are published)
    CMD = All
    UIC Name and SubCo Name boxes checked
    Summarize by Compo
fms_lduic from fms personnel detail line report with the following selections:
    Compo = 1
    FY/Status = latest approved FY20 or FY21
    CMD = All
    Compo, UIC Name, LDUIC Name, SubCo Name, Para Title/LDUIC and LDUIC Only checked
    Import into excel and create pivot table with rows in order of:
        CMD, UIC, FULLSUBCO, LDUIC, and SUM(AUTHMIL)
        Tabular format, no subtotals or grand totals and repeat all rows 
NOTE: fms_lduic contains a subset of authorizations that already exist in fms_uic

"""
def prepare_fms_file(fms_uic, fms_uic_prev):
    fms_uic["LOWEST_UIC"] = ""
    fms_uic["UIC_PUD"] = ""
    fms_uic["UIC_SUB"] = ""
    fms_uic.TEMPLET_QTY = 0
    fms_uic_prev.TEMPLET_QTY = 0    
    
    fms_uic = find_lowest_fms_uic(fms_uic)    
    fms_uic_prev = find_lowest_fms_uic(fms_uic_prev)
   
    fms_uic = calculate_fms_templets(fms_uic)
    
    """Append previous FY UICs to curreny FYs if they are not in the current
    FY fms_uic data frame"""
    print("Checking for missing UICs in current FY fms_uic document.")
    add_uic_count = 0
    for row in fms_uic_prev.itertuples():
        if (row.Index not in fms_uic.index.tolist()):
            print("Adding prev FY UIC " + row.Index + " to fms_uic DF.")
            fms_uic = fms_uic.append(fms_uic_prev.loc[row.Index])
            add_uic_count += 1
    print("Completed current and previous fms_uic compare and added " + 
          str(add_uic_count) + " uics from the previous FY.")
    
    error_count = 0
    total_count = 0
    error_auths = 0
    
    for row in fms_uic.itertuples():
        """Add 95, 96 or 99 coded UIC mil auths to AA parent"""
        if (row.UIC_SUB in ('95', '96', '99')):
            parentAA = row.UIC[0:4] + 'AA'
            total_count += 1
            try:
                fms_uic.at[parentAA, 'TEMPLET_QTY'] += (
                        math.ceil(row.AUTHMIL * TEMPLET_PERCENT))
            except Exception as err:
                print("Unable to locate " + parentAA + " with " + 
                      str(row.AUTHMIL) + " auths in FMS file!")
                error_count += 1
                error_auths += row.AUTHMIL
    
    print("Unable to load " + str(error_count) + " of " + str(total_count) + 
          " AUG 95, 96 or 99 coded UICs with a total of " + str(error_auths) + 
          " authorizations resulting in " + str(error_auths * TEMPLET_PERCENT) + 
          " missing templets.")
    
    """ Identifies  lowest level UIC for each LDUIC's parent"""
    fms_lduic.set_index("LDUIC", drop = False, inplace = True)
    fms_lduic["LOWEST_UIC"] = ""    
    for row in fms_lduic.itertuples():
        if (pd.isna(row.FULLSUBCO)):
            fms_lduic.at[row.Index, 'LOWEST_UIC'] = row.UIC
            fms_lduic.at[row.Index, 'FULLSUBCO'] = 'NONE'
        else:
            fms_lduic.at[row.Index, 'LOWEST_UIC'] = row.FULLSUBCO
        
    return fms_uic

"""Consolidate UIC or sub code UIC into one column named LOWEST_UIC in fms_uic file"""
def find_lowest_fms_uic(fms_file):
    for row in fms_file.itertuples():
        fms_file.at[row.Index, 'UIC_PUD'] = row.UIC[0:4]
        fms_file.at[row.Index, 'UIC_SUB'] = row.UIC[4:6]
        
        if (pd.isna(row.FULLSUBCO)):
            fms_file.at[row.Index, 'LOWEST_UIC'] = row.UIC
        else:
            fms_file.at[row.Index, 'LOWEST_UIC'] = row.FULLSUBCO
    fms_file.set_index("LOWEST_UIC", drop = False, inplace = True)
    return fms_file

"""Calculate baseline templet quantity for each row in the fms_uic file"""
def calculate_fms_templets(fms_file):
    for row in fms_file.itertuples():
        fms_file.at[row.Index, 'TEMPLET_QTY'] = max(
                math.ceil(row.AUTHMIL * TEMPLET_PERCENT), MIN_TEMPLETS)
    return fms_file
    
""" 
Relies on global dataframe HD_map
HD_map implements HQDA G-3 direction for UIC to HDUIC mapping nomenclature
Creates a UIC -> HSDUIC code map 
"""
def prepare_HD_map():
    HD_map.set_index("UIC", drop = True, inplace = True)
    
"""
Relies on global dataframe uic_ouid
"""
def prepare_uic_ouid_map():
    uic_ouid.set_index("UIC", drop = False, inplace = True)

""" 
Relies on global dataframe aos_uic, uic_ouid and HD_map
This file is a composite export of three root nodes in AOS:
    Army commands
    Army Staff
    Secretary of the Army
It is acquired by right clicking on the node in AOS and exporting the UIC tree
for each of these nodes. The files must be merged together in excel and the header
and footer and classification banners must be stripped. Then convert to .csv
"""
def prepare_aos_uic_file():
    errors = 0
    has_uic = True
    
    aos_uic.set_index("UIC", drop = False, inplace = True)
    
    aos_uic["UIC_PUD"] = ""
    aos_uic["UIC_SUB"] = ""
    aos_uic["EXPECTED_HDUIC"] = ""
    aos_uic["CMD"] = ""
    aos_uic["LOCATION_NOT_REQ"] = False
    aos_uic["LOCATION_DATA_COMPLETE"] = False
        
    for row in aos_uic.itertuples():
        aos_uic.at[row.Index, 'UIC_PUD'] = row.UIC[0:4]
        aos_uic.at[row.Index, 'UIC_SUB'] = row.UIC[4:6]
        
        if (HD_map.index.isin([row.UIC[4:6]]).any()):
            aos_uic.at[row.Index, 'EXPECTED_HDUIC'] = (row.UIC[0:4] + 
                      HD_map.loc[row.UIC[4:6]].HDUIC)
        
        #Check that ALL required data fields have data:
        aos_uic.at[row.Index, "LOCATION_DATA_COMPLETE"] = not (
                aos_uic.at[row.Index, "HOGEO"] == "" and
                aos_uic.at[row.Index, "STACO"] == "" and
                aos_uic.at[row.Index, "PH_GEO_TXT"] == "" and
                aos_uic.at[row.Index, "PH_POSTAL_CODE_TXT"] == "" and
                aos_uic.at[row.Index, "PH_CITY_TXT"] == "" and
                aos_uic.at[row.Index, "PH_COUNTRY_TXT"] == ""
                )
        
        has_uic = True        
    
        #Join CMD code from FMS to AOS UIC, if available. Replace with 
        #NON_COMMAND_CODE if the AOS UIC is not in FMS
        try:
            aos_uic.at[row.Index, 'CMD'] = str(fms_uic.at[row.Index, 'CMD'])
        except Exception as err:
            errors += 1
            aos_uic.at[row.Index, 'CMD'] = NON_COMMAND_CODE
            has_uic = False
        
        try:
            aos_uic.at[row.Index, 'CMD'] = str(fms_lduic.at[row.Index, 'CMD'])
        except Exception as err:
            errors += 1
            if (not has_uic):
                aos_uic.at[row.Index, 'CMD'] = NON_COMMAND_CODE
                
        if (row.UIC[4:6] in LOCATION_EXEMPT_SUBCODES):
            aos_uic.at[row.Index, 'LOCATION_NOT_REQ'] = True
    
    #aos_uic["LOCATION_NOT_REQ"] = aos_uic.UIC_SUB.isin(LOCATION_EXEMPT_SUBCODES)

    print("Counted " + str(errors) + " exceptions while mapping CMD to AOS UIC.")
    
    return aos_uic.join(
            uic_ouid[["OUID"]],
            lsuffix = "_left",
            rsuffix = "_right"
            )
        
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
Relies on global dataframe emilpo_uic
This file is a result of the emilpo assignment data etl sql v6 from
MAJ Luoma
"""
def prepare_emilpo_uic_file():
    emilpo_uic.ASSIGNED.fillna(0, inplace = True)   
    emilpo_uic.IN_AUTH.fillna(0, inplace = True)
    emilpo_uic.EXCESS.fillna(0, inplace = True)
    emilpo_uic.set_index("UIC", drop = False, inplace = True)   
        

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
    missing_uics = aos_uic[["UIC", "EXPECTED_HDUIC", "HAS_DUIC"]].where(
            aos_uic.HAS_DUIC == False).dropna()
    return missing_uics.where(missing_uics.EXPECTED_HDUIC != "").dropna()

"""
Relies on global dataframe aos_uic
Checks if the expected DUIC in AOS also exists as a UIC in AOS
Returns: dataframe consisting of the AOS UICs that do not have HSDUICs in AOS
"""
def aos_aos_hduic_check():
    aos_uic['HDUIC_IN_AOS'] = False
    aos_uic.HDUIC_IN_AOS = aos_uic.EXPECTED_HDUIC.isin(aos_uic.index)
    return aos_uic.where(aos_uic.HDUIC_IN_AOS == False).dropna()

"""
Relies on aos_uic and fms_uic global dataframes
Adds a Series to fms_uic DF of UICs in FMS that are not in AOS and returns
a dataframe report of fms uics not in aos
"""
def fms_uic_not_in_aos():
    fms_uic['IN_AOS'] = False
    fms_uic.IN_AOS = fms_uic.LOWEST_UIC.isin(aos_uic.UIC)
    return fms_uic[["LOWEST_UIC", "COMPO", "CMD", "UNITNAME", "AUTHMIL"]].where(
            fms_uic.IN_AOS == False).dropna()

"""
Relies on aos_uic and fms_lduic global dataframes
Adds a Series to fms_lduic DF of LDUICs in FMS that are not in AOS and returns
a dataframe report of fms LDUICs not in AOS
"""
def fms_lduic_not_in_aos():
    fms_lduic['IN_AOS'] = False
    fms_lduic.IN_AOS = fms_lduic.index.isin(aos_uic.UIC)
    return fms_lduic.where(
            fms_lduic.IN_AOS == False).dropna()

"""
Relies on fms_uic and aos_uic global dataframs
Adds a series to aos_uic DF of UICs in AOS that are not in AOS and returns
a dataframe report of aos uics not in fms
"""
def aos_uic_not_in_fms():
    aos_uic['IN_FMS'] = False
    aos_uic.IN_FMS = (
            aos_uic.UIC.isin(fms_uic.LOWEST_UIC) | 
            aos_uic.UIC.isin(fms_uic.UIC)
            )
    return aos_uic[["UIC", "DEPT_NAME", "SHORT_NAME"]].where(
            aos_uic.IN_FMS == False).dropna()
"""
Relies on aos_uic and drrsa_uic global dataframes
Addes a series to aos_uic DF of UICs in AOS that are not in DRRS-A and
returns a dataframe report of aos UICs not in DRRS-A
"""
def aos_uic_not_in_drrsa():
    aos_uic['IN_DRRSA'] = False
    aos_uic.IN_DRRSA = aos_uic.UIC.isin(drrsa_uic.UIC)
    return aos_uic[["UIC", "DEPT_NAME", ]].where(
            aos_uic.IN_DRRSA == False).dropna()
    
"""
Relies on aos_uic, fms_lduic and emilpo_uic global dataframes
Addes a series to emilpo_uic DF of UICs in emilpo not in AOS and returns
a dataframe report of emilpo UICs not in AOS

"""
def emilpo_uic_not_in_aos_fms_drrsa():
    emilpo_uic['IN_AOS'] = False
    emilpo_uic['IN_FMS_UIC'] = False
    emilpo_uic['IN_FMS_LDUIC'] = False
    emilpo_uic['IN_DRRSA'] = False      
    
    emilpo_uic.IN_AOS = emilpo_uic.UIC.isin(aos_uic.UIC)
    emilpo_uic.IN_FMS_UIC = emilpo_uic.UIC.isin(fms_uic.LOWEST_UIC)
    emilpo_uic.IN_FMS_LDUIC = emilpo_uic.UIC.isin(fms_lduic.LDUIC)
    emilpo_uic.IN_DRRSA = emilpo_uic.UIC.isin(drrsa_uic.UIC)
    
    return emilpo_uic[["UIC", "ASSIGNED", "IN_AUTH", "EXCESS"]].where(
            emilpo_uic.IN_AOS == False).dropna()
    
"""
After processing templet generation, checks the output of templet generation
to see if there are UICs missing from the templet generation file.
"""    
def fms_uic_not_in_templet_file():
    fms_uic["TEMPLETS_GENERATED"] = False
    fms_uic.TEMPLETS_GENERATED = fms_uic.LOWEST_UIC.isin(aos_hduic_templets.UIC)
    return fms_uic.where(fms_uic.TEMPLETS_GENERATED == False).dropna()

"""
After loading both emilpo_uic and fms_lduics, creates a df with rollup of
assignments to FMS LDUICs using emilpo_uic assignment data
Relies on emilpo_uic and fms_lduic dataframes
Must be run after emilpo_uic_not_in_aos_fms_drrsa() function
"""
def rollup_lduic_assignments():
    lduic_assignment_rollup = fms_uic[["LOWEST_UIC", "CMD"]].copy(deep = True)
    lduic_assignment_rollup.set_index("LOWEST_UIC", drop = False, inplace = True)
    lduic_assignment_rollup["LDUIC_AUTH"] = 0
    lduic_assignment_rollup["LDUIC_ASSIGNED_TOT"] = 0
    lduic_assignment_rollup["LDUIC_ASSIGNED_AUTH"] = 0
    lduic_assignment_rollup["LDUIC_ASSIGNED_EXCESS"] = 0
    lduic_assignment_rollup["LDUIC_PARENT_IN_AOS"] = False
    lduic_assignment_rollup["LDUIC_PARENT_IN_FMS"] = False
    
    assigned_sum = 0
    
    for row in fms_lduic.itertuples():
        if (row.Index in emilpo_uic.UIC.tolist() and 
                row.LOWEST_UIC in lduic_assignment_rollup.LOWEST_UIC.tolist()):
            try:
                assigned_sum += emilpo_uic.loc[row.Index].ASSIGNED
                lduic_assignment_rollup.at[row.LOWEST_UIC, "LDUIC_ASSIGNED_TOT"] += (
                        emilpo_uic.loc[row.Index].ASSIGNED)
                lduic_assignment_rollup.at[row.LOWEST_UIC, "LDUIC_ASSIGNED_AUTH"] += (
                        emilpo_uic.loc[row.Index].IN_AUTH)
                lduic_assignment_rollup.at[row.LOWEST_UIC, "LDUIC_ASSIGNED_EXCESS"] += (
                        emilpo_uic.loc[row.Index].EXCESS)
            except:
                print("Encountered exception in rollup_lduic_assignments function")
        
    for row in lduic_assignment_rollup.itertuples():
        try:
            lduic_assignment_rollup.at[row.Index, "LDUIC_PARENT_IN_AOS"] = (
                    row.LOWEST_UIC in aos_uic.UIC.tolist())
            lduic_assignment_rollup.at[row.Index, "LDUIC_PARENT_IN_FMS"] = (
                    row.LOWEST_UIC in fms_uic.LOWEST_UIC.tolist())
        except:
            print("Encountered exception in lduic assignment rollup.")
    
    print("rollup_lduic_assignmen sum = : " + str(assigned_sum))
    return lduic_assignment_rollup

"""
Create a dataframe report of hduics and templets to add to aos
Relies on aos_uic, fms_uic and uic_ouid data frames
"""
def gen_aos_hduic_templets():
    debug_uic = "WG2CA0" #### FOR UIC SMOKE TEST ###
    is_debug_uic = False
    
    aos_hduic_templets = aos_uic[["UIC", "OUID", "S_DATE", "T_DATE", "UIC_PUD", 
                                  "UIC_SUB", "EXPECTED_HDUIC", 
                                  "DEPT_NAME", "SHORT_NAME", "HAS_DUIC", 
                                  "IN_FMS"]].where(
            aos_uic.EXPECTED_HDUIC != "")
    aos_hduic_templets["AUTH_MIL"] = 0
    aos_hduic_templets["TEMPLET_QTY"] = 0
    aos_hduic_templets.set_index("UIC", drop = False, inplace = True)
    
    if (debug_uic in aos_hduic_templets.UIC.tolist()):
        print(debug_uic + " is in aos_hduic_templets dataframe")
    else:
        print(debug_uic + " not in aos_hduic_templets dataframe")
    
    if (EXPORT): aos_hduic_templets.to_csv("./export/diagnosis_aoshduictemplets" + TIMESTAMP + ".csv")
    
    fms_auths_templets = fms_uic[["LOWEST_UIC", "AUTHMIL", "TEMPLET_QTY", "IN_AOS"]]
    fms_auths_templets.set_index("LOWEST_UIC", drop = False, inplace = True)
    
    if (debug_uic in fms_auths_templets.LOWEST_UIC.tolist()):
        print(debug_uic + " is in fms_auths_templets")
    else:
        print(debug_uic + " not in fms_auths_templets")
    
    if (EXPORT): fms_auths_templets.to_csv("./export/diagnosis_fmsauthstemplets" + TIMESTAMP + ".csv")   
    
    for row in aos_hduic_templets.itertuples():
        #if (not pd.isna(row.UIC) and row.UIC in fms_auths_templets.index.tolist()):
        if (row.Index == debug_uic): 
            print("Iterating over " + debug_uic) 
            is_debug_uic = True

        try:
            if (fms_auths_templets.AUTHMIL.loc[row.Index] > 0):
                aos_hduic_templets.at[row.Index, "AUTH_MIL"] = (
                        fms_auths_templets.loc[row.UIC].AUTHMIL)
                aos_hduic_templets.at[row.Index, "TEMPLET_QTY"] = (
                        fms_auths_templets.loc[row.UIC].TEMPLET_QTY)
                if (is_debug_uic): print(debug_uic + " passed fms auths > 0 check")
        except:
            if (is_debug_uic): 
                print("WG2CA0 Failed isna and infms check")
                is_debug_uic = False
        
    if (debug_uic in aos_hduic_templets.UIC.tolist()):
        print (debug_uic + " is in aos_hduic_templets dataframe")
    else:
        print (debug_uic + " not in aos_hduic_templets datframe")
    
    print("Finished generating templets\n" +
          "AUTHs in FMS file: " + str(fms_uic.AUTHMIL.sum()) + "\n" +
          "AUTHS in TMP file: " + str(aos_hduic_templets.AUTH_MIL.sum()) + "\n" +
          "            Delta: " + str(fms_uic.AUTHMIL.sum() - aos_hduic_templets.AUTH_MIL.sum()))

    #return aos_hduic_templets.where(aos_hduic_templets.AUTH_MIL > 0).dropna()
    return aos_hduic_templets.where(
            aos_hduic_templets.EXPECTED_HDUIC.isin(uic_ouid.index) == False
            ).dropna()

"""
Relies on aos_hduic_templets and emilpo_uic dataframes
Compares aos_hduic_templets + auths to determine if some units have overstrength
in excess of the designated percentage
Adds column to aos_hduic_templets with emilpo assigned, assigned delta, assigned to auth and excess
"""
def emilpo_assigned_delta():
    print("This is where I am going to compare emilpo assigned to auth + templets")
    
    aos_hduic_templets["UIC_IN_EMILPO"] = False
    
    aos_hduic_templets["EMILPO_ASGD_UIC"] = 0
    aos_hduic_templets["EMILPO_ASGD_LDUIC"] = 0
    aos_hduic_templets["EMILPO_ASGD_TOT"] = 0
    
    emilpo_asgd_uic = 0
    emilpo_asgd_lduic = 0
    emilpo_asgd_tot = 0
    
    aos_hduic_templets["EMILPO_ASGD_AUTHS_UIC"] = 0
    aos_hduic_templets["EMILPO_ASGD_AUTHS_LDUIC"] = 0
    aos_hduic_templets["EMILPO_ASGD_AUTHS_TOT"] = 0
    
    emilpo_asgd_auths_uic = 0
    emilpo_asgd_auths_lduic = 0
    emilpo_asgd_auths_tot = 0
    
    aos_hduic_templets["EMILPO_ASGD_EXCESS_UIC"] = 0
    aos_hduic_templets["EMILPO_ASGD_EXCESS_LDUIC"] = 0
    aos_hduic_templets["EMILPO_ASGD_EXCESS_TOT"] = 0
    
    emilpo_asgd_excess_uic = 0
    emilpo_asgd_excess_lduic = 0
    emilpo_asgd_excess_tot = 0
    
    aos_hduic_templets["CALCULATED_EXCESS"] = 0
    aos_hduic_templets["DELTA_TEMPLET_CALCULATED_EXCESS"] = 0
    aos_hduic_templets["DELTA_TEMPLET_EMILPO_EXCESS"] = 0
    aos_hduic_templets["EMILPO_ADJUSTED_TEMPLET_QTY"] = aos_hduic_templets.TEMPLET_QTY
    
    aos_hduic_templets["QTY_METHOD"] = "AUTHS_PERC"
    
    errors = 0
    
    for row in aos_hduic_templets.itertuples():
        aos_hduic_templets.at[row.UIC, "UIC_IN_EMILPO"] = (
                row.UIC in emilpo_uic.UIC.tolist())
        
        try:
            emilpo_asgd_uic = emilpo_uic.loc[row.UIC].ASSIGNED
            emilpo_asgd_lduic = lduic_assignment_rollup.loc[row.UIC].LDUIC_ASSIGNED_TOT
            emilpo_asgd_tot = emilpo_asgd_uic + emilpo_asgd_lduic
            
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_UIC"] = emilpo_asgd_uic
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_LDUIC"] = emilpo_asgd_lduic
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_TOT"] = emilpo_asgd_tot
            
            emilpo_asgd_auths_uic = emilpo_uic.loc[row.UIC].IN_AUTH
            emilpo_asgd_auths_lduic = lduic_assignment_rollup.loc[row.UIC].LDUIC_ASSIGNED_AUTH
            emilpo_asgd_auths_tot = emilpo_asgd_auths_uic + emilpo_asgd_auths_lduic
            
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_AUTHS_UIC"] = emilpo_asgd_auths_uic
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_AUTHS_LDUIC"] = emilpo_asgd_auths_lduic
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_AUTHS_TOT"] = emilpo_asgd_auths_tot
            
            emilpo_asgd_excess_uic = emilpo_uic.loc[row.UIC].EXCESS
            emilpo_asgd_excess_lduic = lduic_assignment_rollup.loc[row.UIC].LDUIC_ASSIGNED_EXCESS
            emilpo_asgd_excess_tot = emilpo_asgd_excess_uic + emilpo_asgd_excess_lduic
            
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_EXCESS_UIC"] = emilpo_asgd_excess_uic
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_EXCESS_LDUIC"] = emilpo_asgd_excess_lduic
            aos_hduic_templets.at[row.Index, "EMILPO_ASGD_EXCESS_TOT"] = emilpo_asgd_excess_tot
            
            aos_hduic_templets.at[row.Index, "CALCULATED_EXCESS"] = (
                    emilpo_asgd_tot - row.AUTH_MIL)
            
            aos_hduic_templets.at[row.Index, "DELTA_TEMPLET_CALCULATED_EXCESS"] = max(
                    (emilpo_asgd_tot - (row.AUTH_MIL + row.TEMPLET_QTY), 0))
            
            aos_hduic_templets.at[row.Index, "DELTA_TEMPLET_EMILPO_EXCESS"] = max(
                    emilpo_asgd_excess_tot - row.TEMPLET_QTY, 0)
            
            aos_hduic_templets.at[row.Index, "EMILPO_ADJUSTED_TEMPLET_QTY"] = max(
                    aos_hduic_templets.at[row.Index, "TEMPLET_QTY"],
                    emilpo_asgd_tot - row.AUTH_MIL,
                    emilpo_asgd_excess_tot)
            
            if (
                    ((emilpo_asgd_tot - row.AUTH_MIL) > aos_hduic_templets.at[row.Index, "TEMPLET_QTY"])
                    or
                    (emilpo_asgd_excess_tot > aos_hduic_templets.at[row.Index, "TEMPLET_QTY"])
                    ):
                aos_hduic_templets.at[row.Index, "QTY_METHOD"] = "ASGD_OVER"
                    
        except Exception as err:
            errors += 1
            #print("Error encountered when searching for UIC in emilpo assignment file: ", row.UIC, err)
    
    print("emilpo_assigned_delta generated " + str(errors) + 
          " errors where a UIC in AOS was not found in EMilpo")
    

    
    
if (__name__ == "__main__"): main()











