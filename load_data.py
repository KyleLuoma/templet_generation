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
    return pd.read_csv("./data/drrsa/DRRSA_Data_20200114.csv")

""" Retrieve seperate AOS UIC export files and consolidate """
def load_aos_file():
    return (
            pd.read_excel(
            "./data/uic_trees/W00EFF C2 UIC TREE 4-6-2021.xlsx",
            header = 2,
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
                    },
            skipfooter = 1            
            ).fillna("").append(
            pd.read_excel(
            "./data/uic_trees/WARCFF C2 UIC TREE 4-6-2021.xlsx",
            header = 2,
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
                    },
            skipfooter = 1
            ).fillna("")).append(
            pd.read_excel(
            "./data/uic_trees/WSTAFF C2 UIC TREE 4-6-2021.xlsx",
            header = 2,
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
                    },
            skipfooter = 1
            ).fillna("")
            )
    )

""" Retrieve FMS UIC Rollup with Military Authorizations """
def load_fms_file():
    return pd.read_csv("./data/fms/FY21_AC_UIC_and_SUBCO_UIC_Rollup.csv").append(
            pd.read_csv("./data/fms/FY21_RC_UIC_and_SUBCO_UIC_Rollup.csv")
            )

""" Retrieve previous FY FMS UIC Rollup with Military Authorizations"""
def load_prev_fms_file():
    return pd.read_csv("./data/fms/FY20_AC_UIC_and_SUBCO_UIC_Rollup.csv").append(
            pd.read_csv("./data/fms/FY20_RC_UIC_and_SUBCO_UIC_Rollup.csv")
            )

""" Retrieve FMS LDUIC Rollup with Military Authorizations """
def load_fms_lduic_file():
    return pd.read_csv("./data/fms/FY21 AC LDUIC Rollup.csv").append(
            pd.read_csv("./data/fms/FY21 RC LDUIC Rollup.csv")
            )

""" Retrieve AA / SUBCODE to HD map """
def load_HD_map():
    return pd.read_csv("./data/maps_xwalks/UIC_HD_MAP.csv")

""" Retrieve EMILPO assignment rollup file """
#UIC,PARENT_UIC,CMD,IN_AUTH,ASSIGNED,EXCESS
def load_emilpo():
    emilpo = pd.read_csv("./data/emilpo/EMILPO_ASSIGNMENTS_3-3-20.csv")
    emilpo_rollup = (
            emilpo[["UIC_CD", "PARENT_UIC_CD", "STRUC_CMD_CD", 
                    "SSN_MASK_HASH", "PARNO", "MIL_POSN_RPT_NR"]]
            .groupby(["UIC_CD", "PARENT_UIC_CD", "STRUC_CMD_CD"])
            .count()
            .reset_index()
            .rename(columns = {
                    "UIC_CD" : "UIC",
                    "SSN_MASK_HASH" : "ASSIGNED",
                    "PARNO" : "IN_AUTH",
                    "MIL_POSN_RPT_NR" : "EXCESS",
                    "PARENT_UIC_CD" : "PARENT_UIC",
                    "STRUC_CMD_CD" : "CMD"
                    }
        )
    )
    
    return emilpo_rollup.append(
            load_rcms("RAVALA")
            )
    
def load_rcms(file_format = "RAVALA"):
    file = "./data/rcms/rcms_assigned_excess_01302020.csv"
    rcms = pd.read_csv(file)
    if(str.upper(file_format) == "RAVALA"):
        rcms = rcms[["UIC", "Unit Assigned Strength", "Excess"]].rename(
                columns = {
                            "Unit Assigned Strength" : "ASSIGNED",
                            "Excess" : "EXCESS"
                        }
                )
        rcms["CMD"] = "AR"
        rcms["EXCESS"] = rcms.apply(
                    lambda row: row["EXCESS"] if row["EXCESS"] >= 0 else 0,
                    axis = 1
                )
        rcms["IN_AUTH"] = rcms["ASSIGNED"] - rcms["EXCESS"]
    return rcms



""" Retrieve AOS UIC OUID Crosswalk """
def load_uic_ouids():
    return pd.read_csv(
            "./data/maps_xwalks/OUID_UIC_FY21.csv",
            dtype = {"UIC" : str, "OUID" : str}
            )