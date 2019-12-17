# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 11:24:30 2019
@author: LuomaKR
Module to generate metrics against comparisons between FMS, AOS, DRRS-A and EMILPO
Depends on global dataframe outputs from main.py including:
    emilpo_uic
    fms_uic
    aos_hduic_templets
    aos_uic
    drrsa_uic
"""

def generate_dq_metrics(emilpo_uic, grouping):
    
    metrics = emilpo_uic[[grouping, "UIC",]].groupby([grouping]).count()
    metrics.rename(columns = {"UIC" : "EMILPO_UIC"}, inplace = True)
    
    #Generate count of EMILPO UICs in DRRSA
    metrics = metrics.join(
            emilpo_uic[[grouping, "IN_DRRSA"]].where(
                    emilpo_uic.IN_DRRSA == True
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"IN_DRRSA" : "EMILPO_UIC_IN_DRRSA"})
            
    #Generate sum of personnel assigned to each command
    metrics = metrics.join(
            emilpo_uic[[grouping, "ASSIGNED"]].groupby(grouping).sum(),
            lsuffix = "_left",
            rsuffix = "_right"
            ).rename(columns = {"ASSIGNED" : "EMILPO_ASSIGNED"})
    
    #Generate sum of personnel assigned to excess slots in each command
    metrics = metrics.join(
            emilpo_uic[[grouping, "EXCESS"]].groupby(grouping).sum(),
            lsuffix = "_left",
            rsuffix = "_right"
            ).rename(columns = {"EXCESS" : "EMILPO_ASSIGNED_EXCESS"})
    
    #Generate count of EMILPO UICs not in DRRSA
    metrics["EMILPO_UIC_NOT_IN_DRRSA"] = (
            metrics.EMILPO_UIC - metrics.EMILPO_UIC_IN_DRRSA)
    
    #Generate percent of EMILPO_UICs registered in DRRSA
    metrics["PERC_EMILPO_UIC_IN_DRRSA"] = (
            metrics.EMILPO_UIC_IN_DRRSA / metrics.EMILPO_UIC)
    
    #Generate sum of personnel assigned to EMILPO UICs not in DRRSA
    metrics = metrics.join(
            emilpo_uic[[grouping, "ASSIGNED"]].where(
                    emilpo_uic.IN_DRRSA == False
                    ).groupby(grouping).sum(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"ASSIGNED" : "EMILPO_ASSIGNED_TO_UIC_NOT_IN_DRRSA"})
    
    #Generate count of EMILPO UICs in AOS
    metrics = metrics.join(
            emilpo_uic[[grouping, "IN_AOS"]].where(
                    emilpo_uic.IN_AOS == True
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"IN_AOS" : "EMILPO_UIC_IN_AOS"})
            
    #Generate count of EMILPO UICs not in AOS
    metrics["EMILPO_UIC_NOT_IN_AOS"] = (
            metrics.EMILPO_UIC - metrics.EMILPO_UIC_IN_AOS)
    
    #Generate percent of EMILPO_UICs present in AOS
    metrics["PERC_EMILPO_UIC_IN_AOS"] = (
            metrics.EMILPO_UIC_IN_AOS / metrics.EMILPO_UIC)
    
    #Generate sum of personnel assigned to EMILPO UICs not in AOS
    metrics = metrics.join(
            emilpo_uic[[grouping, "ASSIGNED"]].where(
                    emilpo_uic.IN_AOS == False
                    ).groupby(grouping).sum(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"ASSIGNED" : "EMILPO_ASSIGNED_TO_UIC_NOT_IN_AOS"})
            
    #Generate count of EMILPO UICs in FMS
    metrics = metrics.join(
            emilpo_uic[[grouping, "IN_FMS_UIC", "IN_FMS_LDUIC"]].where(
                    (emilpo_uic.IN_FMS_UIC == True) | (emilpo_uic.IN_FMS_LDUIC == True)
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"IN_FMS_UIC" : "EMILPO_UIC_IN_FMS"})
    metrics.drop(columns = ['IN_FMS_LDUIC'], inplace = True)
    
    #Generate count of EMILPO UICs not in FMS
    metrics["EMILPO_UIC_NOT_IN_FMS"] = (
            metrics.EMILPO_UIC - metrics.EMILPO_UIC_IN_FMS)
    
    #Generate percent of EMILPO_UICs present in FMS
    metrics["PERC_EMILPO_UIC_IN_FMS"] = (
            metrics.EMILPO_UIC_IN_FMS / metrics.EMILPO_UIC)
    
    return metrics