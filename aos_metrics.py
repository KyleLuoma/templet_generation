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

def generate_dq_metrics(emilpo_uic, fms_lduic, aos_uic, grouping):
    
    global metrics
    
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
    
    #Generate count of LDUICs in FMS
    metrics = metrics.join(
            fms_lduic[[grouping, "LDUIC"]].groupby(grouping).count(),
            lsuffix = "_left",
            rsuffix = "_right"
            ).rename(columns = {"LDUIC" : "FMS_LDUIC"})
    
    #Generate count of LDUICs in FMS and in AOS
    metrics = metrics.join(
            fms_lduic[[grouping, "IN_AOS"]].where(
                    fms_lduic.IN_AOS == True
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"IN_AOS" : "FMS_LDUIC_IN_AOS"})
    
    #Generate count of LDUICs in FMS and not in AOS
    metrics["FMS_LDUIC_NOT_IN_AOS"] = (
            metrics.FMS_LDUIC - metrics.FMS_LDUIC_IN_AOS)
    
    #Generate percent of LDUICs in FMS in AOS
    metrics["PERC_FMS_LDUIC_IN_AOS"] = (
            metrics.FMS_LDUIC_IN_AOS / metrics.FMS_LDUIC)
    
    #Generate count of UICs in AOS
    metrics = metrics.join(
            aos_uic[[grouping, "UIC_PUD"]].groupby(grouping).count(),
            lsuffix = "_left",
            rsuffix = "_right"
            ).rename(columns = {"UIC_PUD" : "AOS_UIC"})
    
    #Generate count of expected HSDUICs in AOS
    metrics = metrics.join(
            aos_uic[[grouping, "UIC", "EXPECTED_HDUIC"]].where(
                    aos_uic.EXPECTED_HDUIC != "").dropna().groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"EXPECTED_HDUIC" : "AOS_EXPECTED_HSDUIC"})
    metrics.drop(columns = ['UIC'], inplace = True)

    #Generate count of AOS UICs that have HSDUICs in DRRSA
    metrics = metrics.join(
            aos_uic[[grouping, "HAS_DUIC"]].where(
                    aos_uic.HAS_DUIC == True
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"HAS_DUIC" : "AOS_EXPECTED_HSDUIC_IN_DRRS"})
            
    #Generate count of AOS UICs that expect HSDUICs not registered in DRRSA
    metrics["AOS_EXPECTED_HSDUIC_NOT_IN_DRRS"] = (
            metrics.AOS_EXPECTED_HSDUIC - metrics.AOS_EXPECTED_HSDUIC_IN_DRRS)
    
    #Generate percent of AOS UICs that expect HSDUICs registered in DRRSA
    metrics["PERC_AOS_EXPECTED_HSDUIC_IN_DRRS"] = (
            metrics.AOS_EXPECTED_HSDUIC_IN_DRRS / metrics.AOS_EXPECTED_HSDUIC)
    
    #Generate count of AOS UICs that have HSDUICs in AOS
    metrics = metrics.join(
            aos_uic[[grouping, "HDUIC_IN_AOS"]].where(
                    (aos_uic.EXPECTED_HDUIC != False) & (aos_uic.HDUIC_IN_AOS == True)
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"HDUIC_IN_AOS" : "AOS_EXPECTED_HSDUIC_IN_AOS"})
    
    #Generate count of AOS UICs that do not have HSDUICs in AOS
    metrics["AOS_EXPECTED_HSDUIC_NOT_IN_AOS"] = (
            metrics.AOS_EXPECTED_HSDUIC - metrics.AOS_EXPECTED_HSDUIC_IN_AOS)
    
    #Generate percent of AOS UICs that have HSDUICs in AOS
    metrics["PERC_AOS_EXPECTED_HSDUIC_IN_AOS"] = (
            metrics.AOS_EXPECTED_HSDUIC_IN_AOS / metrics.AOS_EXPECTED_HSDUIC)
    
    #Generate count of AOS UICs that do not require location data fields to be filled
    #Exclude UIC Sub Codes 95, 96, 99 and FF
    metrics = metrics.join(
            aos_uic[[grouping, "LOCATION_NOT_REQ"]].where(
                    (aos_uic.LOCATION_NOT_REQ == True)
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right",
            ).rename(columns = {"LOCATION_NOT_REQ" : "LOC_NOT_REQ_IN_AOS"})

    #Generate count of AOS UICs that require location data fields to be filled
    metrics["LOC_REQ_IN_AOS"] = (metrics.AOS_UIC - metrics.LOC_NOT_REQ_IN_AOS)
    
    #Generate count of AOS UICs that require location data that have complete location data
    metrics = metrics.join(
            aos_uic[[grouping, "LOCATION_NOT_REQ", "LOCATION_DATA_COMPLETE"]].where(
                    (aos_uic.LOCATION_NOT_REQ == False) & (aos_uic.LOCATION_DATA_COMPLETE == True)
            ).groupby(grouping).count().fillna(0.0),
            lsuffix = "_left",
            rsuffix = "_right"
            )
    metrics.drop(columns = ['LOCATION_NOT_REQ'], inplace = True)
    
    #Generate count of AOS UICs that require location data that have incomplete location data
    metrics = metrics.join(
            aos_uic[[grouping, "LOCATION_NOT_REQ"]].where(
                    (aos_uic.LOCATION_NOT_REQ == False) & (aos_uic.LOCATION_DATA_COMPLETE == False)
                    ).groupby(grouping).count(),
                    lsuffix = "_left",
                    rsuffix = "_right"
            ).rename(columns = {"LOCATION_NOT_REQ" : "LOCATION_DATA_INCOMPLETE"})

    #Generate percent completion of location data population
    metrics["PERC_LOCATION_DATA_COMPLETE"] = (
            metrics.LOCATION_DATA_COMPLETE / metrics.LOC_REQ_IN_AOS
            )
    
    metrics.fillna(value = 0, inplace = True)
    
    return metrics
