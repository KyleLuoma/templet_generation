# -*- coding: utf-8 -*-
"""
Created on Tue Nov  5 13:31:32 2019

@author: LuomaKR

Templet auto generation scripts for AOS
"""

import pandas as pd
import load_data

def main():
    global aos_uic, drrsa_uic
    aos_uic = load_data.load_aos_file()
    drrsa_uic = load_data.load_drrsa_file()

if (__name__ == "__main__"): main()

