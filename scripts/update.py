#!/usr/bin/env python

import sys
import os
import glob

import pandas as pd
from tvDatafeed import TvDatafeed, Interval

sys.path.insert(1, '../pkgs/')

from methods import download_market_data
from methods import create_market_leaderboard

tv = TvDatafeed()

# Prompt the user for questionz
name = input("Insert crypto to UPDATE (e.g. vet): ")
pair = input("Insert pair exchange, (tested usd, usdt): ")
timeframe = input("Insert timeframe, (e.g. 1d, 4h, 5m): ")
data_dir = f"../data/tmp"


# ~~~~~~~~ GET DATA FOR THE UPDATE ~~~~~~~~ #
print(f"Downloading data for {name}{pair} in tmp...")
df = download_market_data(name, pair, timeframe, data_dir, tv, Interval)
df = list()


# ~~~~~~~~ UPDATE, i.e. MERGE ~~~~~~~~ #
path_data = f"../data/{timeframe}/{pair}/{name}/"
path_tmp = f"../data/tmp"

flist_data = sorted(glob.glob(f'{path_data}/{name.upper()}{pair.upper()}*')) # takes all filepaths which start with e.g. DOGEUSDT
flist_tmp = sorted(glob.glob(f'{path_tmp}/{name.upper()}{pair.upper()}*'))

markets_data = sorted([ os.path.splitext(os.path.basename(file))[0].split('-')[1] for file in flist_data ])
markets_tmp = sorted([ os.path.splitext(os.path.basename(file))[0].split('-')[1] for file in flist_tmp ])

market_list = sorted(list(set(markets_data).intersection(set(markets_tmp))))

dfo = list(); dfn = list()
for mk in market_list:
    dfo.append(pd.read_csv(f'{path_data}/{name.upper()}{pair.upper()}-{mk}.csv'))
    dfn.append(pd.read_csv(f'{path_tmp}/{name.upper()}{pair.upper()}-{mk}.csv'))
    
print("\n~~ Starting Updating of Market Data ~~")
df = list()
for i, mk in enumerate(market_list):
    df1 = dfo[i].copy()
    df2 = dfn[i].copy()
    
    columns = list(df1.columns)
    
    print(f"\t->Updating {name.upper()}{pair.upper()}-{mk}.csv ...")
    dff = pd.merge(df1, df2, on=columns, how='outer').set_index('datetime')
    dff.to_csv(f'{path_data}/{name.upper()}{pair.upper()}-{mk}.csv')
    df.append(dff)
    
print("\n~~ Leaderboard of Markets and Volumes ~~")
dfv = create_market_leaderboard(df)
print(dfv)
path = f"{path_data}/leaderboard.csv"
dfv.to_csv(path,index=False)