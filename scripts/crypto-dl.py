#!/usr/bin/env python
import os
import pandas as pd
from tvDatafeed import TvDatafeed, Interval # github: https://github.com/rongardF/tvDatafeed

tv = TvDatafeed()

def download_market_data(name, pair, timeframe, data_dir):
    asset = name + pair # e.g. 'vetusd'
    symbs = tv.search_symbol(asset) # not case sensitive
    
    if timeframe == '1d':
        tf_interval = Interval.in_daily
    elif timeframe == '4h':
        tf_interval = Interval.in_4_hour
    elif timeframe == '5m':
        tf_interval = Interval.in_5_minute
    
    df = list()
    for el in symbs:
        el_symbol = el['symbol']
        el_market = el['exchange']
        if asset.upper() == el_symbol: # .upper() makes 'vetusd' -> 'VETUSD', needed because case sensitive
            print(f"\t->Downloading from {el_market}...")
            data = tv.get_hist(
                symbol = el_symbol,
                exchange = el_market,
                interval = tf_interval,
                n_bars = 100000
            )
            path = f'{data_dir}/{name}/{el_symbol}-{el_market}.csv'
            try:
                data.to_csv(path)
                df.append(data)
            except AttributeError: # empty dataframes cannot be saved to csv
                continue
    return df
    
def create_market_leaderboard(df):
    s = list(); e = list(); sy = list(); vol = list()
    for d in df:
        try:
            s.append(d.index[0])
            e.append(d.index[-1])
            sy.append(d.symbol.iloc[0])
            vol.append(d.volume.sum())
        except AttributeError:
            continue
    
    dfv = pd.DataFrame(zip(s, e, sy, vol), columns = ['start','end','symb','vol'])
    dfv = dfv[ dfv.vol != 0. ].sort_values(by='vol', ascending=False).reset_index(drop=True)
    
    return dfv

# Prompt the user for questionz
name = input("Insert crypto to download here (e.g. vet): ")
pair = input("Insert pair exchange, (tested usd, usdt): ")
timeframe = input("Insert timeframe, (e.g. 1d, 4h, 5m): ")
data_dir = f"../data/{timeframe}/{pair}"

# Check if input is valid (contains only letters and is not empty)
if name.isalpha() and len(name) > 0:
    # Create a directory if it doesn't exist
    download_path = os.path.join(data_dir, name)
    print("\nDownload path is:", download_path)
    try:
        os.makedirs(download_path)
        print(f"Created a directory for {name} in {data_dir}")
        
        print(f"Downloading data for {name}{pair}...")
        df = download_market_data(name, pair, timeframe, data_dir)
        
        print("\n~~ Leaderboard of Markets and Volumes ~~")
        dfv = create_market_leaderboard(df)
        path = f"{data_dir}/{name}/leaderboard.csv"
        dfv.to_csv(path)
        print(dfv)
    
    except FileExistsError:
        print(f"Directory for {name} in {data_dir} already exists.")
        print(f"Downloading data for {name}{pair}...")
        df = download_market_data(name, pair, timeframe, data_dir)
        
        print("\n~~ Leaderboard of Markets and Volumes ~~")
        dfv = create_market_leaderboard(df)
        path = f"{data_dir}/{name}/leaderboard.csv"
        dfv.to_csv(path)
        print(dfv)

    except ValueError:
        print("Invalid input. Please enter a valid name (letters only and not empty).")

