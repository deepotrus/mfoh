#!/usr/bin/env python

## ~~LIBRARIES~~ ##
import argparse
import pickle
import pandas as pd
import pandas_ta as ta

## ~~METHODS~~ ##
def slice_for_samples(df):
    # Extract samples based on True/False column
    df_list = list()
    new_change = 0
    pre_GC = df.GC.iloc[0]

    for i in range(len(df)):
        row = df.iloc[i]
        if not(pre_GC == row.GC):
            df_list.append(df.iloc[new_change: i])
            pre_GC = df.GC.iloc[i]
            new_change = i
    
    df_list.pop(0) # data before first cross
    df_list.pop(-1) # data after last close
    return df_list

def samples_stats(dfs):
    start = list(); end = list()
    GCs = list()
    pcts = list()
    maxs = list()
    leng = list()

    for d in dfs:
        pcts.append(d.ta.percent_return(cumulative=True).iloc[-1]*100)
        maxs.append(d.ta.percent_return(cumulative=True).max()*100)
        GCs.append(d.GC.iloc[-1])
        start.append(d.index[0])
        end.append(d.index[-1])
        leng.append(len(d))
    
    columns = ['start','end','GC','leng','pct','max']
    return pd.DataFrame(zip(start, end, GCs, leng, pcts, maxs), columns = columns)


## ~~MAIN~~ ##

# Handle input variables
parser = argparse.ArgumentParser()
parser.add_argument('-p', '--pair', help='Pair e.g. usd, usdt', type = str)
parser.add_argument('-tf', '--timeframe', help='e.g. 1d, 4h, 5m', type = str)
parser.add_argument('-k', '--key', help='e.g. btc, eth, doge', type = str)
parser.add_argument('-m', '--market', help='e.g. BINANCE, BITSTAMP', type = str)
parser.add_argument('-sl', '--slow_ma', help='e.g. 200', type = int)
parser.add_argument('-fa', '--fast_ma', help='e.g. 50', type = int)

args = parser.parse_args()

# Load correct mapping dictionary e.g. btc -> BTCUSD
map_path = f"../supp/maps_{args.pair}.pkl"
with open(map_path, 'rb') as pkl_file:
    mapp = pickle.load(pkl_file)

# Load historical prices for a specific market e.g. BINANCE
path = f"../data/{args.timeframe}/usd/{args.key}/"
in_filename = f"{path}/{mapp[args.key]}-{args.market}.csv"
df = pd.read_csv(in_filename).drop(columns = ['symbol']).set_index('datetime')

# Create Simple Moving Average (sma) Cross booleans, GC: golden cross
df['GC'] = df.ta.sma(args.fast_ma) > df.ta.sma(args.slow_ma)

# Extracting trend samples based on GC
dfs = slice_for_samples(df)

# Calculating main stats for these samples
stats = samples_stats(dfs)

# Export to crypto-pair path
ou_filename = f"{path}/trends_{args.timeframe}_{args.fast_ma}-{args.slow_ma}.csv"
stats.to_csv(ou_filename, index=False)
