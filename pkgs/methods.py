import pandas as pd

def download_market_data(name, pair, timeframe, data_dir, tv, Interval):
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
                n_bars = 10000
            )
            path = f'{data_dir}/{name}/{el_symbol}-{el_market}.csv'
            try:
                data.to_csv(path)
                df.append(data)
            except OSError: # This happens when updating data, thus just save to /data/tmp
                data.to_csv(f'{data_dir}/{el_symbol}-{el_market}.csv')
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