#!/usr/bin/env python3
"""Compute transparent hourly sweep/taker/funding summaries from the source archives."""
from __future__ import annotations
import csv
import math
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "readable_data"
OUT = ROOT / "analysis_out"
OUT.mkdir(exist_ok=True)

summary_rows=[]
funding_summary_rows=[]
monthly_rows=[]
for path in sorted((DATA/'1h_by_symbol').glob('*_1h.csv')):
    symbol=path.name.removesuffix('_1h.csv')
    df=pd.read_csv(path)
    # Source open_time is Unix epoch milliseconds, never infer as nanoseconds.
    df['time']=pd.to_datetime(df['open_time'],unit='ms',utc=True)
    df=df.sort_values('time').reset_index(drop=True)
    for c in ['open','high','low','close','volume','taker_buy_volume']:
        df[c]=pd.to_numeric(df[c],errors='coerce')
    sell=(df['volume']-df['taker_buy_volume']).clip(lower=0)
    df['taker_imb']=(df['taker_buy_volume']-sell)/df['volume'].replace(0,np.nan)
    df['taker_imb_ma24']=df['taker_imb'].rolling(24,min_periods=24).mean()
    df['hh_prev24']=df['high'].rolling(24,min_periods=24).max().shift(1)
    df['ll_prev24']=df['low'].rolling(24,min_periods=24).min().shift(1)
    df['sweep_hi']=(df['high']>df['hh_prev24'])&(df['close']<df['hh_prev24'])
    df['sweep_lo']=(df['low']<df['ll_prev24'])&(df['close']>df['ll_prev24'])
    vmean=df['volume'].rolling(96,min_periods=96).mean()
    vstd=df['volume'].rolling(96,min_periods=96).std()
    df['vol_z']=(df['volume']-vmean)/vstd.replace(0,np.nan)
    for n in (6,24,72):
        df[f'fwd_{n}h']=df['close'].shift(-n)/df['close']-1
    event=df[df['sweep_hi']|df['sweep_lo']].copy()
    event['side']=np.where(event['sweep_hi'],'sweep_hi','sweep_lo')
    event['symbol']=symbol
    cols=['symbol','time','side','open','high','low','close','volume','taker_buy_volume','taker_imb','taker_imb_ma24','vol_z','hh_prev24','ll_prev24','fwd_6h','fwd_24h','fwd_72h']
    event[cols].to_csv(OUT/f'{symbol}_SWEEPS.csv',index=False,float_format='%.10g')
    for side in ['sweep_hi','sweep_lo']:
        e=df[df[side]].copy()
        for horizon in (6,24,72):
            col=f'fwd_{horizon}h'
            valid=e[col].dropna()
            # Directional follow-through definition: after high sweep expect down; after low sweep expect up.
            wins=(valid<0).mean() if side=='sweep_hi' else (valid>0).mean()
            summary_rows.append({'symbol':symbol,'side':side,'horizon_hours':horizon,'event_count':len(e),'evaluated_count':len(valid),'directional_followthrough_fraction':wins,'mean_forward_return':valid.mean(),'median_forward_return':valid.median(),'mean_volume_z':e['vol_z'].mean(),'mean_taker_imbalance':e['taker_imb'].mean()})
    # Funding data and robust event extraction.
    fp=DATA/'funding_by_symbol'/f'{symbol}_funding.csv'
    f=pd.read_csv(fp)
    f['time']=pd.to_datetime(f['funding_time_utc'],format='mixed',utc=True)
    for c in ['funding_rate','mark_price','funding_interval_hours']:
        f[c]=pd.to_numeric(f[c],errors='coerce')
    f=f.sort_values('time').reset_index(drop=True)
    mu=f['funding_rate'].rolling(90,min_periods=90).mean()
    sd=f['funding_rate'].rolling(90,min_periods=90).std().replace(0,np.nan)
    f['funding_z_90']=((f['funding_rate']-mu)/sd)
    f['symbol']=symbol
    extremes=f[f['funding_z_90'].abs()>2].copy()
    extremes[['symbol','time','funding_rate','funding_z_90','mark_price','rate_type','funding_interval_hours']].to_csv(OUT/f'{symbol}_FUNDING_EXTREMES.csv',index=False,float_format='%.10g')
    funding_summary_rows.append({'symbol':symbol,'rows':len(f),'start_utc':f['time'].min().isoformat(),'end_utc':f['time'].max().isoformat(),'mean_funding_rate':f['funding_rate'].mean(),'median_funding_rate':f['funding_rate'].median(),'positive_fraction':(f['funding_rate']>0).mean(),'extreme_abs_z_gt_2_count':len(extremes),'max_abs_funding_z_90':f['funding_z_90'].abs().max()})
    # Monthly metrics. Volatility is annualized from hourly close returns; range is high/low - 1.
    df['ret_1h']=df['close'].pct_change()
    df['month']=df['time'].dt.to_period('M').astype(str)
    for month, g in df.groupby('month',sort=True):
        gr=g['ret_1h'].dropna()
        funding_month=f[f['time'].dt.strftime('%Y-%m')==month]['funding_rate']
        monthly_rows.append({'symbol':symbol,'month_utc':month,'bars':len(g),'first_open_utc':g['time'].min().isoformat(),'last_close_utc':g['time'].max().isoformat(),'monthly_return':g['close'].iloc[-1]/g['open'].iloc[0]-1,'hourly_realized_vol_annualized':gr.std()*math.sqrt(24*365) if len(gr)>1 else np.nan,'month_high':g['high'].max(),'month_low':g['low'].min(),'high_low_range_fraction':g['high'].max()/g['low'].min()-1 if g['low'].min() else np.nan,'mean_hourly_taker_imbalance':g['taker_imb'].mean(),'mean_funding_rate':funding_month.mean(),'max_funding_rate':funding_month.max(),'min_funding_rate':funding_month.min()})

pd.DataFrame(summary_rows).to_csv(OUT/'GRAND_SUMMARY.csv',index=False,float_format='%.10g')
pd.DataFrame(funding_summary_rows).to_csv(OUT/'FUNDING_SUMMARY.csv',index=False,float_format='%.10g')
pd.DataFrame(monthly_rows).to_csv(OUT/'MONTHLY_REGIMES.csv',index=False,float_format='%.10g')

readme='''# Computed hourly/funding feature summaries\n\nComputed from the full supplied hourly and funding CSVs for every symbol in the source dataset. These are exploratory descriptive statistics, not evidence of a profitable strategy. The sweep rules are a simple rolling 24-hour high/low breach-and-close-back-inside definition. Forward returns are evaluated only where the complete horizon exists; directional follow-through is negative return after a high sweep and positive return after a low sweep. No fees, slippage, entry timing, or position sizing are modeled.\n\n## Important corrections to the earlier suggested code\n\n- Hourly source column is `taker_buy_volume`, not `taker_buy_base`.\n- `open_time` is Unix milliseconds: parse using `pd.to_datetime(open_time, unit="ms", utc=True)`. Treating it as default nanoseconds gives incorrect timestamps.\n- A win-rate comparison must exclude rows with missing forward returns (the final 6/24/72 hours).\n- Source time spans differ by symbol due to listing history; inspect each CSV's first/last timestamps.\n\n## Files\n\n- `GRAND_SUMMARY.csv`: sweep counts, evaluated counts, directional follow-through fraction and return statistics per symbol/side/horizon.\n- `FUNDING_SUMMARY.csv`: funding coverage and extreme z-score counts per symbol.\n- `MONTHLY_REGIMES.csv`: monthly returns, annualized hourly realized volatility, high/low range, taker imbalance and funding summaries.\n- `<SYMBOL>_SWEEPS.csv`: individual sweep events and 6/24/72-hour forward returns.\n- `<SYMBOL>_FUNDING_EXTREMES.csv`: funding events whose rolling 90-observation z-score exceeds +/-2.\n'''
(OUT/'README.md').write_text(readme,encoding='utf-8')
print('Symbols analyzed:',len(funding_summary_rows))
print('Hourly bars:',sum(len(pd.read_csv(p,usecols=['open_time'])) for p in (DATA/'1h_by_symbol').glob('*_1h.csv')))
print('Sweep events:',sum(1 for p in OUT.glob('*_SWEEPS.csv') for _ in open(p,encoding='utf-8'))-len(list(OUT.glob('*_SWEEPS.csv'))))
print('Extreme funding events:',sum(1 for p in OUT.glob('*_FUNDING_EXTREMES.csv') for _ in open(p,encoding='utf-8'))-len(list(OUT.glob('*_FUNDING_EXTREMES.csv'))))
print('Output files:',len(list(OUT.glob('*.csv')))+1)
print('\nGrand summary, top rows:')
print(pd.read_csv(OUT/'GRAND_SUMMARY.csv').head(8).to_string(index=False))
