#!/usr/bin/env python3
"""Download and audit official Binance USDⓈ-M Futures market data only.

Requested window: [2024-10-19T00:00:00Z, 2026-10-19T00:00:00Z).
No strategy, signals, or synthetic bars are created by this program.
"""
from __future__ import annotations
import argparse, csv, gzip, hashlib, io, json, math, os, random, re, shutil, sys, time, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parent
START=pd.Timestamp('2024-10-19T00:00:00Z')
END=pd.Timestamp('2026-10-19T00:00:00Z')
START_MS=int(START.timestamp()*1000); END_MS=int(END.timestamp()*1000)
SYMBOLS='BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT ADAUSDT DOGEUSDT LINKUSDT LTCUSDT DOTUSDT AVAXUSDT TRXUSDT BCHUSDT NEARUSDT APTUSDT ARBUSDT OPUSDT SUIUSDT INJUSDT FILUSDT'.split()
INTERVALS={'1m':60_000,'5m':300_000,'1h':3_600_000}
KLINE_HEADERS=['open_time','open','high','low','close','volume','close_time','quote_volume','count','taker_buy_volume','taker_buy_quote_volume','ignore']
DATA_BASE='https://data.binance.vision/data/futures/um'
API_BASE='https://fapi.binance.com'
SOURCE_LOG=ROOT/'source_download_log.csv'


def now_utc(): return datetime.now(timezone.utc).isoformat()
def rel(p:Path):
    try:return str(p.relative_to(ROOT))
    except ValueError:return str(p)

def request_bytes(url, timeout=90, retries=5):
    last=None
    for attempt in range(retries):
        try:
            req=Request(url,headers={'User-Agent':'Mozilla/5.0 Manus data audit contact: public-data research'})
            with urlopen(req,timeout=timeout) as resp:
                b=resp.read()
                return resp.status,b,dict(resp.headers)
        except HTTPError as e:
            if e.code==404:return 404,b'',dict(e.headers or {})
            last=e
            if e.code not in (408,425,429,500,502,503,504):break
            retry_after=float(e.headers.get('Retry-After','0') or 0) if e.headers else 0
            time.sleep(max(retry_after,min(30,0.7*(2**attempt))+random.random()*0.3))
        except Exception as e:
            last=e; time.sleep(min(30,0.7*(2**attempt))+random.random()*0.3)
    raise RuntimeError(f'GET failed after {retries} attempts: {url}: {last!r}')

def get_json(url, timeout=60):
    status,b,h=request_bytes(url,timeout=timeout)
    if status!=200:raise RuntimeError(f'HTTP {status}: {url}')
    return json.loads(b.decode('utf-8')),b,h

def append_source_logs(rows):
    if not rows:return
    fields=['logged_utc','target_file','data_type','symbol','interval','period','source_url','checksum_url','http_status','source_bytes','source_sha256','checksum_status','checksum_expected_sha256','records_extracted','download_status','notes']
    exists=SOURCE_LOG.exists()
    with SOURCE_LOG.open('a',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore')
        if not exists:w.writeheader()
        for r in rows:
            rr={k:r.get(k,'') for k in fields};rr['logged_utc']=rr.get('logged_utc') or now_utc();w.writerow(rr)

def checksum_for(url, payload):
    check_url=url+'.CHECKSUM'
    try:
        status,b,_=request_bytes(check_url,timeout=40,retries=3)
    except Exception as e:
        return hashlib.sha256(payload).hexdigest(),'checksum_unavailable','',check_url,str(e)
    got=hashlib.sha256(payload).hexdigest()
    if status!=200:
        return got,'checksum_unavailable','',check_url,f'checksum HTTP {status}'
    text=b.decode('utf-8','replace')
    m=re.search(r'(?i)\b([0-9a-f]{64})\b',text)
    if not m:return got,'checksum_format_unrecognized','',check_url,text[:200]
    expected=m.group(1).lower()
    if got.lower()!=expected:
        raise RuntimeError(f'SHA256 mismatch for {url}: got {got}, expected {expected}')
    return got,'source_checksum_verified',expected,check_url,''

def month_starts():
    cur=START.normalize().replace(day=1)
    last=(END-pd.Timedelta(nanoseconds=1)).normalize().replace(day=1)
    out=[]
    while cur<=last:
        out.append(cur);cur=(cur+pd.offsets.MonthBegin(1))
    return out

def month_window(month_start, step_ms):
    a=max(START,month_start)
    b=min(END,month_start+pd.offsets.MonthBegin(1))
    return int(a.timestamp()*1000),int(b.timestamp()*1000)

def expected_count(a_ms,b_ms,step_ms):return max(0,(b_ms-a_ms)//step_ms)

def month_sources(symbol,interval,month_start):
    ym=month_start.strftime('%Y-%m')
    a_ms,b_ms=month_window(month_start,INTERVALS[interval])
    final_month=month_start.strftime('%Y-%m')=='2026-10'
    if not final_month:
        name=f'{symbol}-{interval}-{ym}.zip'
        return [f'{DATA_BASE}/monthly/klines/{symbol}/{interval}/{name}']
    # Requested end is 2026-10-19 00:00 UTC; use completed daily files through Oct 18.
    out=[];day=pd.Timestamp('2026-10-01T00:00:00Z')
    while day<END:
        name=f'{symbol}-{interval}-{day:%Y-%m-%d}.zip'
        out.append(f'{DATA_BASE}/daily/klines/{symbol}/{interval}/{name}')
        day+=pd.Timedelta(days=1)
    return out

def read_kline_archive(payload, writer, requested_start_ms, requested_end_ms, header_expected=None):
    z=zipfile.ZipFile(io.BytesIO(payload))
    names=[n for n in z.namelist() if n.lower().endswith('.csv')]
    if not names:raise RuntimeError('ZIP contains no CSV')
    total=0;first=None;last=None;headers=None;previous=None;out_of_order=0;duplicate=0
    with z.open(names[0]) as raw:
        text=io.TextIOWrapper(raw,encoding='utf-8',newline='')
        reader=csv.reader(text)
        try:row=next(reader)
        except StopIteration:return {'rows':0,'headers':KLINE_HEADERS,'first':None,'last':None,'order_errors':0,'duplicates':0}
        if row and row[0].strip().lower()=='open_time':headers=row
        else:
            headers=header_expected or KLINE_HEADERS
            data_iter=iter([row]);reader_iter=iter(reader)
            def chain():
                yield from data_iter
                yield from reader_iter
            reader=chain()
        if len(headers)!=12:raise RuntimeError(f'Unexpected Kline header width={len(headers)}: {headers}')
        if header_expected is not None and headers!=header_expected:raise RuntimeError(f'Header drift: {headers} != {header_expected}')
        for row in reader:
            if not row:continue
            if len(row)!=12:raise RuntimeError(f'Unexpected row width={len(row)}')
            try:ts=int(row[0])
            except Exception:raise RuntimeError(f'Invalid open_time: {row[0]!r}')
            if requested_start_ms<=ts<requested_end_ms:
                if previous is not None:
                    if ts<previous:out_of_order+=1
                    if ts==previous:duplicate+=1
                previous=ts;writer.writerow(row);total+=1
                if first is None:first=ts
                last=ts
    return {'rows':total,'headers':headers,'first':first,'last':last,'order_errors':out_of_order,'duplicates':duplicate}

def download_kline_month(task):
    symbol,interval,month_start=task
    ym=month_start.strftime('%Y-%m');step=INTERVALS[interval]
    a_ms,b_ms=month_window(month_start,step)
    target=ROOT/'market_data'/'klines'/interval/symbol/f'{symbol}_{interval}_{ym}.csv.gz'
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists() and target.stat().st_size>0:
        return {'status':'skipped_existing','target':target,'symbol':symbol,'interval':interval,'month':ym,'rows':None,'logs':[]}
    urls=month_sources(symbol,interval,month_start)
    temp=target.with_suffix(target.suffix+'.part')
    total=0;headers=None;first=None;last=None;logs=[];daily_errors=[];order_errors=0;dups=0
    try:
        with gzip.open(temp,'wt',encoding='utf-8',newline='',compresslevel=6) as out:
            writer=csv.writer(out)
            for url in urls:
                try:
                    status,payload,_=request_bytes(url,timeout=120,retries=5)
                    if status!=200:raise RuntimeError(f'HTTP {status}')
                    digest,cs_status,expected,check_url,cs_note=checksum_for(url,payload)
                    # A missing public checksum is recorded transparently; a present wrong one fails closed.
                    rowstats=read_kline_archive(payload,writer,a_ms,b_ms,headers)
                    if headers is None:
                        headers=rowstats['headers'];writer.writerow(headers)
                        # The first archive parser wrote only data rows; header is inserted at file beginning below via temp strategy.
                        # To keep CSV header first, this branch is handled by writing a separate header-first file in next pass.
                    total+=rowstats['rows'];order_errors+=rowstats['order_errors'];dups+=rowstats['duplicates']
                    if rowstats['first'] is not None:first=rowstats['first'] if first is None else min(first,rowstats['first'])
                    if rowstats['last'] is not None:last=rowstats['last'] if last is None else max(last,rowstats['last'])
                    logs.append({'target_file':rel(target),'data_type':'kline','symbol':symbol,'interval':interval,'period':ym,'source_url':url,'checksum_url':check_url,'http_status':200,'source_bytes':len(payload),'source_sha256':digest,'checksum_status':cs_status,'checksum_expected_sha256':expected,'records_extracted':rowstats['rows'],'download_status':'downloaded','notes':cs_note})
                except Exception as e:
                    daily_errors.append(f'{url}: {e}')
                    logs.append({'target_file':rel(target),'data_type':'kline','symbol':symbol,'interval':interval,'period':ym,'source_url':url,'http_status':'error','download_status':'failed','notes':str(e)})
        if daily_errors:
            temp.unlink(missing_ok=True)
            return {'status':'failed','target':target,'symbol':symbol,'interval':interval,'month':ym,'rows':0,'logs':logs,'error':' | '.join(daily_errors)}
        if not total:
            temp.unlink(missing_ok=True)
            return {'status':'no_rows','target':target,'symbol':symbol,'interval':interval,'month':ym,'rows':0,'logs':logs,'error':'Source archives returned no rows in requested month window'}
        # The CSV writer above streams rows before knowing source headers; normalize by reconstructing header from source schema.
        # Current Binance USD-M source header is stable; insert the header by writing a new gzip and copying data rows.
        # To avoid memory load, the data rows are already in temp; stream them into a final header-first gzip.
        header=KLINE_HEADERS
        corrected=target.with_suffix(target.suffix+'.header.part')
        with gzip.open(temp,'rt',encoding='utf-8',newline='') as inp, gzip.open(corrected,'wt',encoding='utf-8',newline='',compresslevel=6) as out:
            w=csv.writer(out);w.writerow(header)
            for row in csv.reader(inp):
                # The temporary file has an erroneous late header marker; ignore it.
                if row and row[0].strip().lower()=='open_time':continue
                if row:w.writerow(row)
        corrected.replace(target);temp.unlink(missing_ok=True)
        return {'status':'downloaded','target':target,'symbol':symbol,'interval':interval,'month':ym,'rows':total,'first':first,'last':last,'order_errors':order_errors,'duplicates':dups,'expected':expected_count(a_ms,b_ms,step),'logs':logs}
    except Exception as e:
        temp.unlink(missing_ok=True)
        return {'status':'failed','target':target,'symbol':symbol,'interval':interval,'month':ym,'rows':0,'logs':logs,'error':repr(e)}

def run_kline_download(intervals,workers=8):
    tasks=[(sym,it,m) for it in intervals for sym in SYMBOLS for m in month_starts()]
    print(f'Kline tasks queued: {len(tasks)} for intervals={intervals}',flush=True)
    done=0;fail=[]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(download_kline_month,t) for t in tasks]
        for fut in as_completed(futures):
            r=fut.result();done+=1;append_source_logs(r.get('logs',[]))
            if r['status'] in ('failed','no_rows'):fail.append({k:r.get(k) for k in ('symbol','interval','month','status','error')})
            if done%25==0 or done==len(tasks):print(f'Klines {done}/{len(tasks)} complete; failures={len(fail)}',flush=True)
    (ROOT/'klines_download_failures.json').write_text(json.dumps(fail,indent=2,ensure_ascii=False),encoding='utf-8')
    return fail

def run_coverage_check():
    url=f'{API_BASE}/fapi/v1/exchangeInfo'
    obj,raw,_=get_json(url)
    (ROOT/'exchangeInfo.json').write_bytes(raw)
    (ROOT/'exchangeInfo_retrieved_utc.txt').write_text(now_utc()+'\nSource: '+url+'\n',encoding='utf-8')
    index={x['symbol']:x for x in obj.get('symbols',[])}
    rows=[]
    for sym in SYMBOLS:
        x=index.get(sym)
        if not x:
            rows.append({'symbol':sym,'found_in_current_exchangeInfo':False,'contract_type':'','status':'','quote_asset':'','onboard_time_utc':'','onboard_before_period':False,'period_start':'2024-10-19T00:00:00Z','period_end_exclusive':'2026-10-19T00:00:00Z','check':'not_in_current_exchangeInfo'})
            continue
        onboard=int(x.get('onboardDate',0) or 0)
        rows.append({'symbol':sym,'found_in_current_exchangeInfo':True,'contract_type':x.get('contractType',''),'status':x.get('status',''),'quote_asset':x.get('quoteAsset',''),'onboard_time_utc':pd.to_datetime(onboard,unit='ms',utc=True).isoformat(),'onboard_before_period':onboard<=START_MS,'period_start':START.isoformat(),'period_end_exclusive':END.isoformat(),'check':'pass' if x.get('contractType')=='PERPETUAL' and x.get('quoteAsset')=='USDT' and onboard<=START_MS else 'review'})
    pd.DataFrame(rows).to_csv(ROOT/'symbol_listing_check.csv',index=False)
    # Save observed response and size estimate for the explicitly optional aggregate-trades stage.
    agg_url=f'{DATA_BASE}/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-01.zip'
    try:
        req=Request(agg_url,method='HEAD',headers={'User-Agent':'Mozilla/5.0'})
        with urlopen(req,timeout=30) as resp: agg_size=int(resp.headers.get('Content-Length','0'))
    except Exception as e:agg_size=0
    optional={'sample_url':agg_url,'sample_compressed_bytes':agg_size,'downloaded':False,'reason':'One BTCUSDT month is already hundreds of MB compressed; collecting all symbols and months is not practical within the current dataset task. No claim of completeness.'}
    (ROOT/'aggregate_trades_scope.json').write_text(json.dumps(optional,indent=2,ensure_ascii=False),encoding='utf-8')
    bad=[r for r in rows if r['check']!='pass']
    print(f'ExchangeInfo: {len(rows)-len(bad)}/{len(rows)} requested symbols passed contractType/quote/onboard checks.',flush=True)
    if bad:print('Review symbols:',bad,flush=True)
    print(f'AggregateTrades sample compressed size: {agg_size:,} bytes (not downloaded).',flush=True)
    return rows

def monthly_funding_urls(symbol,month):
    ym=month.strftime('%Y-%m');name=f'{symbol}-fundingRate-{ym}.zip'
    return f'{DATA_BASE}/monthly/fundingRate/{symbol}/{name}'

def parse_funding_archive(payload,start_ms,end_ms):
    z=zipfile.ZipFile(io.BytesIO(payload));names=[n for n in z.namelist() if n.lower().endswith('.csv')]
    if not names:raise RuntimeError('Funding ZIP contains no CSV')
    rows=[]
    with z.open(names[0]) as raw:
        reader=csv.DictReader(io.TextIOWrapper(raw,encoding='utf-8',newline=''))
        required={'calc_time','funding_interval_hours','last_funding_rate'}
        if not required.issubset(set(reader.fieldnames or [])):raise RuntimeError(f'Unexpected funding archive schema: {reader.fieldnames}')
        for r in reader:
            t=int(float(r['calc_time']))
            if start_ms<=t<end_ms:
                rows.append({'archive_calc_time_ms':t,'funding_interval_hours':float(r['funding_interval_hours']),'archive_funding_rate':r['last_funding_rate']})
    return rows

def fetch_funding_api_symbol(symbol):
    page_start=START_MS;out=[];logs=[];seen=set();pages=0
    while page_start<END_MS:
        params={'symbol':symbol,'startTime':page_start,'endTime':END_MS-1,'limit':1000}
        url=f'{API_BASE}/fapi/v1/fundingRate?{urlencode(params)}'
        status,b,_=request_bytes(url,timeout=60,retries=5)
        pages+=1;digest=hashlib.sha256(b).hexdigest()
        if status!=200:raise RuntimeError(f'Funding API HTTP {status} for {symbol}: {url}')
        data=json.loads(b.decode('utf-8'))
        logs.append({'target_file':rel(ROOT/'market_data'/'funding'/symbol/f'{symbol}_funding.csv.gz'),'data_type':'funding_api','symbol':symbol,'interval':'funding','period':'requested_window','source_url':url,'http_status':status,'source_bytes':len(b),'source_sha256':digest,'checksum_status':'official_api_response_sha256_recorded','download_status':'downloaded','records_extracted':len(data)})
        if not data:break
        new=[]
        for r in data:
            t=int(r['fundingTime'])
            if START_MS<=t<END_MS and t not in seen:
                seen.add(t);new.append(r)
        out.extend(new)
        last=max(int(r['fundingTime']) for r in data)
        if last<page_start or len(data)<1000:break
        page_start=last+1
        time.sleep(0.35)  # conservative public-endpoint pacing
        if pages>100:raise RuntimeError(f'Funding API pagination safety cap reached for {symbol}')
    return out,logs

def download_funding_archives(workers=8):
    tasks=[(sym,m) for sym in SYMBOLS for m in month_starts() if m.strftime('%Y-%m')!='2026-10']
    result={s:[] for s in SYMBOLS};fail=[]
    def one(task):
        sym,m=task;a,b=month_window(m,1)
        url=monthly_funding_urls(sym,m)
        log={'target_file':rel(ROOT/'market_data'/'funding'/sym/f'{sym}_funding.csv.gz'),'data_type':'funding_archive','symbol':sym,'interval':'funding','period':m.strftime('%Y-%m'),'source_url':url}
        try:
            status,payload,_=request_bytes(url,timeout=60,retries=4)
            if status!=200:raise RuntimeError(f'HTTP {status}')
            digest,cs_status,expected,check_url,note=checksum_for(url,payload)
            rows=parse_funding_archive(payload,START_MS,END_MS)
            log.update({'http_status':status,'source_bytes':len(payload),'source_sha256':digest,'checksum_status':cs_status,'checksum_expected_sha256':expected,'checksum_url':check_url,'records_extracted':len(rows),'download_status':'downloaded','notes':note})
            return sym,rows,log,None
        except Exception as e:
            log.update({'http_status':'error','download_status':'failed','notes':str(e)})
            return sym,[],log,str(e)
    print(f'Funding archive cross-check tasks: {len(tasks)}',flush=True)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(one,t) for t in tasks]
        for i,f in enumerate(as_completed(futures),1):
            sym,rows,log,err=f.result();result[sym].extend(rows);append_source_logs([log])
            if err:fail.append({'symbol':sym,'period':log['period'],'error':err})
            if i%100==0 or i==len(tasks):print(f'Funding archives {i}/{len(tasks)}; failures={len(fail)}',flush=True)
    (ROOT/'funding_archive_failures.json').write_text(json.dumps(fail,indent=2,ensure_ascii=False),encoding='utf-8')
    return result

def write_funding_files():
    archive=download_funding_archives()
    fail_api=[];total=0
    outcols=['symbol','funding_time_ms','funding_time_utc','funding_rate','mark_price','rate_type','funding_interval_hours','archive_calc_time_ms','archive_funding_rate','api_archive_rate_diff','archive_timestamp_diff_ms']
    for i,sym in enumerate(SYMBOLS,1):
        target=ROOT/'market_data'/'funding'/sym/f'{sym}_funding.csv.gz';target.parent.mkdir(parents=True,exist_ok=True)
        try:
            api,logs=fetch_funding_api_symbol(sym);append_source_logs(logs)
            if not api:raise RuntimeError('No rows returned by official fundingRate endpoint')
            api=sorted(api,key=lambda r:int(r['fundingTime']))
            adf=pd.DataFrame(archive.get(sym,[]))
            if adf.empty:
                merged=pd.DataFrame({'api':api})
                merged['archive_calc_time_ms']=np.nan;merged['funding_interval_hours']=np.nan;merged['archive_funding_rate']=np.nan;merged['archive_timestamp_diff_ms']=np.nan
            else:
                api_df=pd.DataFrame(api)
                api_df['funding_time_ms']=api_df['fundingTime'].astype('int64')
                api_df=api_df.sort_values('funding_time_ms')
                adf=adf.sort_values('archive_calc_time_ms')
                merged=pd.merge_asof(api_df,adf,left_on='funding_time_ms',right_on='archive_calc_time_ms',direction='nearest',tolerance=10_000)
                merged['archive_timestamp_diff_ms']=merged['funding_time_ms']-merged['archive_calc_time_ms']
                merged['api']=None
            out=[]
            for _,r in merged.iterrows():
                if 'api' in merged.columns and r.get('api') is not None and not (isinstance(r.get('api'),float) and np.isnan(r.get('api'))):
                    a=r['api'];t=int(a['fundingTime']);rate=a.get('fundingRate','');mark=a.get('markPrice','');rtype=a.get('rateType','')
                    ih='';calc='';arate='';tdiff=''
                else:
                    t=int(r['funding_time_ms']);rate=r.get('fundingRate','');mark=r.get('markPrice','');rtype=r.get('rateType','')
                    ih=r.get('funding_interval_hours','');calc=r.get('archive_calc_time_ms','');arate=r.get('archive_funding_rate','');tdiff=r.get('archive_timestamp_diff_ms','')
                    if pd.isna(ih):ih=''
                    if pd.isna(calc):calc=''
                    if pd.isna(arate):arate=''
                    if pd.isna(tdiff):tdiff=''
                try:diff=abs(float(rate)-float(arate)) if arate!='' else ''
                except Exception:diff=''
                out.append({'symbol':sym,'funding_time_ms':t,'funding_time_utc':pd.to_datetime(t,unit='ms',utc=True).isoformat(),'funding_rate':rate,'mark_price':mark,'rate_type':rtype,'funding_interval_hours':ih,'archive_calc_time_ms':calc,'archive_funding_rate':arate,'api_archive_rate_diff':diff,'archive_timestamp_diff_ms':tdiff})
            odf=pd.DataFrame(out,columns=outcols).drop_duplicates('funding_time_ms').sort_values('funding_time_ms')
            odf.to_csv(target,index=False,compression={'method':'gzip','compresslevel':6},float_format='%.16g')
            total+=len(odf)
        except Exception as e:
            fail_api.append({'symbol':sym,'error':repr(e)})
        if i%5==0 or i==len(SYMBOLS):print(f'Funding API {i}/{len(SYMBOLS)}; failed={len(fail_api)}',flush=True)
    (ROOT/'funding_api_failures.json').write_text(json.dumps(fail_api,indent=2,ensure_ascii=False),encoding='utf-8')
    print(f'Funding output records: {total:,}; failures={len(fail_api)}',flush=True)
    return fail_api

def utc_ms(value):
    if value is None or (isinstance(value,float) and np.isnan(value)):return ''
    try:return pd.to_datetime(int(value),unit='ms',utc=True).isoformat()
    except Exception:return ''

def stream_csv_gz_summary(path):
    count=0;first=None;last=None;header=[]
    with gzip.open(path,'rt',encoding='utf-8',newline='') as f:
        r=csv.reader(f);header=next(r,[])
        for row in r:
            if not row:continue
            count+=1
            if header and header[0] in ('open_time','funding_time_ms'):
                t=int(float(row[0]));first=t if first is None else first;last=t
    return count,first,last,header

def build_data_manifest():
    logs=pd.read_csv(SOURCE_LOG).fillna('') if SOURCE_LOG.exists() else pd.DataFrame()
    rows=[]
    for interval,step in INTERVALS.items():
        for sym in SYMBOLS:
            for m in month_starts():
                ym=m.strftime('%Y-%m');a,b=month_window(m,step);target=ROOT/'market_data'/'klines'/interval/sym/f'{sym}_{interval}_{ym}.csv.gz'
                sr=logs[(logs.get('target_file','')==rel(target))] if not logs.empty else pd.DataFrame()
                successful=sr[sr.download_status=='downloaded'] if (not sr.empty and 'download_status' in sr) else pd.DataFrame()
                urls=';'.join(map(str,successful.get('source_url',pd.Series(dtype=str)).tolist()))
                src_hash=';'.join(map(str,successful.get('source_sha256',pd.Series(dtype=str)).tolist()))
                checks=';'.join(map(str,successful.get('checksum_status',pd.Series(dtype=str)).tolist()))
                expected=expected_count(a,b,step)
                if target.exists():
                    n,first,last,headers=stream_csv_gz_summary(target);size=target.stat().st_size
                    status='complete' if n==expected else 'partial'
                    verification='source_checksum_verified' if checks and all(x=='source_checksum_verified' for x in checks.split(';')) else ('checksum_not_available_for_some_sources' if checks else 'source_log_missing')
                else:n=0;first=last=None;headers=[];size=0;status='missing';verification='not_downloaded'
                rows.append({'file_name':rel(target) if target.exists() else '', 'symbol':sym,'data_type':'kline','interval':interval,'period_expected_start_utc':utc_ms(a),'period_expected_end_utc_exclusive':utc_ms(b),'period_actual_first_open_utc':utc_ms(first),'period_actual_last_open_utc':utc_ms(last),'row_count':n,'expected_row_count':expected,'size_bytes':size,'source':'Binance Vision USD-M Futures','source_url':urls,'source_archive_sha256':src_hash,'download_status':status,'verification_status':verification,'notes':';'.join(map(str,successful.get('notes',pd.Series(dtype=str)).tolist()))})
    # Funding records are one file per symbol.
    for sym in SYMBOLS:
        target=ROOT/'market_data'/'funding'/sym/f'{sym}_funding.csv.gz'
        sr=logs[(logs.get('target_file','')==rel(target))] if not logs.empty else pd.DataFrame()
        if target.exists():
            df=pd.read_csv(target,compression='gzip');n=len(df);first=int(df.funding_time_ms.min()) if n else None;last=int(df.funding_time_ms.max()) if n else None
            status='downloaded' if n else 'empty';size=target.stat().st_size
        else:n=0;first=last=None;status='missing';size=0
        rows.append({'file_name':rel(target) if target.exists() else '', 'symbol':sym,'data_type':'funding_rate','interval':'funding events','period_expected_start_utc':START.isoformat(),'period_expected_end_utc_exclusive':END.isoformat(),'period_actual_first_open_utc':utc_ms(first),'period_actual_last_open_utc':utc_ms(last),'row_count':n,'expected_row_count':'variable funding schedule','size_bytes':size,'source':'Binance USD-M Futures API with Binance Vision archive cross-check','source_url':';'.join(map(str,sr.get('source_url',pd.Series(dtype=str)).tolist())),'source_archive_sha256':';'.join(map(str,sr.get('source_sha256',pd.Series(dtype=str)).tolist())),'download_status':status,'verification_status':'API rows retained; archive rate cross-check reported separately','notes':'markPrice from official fundingRate REST response where supplied'})
    df=pd.DataFrame(rows);df.to_csv(ROOT/'data_manifest.csv',index=False)
    return df

def load_symbol_interval(symbol,interval):
    files=sorted((ROOT/'market_data'/'klines'/interval/symbol).glob(f'{symbol}_{interval}_*.csv.gz'))
    if not files:return pd.DataFrame()
    frames=[]
    for p in files:
        try:frames.append(pd.read_csv(p,compression='gzip',low_memory=False))
        except Exception as e:print('READ ERROR',p,e,flush=True)
    if not frames:return pd.DataFrame()
    df=pd.concat(frames,ignore_index=True)
    for col in KLINE_HEADERS:
        if col in df.columns:df[col]=pd.to_numeric(df[col],errors='coerce')
    return df.sort_values('open_time').reset_index(drop=True)

def quality_one(symbol,interval):
    df=load_symbol_interval(symbol,interval);step=INTERVALS[interval];expected=expected_count(START_MS,END_MS,step)
    if df.empty:
        return {'symbol':symbol,'interval':interval,'row_count':0,'expected_row_count':expected,'first_open_utc':'','last_open_utc':'','duplicate_timestamps':0,'missing_expected_bars':expected,'gap_events':0,'non_grid_timestamps':0,'missing_or_nonnumeric_cells':0,'invalid_ohlc_rows':0,'negative_volume_rows':0,'invalid_trade_count_rows':0,'close_time_alignment_errors':0,'last_candle_closed':'no_data'}
    o=df.open_time.astype('int64').to_numpy();unique=np.unique(o);diff=np.diff(unique)
    gap_events=int(np.sum(diff>step));gap_missing=int(np.sum(np.maximum(diff//step-1,0)))
    first=int(unique[0]);last=int(unique[-1]);edge_missing=max(0,(first-START_MS)//step)+max(0,(END_MS-step-last)//step)
    dup=int(df.open_time.duplicated(keep='first').sum())
    required=['open_time','open','high','low','close','volume','close_time','quote_volume','count','taker_buy_volume','taker_buy_quote_volume']
    missing_cells=int(df[required].isna().sum().sum())
    bad_ohlc=int(((df.high<df.low)|(df.high<df[['open','close']].max(axis=1))|(df.low>df[['open','close']].min(axis=1))).sum())
    volcols=['volume','quote_volume','taker_buy_volume','taker_buy_quote_volume']
    negative=int((df[volcols]<0).any(axis=1).sum())
    bad_count=int((df['count']<0).sum())
    close_errors=int((df.close_time!=(df.open_time+step-1)).sum())
    nongrid=int(((df.open_time-START_MS)%step!=0).sum())
    closed=bool((df.close_time.max()<END_MS) and (df.close_time.max()>=df.open_time.max()))
    return {'symbol':symbol,'interval':interval,'row_count':len(df),'expected_row_count':expected,'first_open_utc':utc_ms(first),'last_open_utc':utc_ms(last),'duplicate_timestamps':dup,'missing_expected_bars':max(0,expected-len(unique)),'gap_events':gap_events,'missing_bars_inside_gaps':gap_missing,'missing_at_period_edges':edge_missing,'non_grid_timestamps':nongrid,'missing_or_nonnumeric_cells':missing_cells,'invalid_ohlc_rows':bad_ohlc,'negative_volume_rows':negative,'invalid_trade_count_rows':bad_count,'close_time_alignment_errors':close_errors,'last_candle_closed':'yes' if closed else 'no'}

def funding_quality_one(symbol):
    p=ROOT/'market_data'/'funding'/symbol/f'{symbol}_funding.csv.gz'
    if not p.exists():return {'symbol':symbol,'row_count':0,'first_funding_utc':'','last_funding_utc':'','duplicate_funding_times':0,'missing_mark_price_rows':0,'archive_matched_rows':0,'archive_rate_mismatches':0,'unexpected_funding_gaps':0,'notes':'funding file missing'}
    df=pd.read_csv(p,compression='gzip');df['funding_time_ms']=pd.to_numeric(df.funding_time_ms,errors='coerce')
    df['funding_rate_num']=pd.to_numeric(df.funding_rate,errors='coerce');df['mark_price_num']=pd.to_numeric(df.mark_price,errors='coerce')
    df['archive_funding_rate_num']=pd.to_numeric(df.archive_funding_rate,errors='coerce');df['funding_interval_hours_num']=pd.to_numeric(df.funding_interval_hours,errors='coerce')
    d=df.sort_values('funding_time_ms');t=d.funding_time_ms.to_numpy(dtype='float64');dt=np.diff(t)/3_600_000
    exp=d.funding_interval_hours_num.to_numpy(dtype='float64')[:-1] if len(d)>1 else np.array([])
    gap=int(np.sum((exp>0)&(dt>exp*1.5+0.1)))
    matched=int(d.archive_calc_time_ms.notna().sum())
    mismatch=int(((d.funding_rate_num-d.archive_funding_rate_num).abs()>1e-12).fillna(False).sum())
    return {'symbol':symbol,'row_count':len(df),'first_funding_utc':utc_ms(int(d.funding_time_ms.min())) if len(df) else '', 'last_funding_utc':utc_ms(int(d.funding_time_ms.max())) if len(df) else '', 'duplicate_funding_times':int(d.funding_time_ms.duplicated().sum()),'missing_mark_price_rows':int(d.mark_price_num.isna().sum()),'archive_matched_rows':matched,'archive_rate_mismatches':mismatch,'unexpected_funding_gaps':gap,'max_gap_hours':float(np.max(dt)) if len(dt) else 0,'notes':'gap flag uses previous matched archive funding_interval_hours; archive timestamp match tolerance 10 seconds'}

def hourly_validation_one(symbol):
    m=load_symbol_interval(symbol,'1m');h=load_symbol_interval(symbol,'1h')
    fields=['open','high','low','close','volume','quote_volume','count','taker_buy_volume','taker_buy_quote_volume']
    if m.empty or h.empty:return {'symbol':symbol,'native_hourly_rows':len(h),'hourly_rows_reconstructed_from_1m':0,'complete_hour_comparisons':0,'hours_without_60_unique_minutes':0,'rows_with_any_field_mismatch':0,'close_time_mismatches':0,'notes':'one or both frames missing'}
    m=m.sort_values('open_time').copy();m['hour_open_ms']=(m.open_time.astype('int64')//3_600_000)*3_600_000
    agg=m.groupby('hour_open_ms',sort=True).agg(open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),volume=('volume','sum'),quote_volume=('quote_volume','sum'),count=('count','sum'),taker_buy_volume=('taker_buy_volume','sum'),taker_buy_quote_volume=('taker_buy_quote_volume','sum'),minute_rows=('open_time','size'),minute_unique=('open_time','nunique'),close_time=('close_time','last')).reset_index()
    native=h.rename(columns={'open_time':'hour_open_ms'}).copy()
    merged=native.merge(agg,on='hour_open_ms',how='left',suffixes=('_native','_m1'))
    complete=merged[(merged.minute_rows==60)&(merged.minute_unique==60)].copy()
    mismatches=np.zeros(len(complete),dtype=bool);maxdiff={}
    for f in fields:
        x=pd.to_numeric(complete[f+'_native'],errors='coerce').to_numpy(dtype='float64')
        y=pd.to_numeric(complete[f+'_m1'],errors='coerce').to_numpy(dtype='float64')
        d=np.abs(x-y);maxdiff[f]=float(np.nanmax(d)) if len(d) and np.isfinite(d).any() else ''
        tol=0 if f=='count' else np.maximum(1e-8,1e-12*np.abs(x))
        mismatches|=(d>tol)
    close_mismatch=int((complete.close_time_native!=complete.close_time_m1).sum()) if len(complete) else 0
    row={'symbol':symbol,'native_hourly_rows':len(native),'hourly_rows_reconstructed_from_1m':len(agg),'complete_hour_comparisons':len(complete),'hours_without_60_unique_minutes':int(len(merged)-len(complete)),'rows_with_any_field_mismatch':int(mismatches.sum()),'close_time_mismatches':close_mismatch,'notes':'UTC hour; compare complete 60-unique-minute groups only; float tolerance max(1e-8,1e-12*abs(native)), trade count exact'}
    for f,v in maxdiff.items():row[f'max_abs_diff_{f}']=v
    return row

def write_hourly_mismatch_details():
    fields=['open','high','low','close','volume','quote_volume','count','taker_buy_volume','taker_buy_quote_volume']
    out=[]
    for symbol in SYMBOLS:
        m=load_symbol_interval(symbol,'1m');h=load_symbol_interval(symbol,'1h')
        if m.empty or h.empty:continue
        m=m.sort_values('open_time').copy();m['hour_open_ms']=(m.open_time.astype('int64')//3_600_000)*3_600_000
        agg=m.groupby('hour_open_ms',sort=True).agg(open=('open','first'),high=('high','max'),low=('low','min'),close=('close','last'),volume=('volume','sum'),quote_volume=('quote_volume','sum'),count=('count','sum'),taker_buy_volume=('taker_buy_volume','sum'),taker_buy_quote_volume=('taker_buy_quote_volume','sum'),minute_rows=('open_time','size'),minute_unique=('open_time','nunique')).reset_index()
        native=h.rename(columns={'open_time':'hour_open_ms'})
        merged=native.merge(agg,on='hour_open_ms',suffixes=('_native','_m1'))
        merged=merged[(merged.minute_rows==60)&(merged.minute_unique==60)]
        for _,r in merged.iterrows():
            for field in fields:
                n=float(r[field+'_native']);v=float(r[field+'_m1']);delta=abs(n-v)
                tolerance=0 if field=='count' else max(1e-8,1e-12*abs(n))
                if delta>tolerance:
                    out.append({'symbol':symbol,'hour_open_utc':pd.to_datetime(int(r.hour_open_ms),unit='ms',utc=True).isoformat(),'field':field,'native_1h_value':n,'aggregated_1m_value':v,'absolute_difference':delta})
    df=pd.DataFrame(out,columns=['symbol','hour_open_utc','field','native_1h_value','aggregated_1m_value','absolute_difference'])
    df.to_csv(ROOT/'hourly_validation_mismatch_detail.csv',index=False)
    return df

def write_quality_reports():
    q=[quality_one(sym,it) for it in INTERVALS for sym in SYMBOLS]
    pd.DataFrame(q).to_csv(ROOT/'data_quality_report.csv',index=False)
    fq=[funding_quality_one(s) for s in SYMBOLS];pd.DataFrame(fq).to_csv(ROOT/'funding_quality_report.csv',index=False)
    hv=[hourly_validation_one(s) for s in SYMBOLS];pd.DataFrame(hv).to_csv(ROOT/'hourly_validation_report.csv',index=False)
    write_hourly_mismatch_details()
    print('Quality summary:')
    qdf=pd.DataFrame(q)
    print(qdf.groupby('interval').agg(symbols=('symbol','nunique'),rows=('row_count','sum'),missing=('missing_expected_bars','sum'),duplicates=('duplicate_timestamps','sum'),bad_ohlc=('invalid_ohlc_rows','sum')).to_string(),flush=True)
    print('Hourly comparison:')
    print(pd.DataFrame(hv)[['symbol','complete_hour_comparisons','hours_without_60_unique_minutes','rows_with_any_field_mismatch']].to_string(index=False),flush=True)
    return qdf,pd.DataFrame(fq),pd.DataFrame(hv)

def build_data_manifest():
    logs=pd.read_csv(SOURCE_LOG).fillna('') if SOURCE_LOG.exists() else pd.DataFrame()
    rows=[]
    for interval,step in INTERVALS.items():
        for sym in SYMBOLS:
            for m in month_starts():
                ym=m.strftime('%Y-%m');a,b=month_window(m,step);target=ROOT/'market_data'/'klines'/interval/sym/f'{sym}_{interval}_{ym}.csv.gz'
                sr=logs[logs.target_file==rel(target)] if (not logs.empty and 'target_file' in logs) else pd.DataFrame()
                successful=sr[sr.download_status=='downloaded'] if (not sr.empty and 'download_status' in sr) else pd.DataFrame()
                urls=';'.join(map(str,successful.get('source_url',pd.Series(dtype=str)).tolist()))
                hashes=';'.join(map(str,successful.get('source_sha256',pd.Series(dtype=str)).tolist()))
                checks=';'.join(map(str,successful.get('checksum_status',pd.Series(dtype=str)).tolist()))
                expected=expected_count(a,b,step)
                if target.exists():
                    n,first,last,headers=stream_csv_gz_summary(target);size=target.stat().st_size;status='complete' if n==expected else 'partial'
                    verification='source_checksum_verified' if checks and all(x=='source_checksum_verified' for x in checks.split(';')) else ('checksum_unavailable_for_some_sources' if checks else 'source_log_missing')
                else:n=0;first=last=None;headers=[];size=0;status='missing';verification='not_downloaded'
                rows.append({'file_name':rel(target) if target.exists() else '', 'symbol':sym,'data_type':'kline','interval':interval,'period_expected_start_utc':utc_ms(a),'period_expected_end_utc_exclusive':utc_ms(b),'period_actual_first_open_utc':utc_ms(first),'period_actual_last_open_utc':utc_ms(last),'row_count':n,'expected_row_count':expected,'size_bytes':size,'source':'Binance Vision USD-M Futures','source_url':urls,'source_archive_sha256':hashes,'download_status':status,'verification_status':verification,'notes':';'.join(map(str,successful.get('notes',pd.Series(dtype=str)).tolist()))})
    for sym in SYMBOLS:
        target=ROOT/'market_data'/'funding'/sym/f'{sym}_funding.csv.gz';sr=logs[logs.target_file==rel(target)] if (not logs.empty and 'target_file' in logs) else pd.DataFrame()
        if target.exists():
            df=pd.read_csv(target,compression='gzip');n=len(df);first=int(df.funding_time_ms.min()) if n else None;last=int(df.funding_time_ms.max()) if n else None;size=target.stat().st_size;status='downloaded' if n else 'empty'
        else:n=0;first=last=None;size=0;status='missing'
        rows.append({'file_name':rel(target) if target.exists() else '', 'symbol':sym,'data_type':'funding_rate','interval':'funding events','period_expected_start_utc':START.isoformat(),'period_expected_end_utc_exclusive':END.isoformat(),'period_actual_first_open_utc':utc_ms(first),'period_actual_last_open_utc':utc_ms(last),'row_count':n,'expected_row_count':'variable funding schedule','size_bytes':size,'source':'Binance USD-M Futures API with Binance Vision archive cross-check','source_url':';'.join(map(str,sr.source_url.tolist())) if len(sr) else '','source_archive_sha256':';'.join(map(str,sr.source_sha256.tolist())) if len(sr) else '','download_status':status,'verification_status':'official API retained; archive rate comparison in funding_quality_report.csv','notes':'markPrice retained from fundingRate endpoint when present'})
    df=pd.DataFrame(rows);df.to_csv(ROOT/'data_manifest.csv',index=False);return df

def file_sha256(path):
    h=hashlib.sha256()
    with open(path,'rb') as f:
        for b in iter(lambda:f.read(4*1024*1024),b''):h.update(b)
    return h.hexdigest()

def make_data_archives():
    made=[]
    for interval in ['1m','5m','1h']:
        files=sorted((ROOT/'market_data'/'klines'/interval).rglob('*.csv.gz'))
        if not files:continue
        out=ROOT/f'BINANCE_FUTURES_{interval}_2024-10_to_2026-10.zip'
        with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as z:
            for f in files:z.write(f,f.relative_to(ROOT).as_posix())
        made.append(out);print('Archive',out.name,'files',len(files),'bytes',out.stat().st_size,flush=True)
    fund=sorted((ROOT/'market_data'/'funding').rglob('*.csv.gz'))
    if fund:
        out=ROOT/'BINANCE_FUTURES_FUNDING_2024-10_to_2026-10.zip'
        with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=1,allowZip64=True) as z:
            for f in fund:z.write(f,f.relative_to(ROOT).as_posix())
        made.append(out);print('Archive',out.name,'files',len(fund),'bytes',out.stat().st_size,flush=True)
    reportfiles=[ROOT/n for n in ['README_AR.md','SOURCE_NOTES.md','data_manifest.csv','data_quality_report.csv','funding_quality_report.csv','hourly_validation_report.csv','hourly_validation_mismatch_detail.csv','hourly_validation_api_spotcheck.csv','source_download_log.csv','symbol_listing_check.csv','exchangeInfo.json','aggregate_trades_scope.json','download_and_audit_binance.py','exchangeInfo_retrieved_utc.txt'] if (ROOT/n).exists()]
    if reportfiles:
        out=ROOT/'BINANCE_FUTURES_AUDIT_REPORTS.zip'
        with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6,allowZip64=True) as z:
            for f in reportfiles:z.write(f,f.relative_to(ROOT).as_posix())
        made.append(out)
    return made

def write_readme(manifest,q,fq,hv,archives):
    qdf=q if isinstance(q,pd.DataFrame) else pd.DataFrame(q)
    lines=[]
    lines += ['# Binance USDⓈ-M Futures Historical Data Audit','',
      '## الفترة والنطاق','',
      f'الفترة المطلوبة: `{START.isoformat()}` شاملة حتى `{END.isoformat()}` غير شاملة، أي 730 يومًا. المنطقة الزمنية لجميع الطوابع UTC. البيانات مخصصة للتحقق التاريخي فقط؛ لم تُطوّر أو تُعدّل أي استراتيجية في هذه المهمة.','',
      f'الرموز: {", ".join(SYMBOLS)}. جرى التحقق من `/fapi/v1/exchangeInfo`، ونوع العقود الحالية perpetual وتسويتها USDT، كما حُفظ `onboardDate` لكل رمز [2] [4]. فحص تغطية الشموع الفعلية هو المرجع النهائي لأي نقص.','',
      '## المصادر والحقول','',
      'شموع 1m و5m و1h مأخوذة من ملفات Binance Vision تحت `data/futures/um`, وهي أرشيفات USDⓈ-M Futures وليست Spot أو COIN-M [1] [3]. احتُفظ بترتيب حقول المصدر الأصلي وأضيف صف أسماء الحقول الذي تعرضه ملفات Binance [1]. الطوابع الأصلية Unix milliseconds UTC؛ الأسعار والأحجام لم تُحوّل أو تُعدّل. الملفات الشهرية بصيغة `.csv.gz` داخل أرشيفات ZIP.','',
      'حقول شموع المصدر: `open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_volume, taker_buy_quote_volume, ignore`. الحجم الأساسي بوحدة الأصل، و`quote_volume` و` taker_buy_quote_volume` بوحدة USDT بحسب ملف العقد.','',
      'التمويل مأخوذ من Binance USD-M Futures `/fapi/v1/fundingRate` متضمنًا `fundingRate`, `fundingTime`, و`markPrice` عندما يرسله المصدر [2] [5]. قورن المعدل وتوقيت السجل بأرشيف Binance Vision الشهري الذي يضم وقت الحساب وفاصل التمويل [1] [3]. المقارنة الأرشيفية تمت حتى 2026-09؛ سجلات أكتوبر 2026 الجزئية مصدرها API فقط. لم تُسند قيمة صفر إلى سجل مفقود، ولم يُستوفَ أي نقص.','',
      '## نتائج التغطية وجودة البيانات','']
    if not qdf.empty:
        lines.append('ملخص الشموع بحسب الإطار:')
        for it,g in qdf.groupby('interval'):
            lines.append(f'- `{it}`: {int(g.symbol.nunique())} رموز، {int(g.row_count.sum()):,} صفًا؛ المفقود المتوقع {int(g.missing_expected_bars.sum()):,}، المكرر {int(g.duplicate_timestamps.sum()):,}، OHLC غير صالح {int(g.invalid_ohlc_rows.sum()):,}، خلايا ناقصة/غير رقمية {int(g.missing_or_nonnumeric_cells.sum()):,}.')
    if fq is not None and len(fq):
        lines.append('')
        fdf=fq if isinstance(fq,pd.DataFrame) else pd.DataFrame(fq)
        lines.append(f'التمويل: {int(fdf.row_count.sum()):,} سجلًا عبر {int(fdf.symbol.nunique())} رموز؛ قيم mark price المفقودة {int(fdf.missing_mark_price_rows.sum()):,}؛ فجوات تمويل محتملة بحسب الفاصل التاريخي {int(fdf.unexpected_funding_gaps.sum()):,}؛ اختلافات المعدل عن أرشيف Vision {int(fdf.archive_rate_mismatches.sum()):,}. افحص التقرير لكل رمز قبل الاستخدام.')
    if hv is not None and len(hv):
        vdf=hv if isinstance(hv,pd.DataFrame) else pd.DataFrame(hv)
        lines.append('')
        detail_path=ROOT/'hourly_validation_mismatch_detail.csv'
        detail=pd.read_csv(detail_path) if detail_path.exists() else pd.DataFrame()
        mismatch_hours=detail[['symbol','hour_open_utc']].drop_duplicates().shape[0] if len(detail) else 0
        lines.append(f'مطابقة 1h مع تجميع 1m: {int(vdf.complete_hour_comparisons.sum()):,} ساعات كاملة قابلة للمقارنة؛ {int(vdf.hours_without_60_unique_minutes.sum()):,} ساعات لم يتوفر لها 60 دقيقة فريدة؛ {int(vdf.rows_with_any_field_mismatch.sum()):,} ساعات بها اختلاف واحد أو أكثر في الحقول المقاسة، عبر {mismatch_hours} رمز/ساعة فريدة. راجع `hourly_validation_report.csv` لأقصى الفروق و`hourly_validation_mismatch_detail.csv` لسجل كل فرق. لا تُعامل هذه الفروق كضوضاء رقمية: قيم التداول/الحجم تختلف في بعض الساعات، ولم تُسوَّ البيانات بين المصدرين.')
        if len(detail):
            maxima=[]
            for field in ['open','high','low','close','volume','quote_volume','count']:
                subset=detail[detail.field==field]
                if len(subset):
                    r=subset.loc[subset.absolute_difference.idxmax()]
                    maxima.append(f"{field}={r.absolute_difference:,.12g} ({r.symbol})")
            lines.append('أكبر فروق مطلقة بين المصدرين: '+ '; '.join(maxima)+'. volume بوحدة الأصل للرمز المشار إليه، وquote_volume بوحدة USDT؛ هذه فروق في الساعات المتباينة وليست فروق تقريب عامة.')
        api_spot=ROOT/'hourly_validation_api_spotcheck.csv'
        if api_spot.exists():
            api=pd.read_csv(api_spot).fillna('')
            rest_bad=int((api.api_1m_aggregate_vs_api_1h_mismatched_fields!='').sum())
            lines.append(f'فحص إضافي محدود عبر Klines REST الرسمية على 4 رموز × 5 ساعات: أُعيدت 60 شمعة 1m وشمعة 1h لكل عينة؛ {rest_bad} من 20 عينة ظل فيها تجميع REST لـ1m مختلفًا عن شمعة REST لـ1h. التفاصيل وروابط API في `hourly_validation_api_spotcheck.csv`؛ لا تدّعي هذه العينة حسم سبب الاختلافات أو إصلاحها.')
    lines += ['',
      'الملفات الأخيرة ضمن الفترة مغلقة بالكامل لأن آخر وقت فتح مطلوب يسبق النهاية الحصرية. يفحص `data_quality_report.csv` توافق وقت الإغلاق وبيانات الشموع الأخيرة. الفجوات تُسجّل ولا تُملأ.','',
      '## استخدام الملفات','',
      'فك ضغط أرشيف الإطار المطلوب، ثم فك ضغط ملفات `.csv.gz` داخله. يمكن قراءتها في Python بـ `pandas.read_csv("BTCUSDT_1m_2024-10.csv.gz")`. لا تغيّر أسماء الأعمدة إذا أردت إعادة المقارنة مع أرشيف Binance.','',
      'ملفات التدقيق: `data_manifest.csv`, `data_quality_report.csv`, `funding_quality_report.csv`, `hourly_validation_report.csv`, `hourly_validation_mismatch_detail.csv`, `hourly_validation_api_spotcheck.csv`, `source_download_log.csv`, `symbol_listing_check.csv`, و`SHA256SUMS.txt`. يسجل `data_manifest.csv` الفترة المتوقعة والفعلية، عدد الصفوف، الحجم، المصدر وحالة التحقق لكل ملف.','',
      '## قيود وما لم يُنزّل','',
      'لم تُنزّل Aggregate Trades التفصيلية. حجم ملف Binance الرسمي لعينة BTCUSDT يناير 2025 كان 692,363,439 bytes مضغوطًا [1]؛ تعميم الجمع على 20 عقدًا وسنتين غير عملي هنا. هذا قياس عينة، وليس تقديرًا دقيقًا لحجم كامل البيانات. لا تتضمن الحزمة بيانات اصطناعية ولا Spot ولا COIN-M.','',
      '## أرشيفات التسليم','']
    for a in archives:
        if a.name=='BINANCE_FUTURES_AUDIT_REPORTS.zip':size='حزمة تقارير وبيانات وصفية'
        else:size=f'{a.stat().st_size:,} bytes' if a.exists() else 'سيُنشأ بعد اكتمال التغليف'
        lines.append(f'- `{a.name}` — {size}.')
    lines += ['', '## المراجع الرسمية','',
      '[1]: https://github.com/binance/binance-public-data "Binance Public Data README"','',
      '[2]: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data "Binance USD-M Futures Market Data API"','',
      '[3]: https://data.binance.vision/data/futures/um/ "Binance Data Vision USD-M archive"','',
      '[4]: https://fapi.binance.com/fapi/v1/exchangeInfo "Binance Exchange Information"','',
      '[5]: https://fapi.binance.com/fapi/v1/fundingRate "Binance Funding Rate History"','']
    (ROOT/'README_AR.md').write_text('\n'.join(lines),encoding='utf-8')

def write_sha256sums():
    files=[]
    for p in ROOT.rglob('*'):
        if not p.is_file() or p.name=='SHA256SUMS.txt' or p.name.endswith('.part'):continue
        if any(x in p.parts for x in ['.git','__pycache__']):continue
        files.append(p)
    lines=[]
    for p in sorted(files):lines.append(f'{file_sha256(p)}  {p.relative_to(ROOT).as_posix()}')
    (ROOT/'SHA256SUMS.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')

def audit_and_package():
    manifest=build_data_manifest();q,fq,hv=write_quality_reports()
    # Preliminary README fixes archive names; write before making report ZIP.
    planned=[ROOT/f'BINANCE_FUTURES_{it}_2024-10_to_2026-10.zip' for it in ['1m','5m','1h']]
    planned += [ROOT/'BINANCE_FUTURES_FUNDING_2024-10_to_2026-10.zip',ROOT/'BINANCE_FUTURES_AUDIT_REPORTS.zip']
    write_readme(manifest,q,fq,hv,planned)
    archives=make_data_archives()
    # Rebuild README with actual archives, then rebuild reports ZIP to contain final README and QA outputs.
    write_readme(manifest,q,fq,hv,archives)
    reports=[ROOT/n for n in ['README_AR.md','SOURCE_NOTES.md','data_manifest.csv','data_quality_report.csv','funding_quality_report.csv','hourly_validation_report.csv','hourly_validation_mismatch_detail.csv','hourly_validation_api_spotcheck.csv','source_download_log.csv','symbol_listing_check.csv','exchangeInfo.json','aggregate_trades_scope.json','download_and_audit_binance.py','exchangeInfo_retrieved_utc.txt'] if (ROOT/n).exists()]
    reportzip=ROOT/'BINANCE_FUTURES_AUDIT_REPORTS.zip'
    with zipfile.ZipFile(reportzip,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6,allowZip64=True) as z:
        for f in reports:z.write(f,f.relative_to(ROOT).as_posix())
    write_sha256sums()
    print('Audit and packaging complete.',flush=True)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--stage',choices=['check','klines','funding','audit','all'],default='all')
    ap.add_argument('--intervals',nargs='*',choices=list(INTERVALS),default=['1m','5m','1h'])
    ap.add_argument('--workers',type=int,default=8)
    args=ap.parse_args();ROOT.mkdir(parents=True,exist_ok=True)
    if args.stage in ('check','all','klines','funding') and not (ROOT/'exchangeInfo.json').exists():run_coverage_check()
    elif args.stage=='check':run_coverage_check()
    if args.stage=='check':return
    if args.stage in ('klines','all'):
        failures=run_kline_download(args.intervals,workers=args.workers)
        print(f'Kline downloads finished with {len(failures)} failed/no-row tasks.',flush=True)
    if args.stage in ('funding','all'):
        write_funding_files()
    if args.stage in ('audit','all'):
        audit_and_package()

if __name__=='__main__':main()
