import pandas as pd, numpy as np
B='/mnt/user-data/uploads/MarketPulse/data/backtest/'
ctx=pd.read_csv(B+'label_context.csv', parse_dates=['ts_utc'])
ev=[]
for f in ['label_z.csv','label_market.csv','label_fixed.csv']:
    d=pd.read_csv(B+f); d['file']=f; d['row']=range(len(d)); ev.append(d)
ev=pd.concat(ev, ignore_index=True)
ev['t']=pd.to_datetime(ev['detected_minute_utc'])

out=[]
for _,e in ev.iterrows():
    c=ctx[(ctx.file==e.file)&(ctx.row==e.row)].sort_values('ts_utc').set_index('ts_utc')
    day=c[(c.index>=e.t.normalize())&(c.index<e.t.normalize()+pd.Timedelta(days=1))]
    # 5-min features on a continuous minute grid (missing minutes: carry price, zero volume)
    g=c.reindex(pd.date_range(c.index.min(), c.index.max(), freq='1min'))
    g['close']=g['close'].ffill(); g['quote_volume']=g['quote_volume'].fillna(0); g['trades']=g['trades'].fillna(0)
    g['r5']=np.log(g['close']/g['close'].shift(5)).abs()
    g['qv5']=g['quote_volume'].rolling(5).sum(); g['tr5']=g['trades'].rolling(5).sum()
    gd=g[(g.index>=e.t.normalize())&(g.index<e.t.normalize()+pd.Timedelta(days=1))]
    t=e.t
    r5=g.at[t,'r5']; qv5=g.at[t,'qv5']; tr5=g.at[t,'tr5']
    pct=(gd['r5']<r5).mean()*100                       # percentile of this 5-min move within the day
    move_vs_median=r5/max(gd['r5'].median(),1e-12)
    vol_vs_median=qv5/max(gd['qv5'].median(),1e-12)
    ref=g['close'].shift(5).at[t]; px=g.at[t,'close']
    after5=g['close'].get(t+pd.Timedelta(minutes=5), np.nan)
    kept=(after5-ref)/(px-ref) if px!=ref else np.nan   # share of the move still there 5 min later
    C1 = (pct>=99 and move_vs_median>=5) or move_vs_median>=15   # extreme days: later moves push early ones below p99
    C2 = vol_vs_median>=5
    artifact = tr5<20 or (kept<0.2 and vol_vs_median<3)
    if C1 and C2 and not artifact: pre='1'
    elif (not C1 and pct<95 and move_vs_median<10) or (not C2 and vol_vs_median<2.5) or artifact: pre='0'
    else: pre='review'
    why=[]
    why.append(f"move is {pct:.1f}th pct of the day ({move_vs_median:.1f}x median 5-min move)")
    why.append(f"volume {vol_vs_median:.1f}x day median")
    why.append(f"{int(tr5)} trades in 5 min")
    if not np.isnan(kept): why.append(f"{kept*100:.0f}% of the move still there 5 min later")
    out.append(dict(file=e.file,row=e.row,symbol=e.symbol,minute=e.detected_minute_utc,scope=e.scope,
        breadth=e.market_breadth,chg=e.price_change_5m_pct,z=e.z_score,vr=e.volume_ratio,
        pct=round(pct,1),mvm=round(move_vs_median,1),vvm=round(vol_vs_median,1),tr5=int(tr5),
        kept=None if np.isnan(kept) else round(kept,2),C1=C1,C2=C2,artifact=artifact,prelabel=pre,why='; '.join(why)))
o=pd.DataFrame(out); o.to_csv('prelabels.csv',index=False)
print(o.groupby(['file','prelabel']).size().unstack(fill_value=0))
print(o[['pct','mvm','vvm','tr5','kept']].describe().round(2))
