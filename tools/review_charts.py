import pandas as pd, numpy as np, matplotlib, base64, io, json
matplotlib.use('Agg'); import matplotlib.pyplot as plt, matplotlib.dates as mdates, matplotlib.ticker
B='/mnt/user-data/uploads/MarketPulse/data/backtest/'
ctx=pd.read_csv(B+'label_context.csv', parse_dates=['ts_utc'])
o=pd.read_csv('prelabels.csv', keep_default_na=False)
cards=[]
for _,e in o.iterrows():
    t=pd.Timestamp(e.minute)
    c=ctx[(ctx.file==e.file)&(ctx.row==e.row)&(ctx.ts_utc>=t-pd.Timedelta(hours=3))&(ctx.ts_utc<=t+pd.Timedelta(hours=3))].sort_values('ts_utc')
    fig,(a1,a2)=plt.subplots(2,1,figsize=(8.4,3.4),sharex=True,gridspec_kw=dict(height_ratios=[3,1.2],hspace=0.05),dpi=90)
    for a in (a1,a2):
        a.axvspan(t-pd.Timedelta(minutes=4),t+pd.Timedelta(minutes=1),color='#f5a623',alpha=0.35,lw=0)
        a.spines[['top','right']].set_visible(False); a.tick_params(labelsize=7.5,colors='#555')
    a1.fill_between(c.ts_utc,c.low,c.high,color='#4a7bd0',alpha=0.18,lw=0)
    a1.plot(c.ts_utc,c.close,color='#1f4e9c',lw=0.9)
    a1.set_ylabel('price (USDT)',fontsize=7.5,color='#555')
    a2.bar(c.ts_utc,c.quote_volume,width=1/1440,color='#7a7a7a')
    a2.set_ylabel('USDT vol',fontsize=7.5,color='#555')
    a2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v,_: f'{v/1e6:.1f}M' if v>=1e6 else f'{v/1e3:.0f}K'))
    a1.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v,_: f'{v:.6g}'))
    a2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    a2.set_xlabel('UTC  (orange = flagged 5-min window)',fontsize=7.5,color='#555')
    buf=io.BytesIO(); fig.savefig(buf,format='png',bbox_inches='tight'); plt.close(fig)
    d=e.to_dict(); d['img']=base64.b64encode(buf.getvalue()).decode()
    cards.append(d)
json.dump(cards,open('cards.json','w'))
print(len(cards), round(sum(len(c['img']) for c in cards)/1e6,2),'MB')
