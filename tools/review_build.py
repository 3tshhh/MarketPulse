import json
cards=json.load(open('cards.json'))
key={'label_z.csv':'z','label_market.csv':'m','label_fixed.csv':'f'}
data=[]
for c in cards:
    data.append(dict(k=f"{key[c['file']]}-{c['row']}",file=c['file'],row=int(c['row']),sym=c['symbol'],t=c['minute'],
        scope=c['scope'],br=int(c['breadth']),chg=float(c['chg']),z=float(c['z']),vr=float(c['vr']),
        pre=c['prelabel'],why=c['why'],img=c['img']))
js=json.dumps(data).replace('<','\\u003c')
html=open('template.html').read().replace('__DATA__',js)
open('label_review.html','w').write(html)
print(len(html)/1e6,'MB')
