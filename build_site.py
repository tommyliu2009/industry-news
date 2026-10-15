#!/usr/bin/env python3
"""由 news_db.json 產生 data.js。只輸出已有合格摘要（summary_ok 且 50～200 字）的新聞；只保留近 60 天。"""
import json, datetime as dt
db = json.load(open('news_db.json')); st = json.load(open('source_status.json'))
import sys as _s; _s.path.insert(0, '/workspace/shared-data'); import news_filter  # 共用過濾：刪檔名／網址／行政公告等，修正標題
db = news_filter.clean_db(db, 'news_db.json', site='industry-news')
cut = (dt.date.today() - dt.timedelta(days=60)).isoformat()
ok = [v for v in db.values() if v.get('summary_ok') and 50 <= len(v.get('summary', '')) <= 200 and v['date'] >= cut]
items = sorted(ok, key=lambda v: (v['date'], v.get('added', '')), reverse=True)
K = ('title', 'title_zh', 'url', 'date', 'category', 'topic', 'unit', 'summary', 'also')
items = [{k: v[k] for k in K if v.get(k)} for v in items]
open('data.js', 'w').write('window.NEWS=' + json.dumps(items, ensure_ascii=False) + ';\nwindow.UPDATED=' + json.dumps(st['updated']) + ';\n')
print('網站資料', len(items), '則；未上線（待摘要）', sum(1 for v in db.values() if not v.get('summary_ok')), '則')
