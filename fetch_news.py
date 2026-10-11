#!/usr/bin/env python3
"""全球產業新聞網：抓取中央社、政府/法人新聞稿、國際媒體與 AI 官方部落格，分類後寫入 news_db.json
程式只放暫時摘要（summary_ok=False，不上線）；每則須依原文寫 50～200 字繁中摘要後以 apply_summaries.py 寫回。
用法: python3 fetch_news.py [天數，預設3]"""
import sys, json, re, time, html, datetime as dt, urllib.parse, concurrent.futures as cf, ssl
import feedparser, requests, trafilatura
from requests.adapters import HTTPAdapter
class _SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *a, **k):
        ctx = ssl.create_default_context(); ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
        k['ssl_context'] = ctx; return super().init_poolmanager(*a, **k)
_S = requests.Session(); _S.mount('https://', _SSLAdapter())
from googlenewsdecoder import gnewsdecoder
from pubdate import extract as extract_pubdate, text_of
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 3
MAX_NEW = int(sys.argv[2]) if len(sys.argv) > 2 else 60   # 每次最多新增候選（摘要要逐則寫）
DB = 'news_db.json'; STATUS = 'source_status.json'
UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36', 'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8'}
CATS = ['台灣產業', '全球AI', '全球產業趨勢', '政策與補助']
# 名稱: (RSS 網址, 預設分類, 單位名稱)；預設分類 None 代表依內容判斷
RSS = {
 '中央社-產經': ('https://feeds.feedburner.com/rsscna/finance', None, '中央社'),
 '中央社-科技': ('https://feeds.feedburner.com/rsscna/technology', None, '中央社'),
 '國科會': ('https://www.nstc.gov.tw/rss?language=ch', '政策與補助', '國科會'),
 'TechCrunch AI': ('https://techcrunch.com/category/artificial-intelligence/feed/', '全球AI', 'TechCrunch'),
 'The Verge AI': ('https://www.theverge.com/rss/ai-artificial-intelligence/index.xml', '全球AI', 'The Verge'),
 'MIT Technology Review': ('https://www.technologyreview.com/topic/artificial-intelligence/feed', '全球AI', 'MIT Technology Review'),
 'OpenAI': ('https://openai.com/news/rss.xml', '全球AI', 'OpenAI'),
 'Google AI Blog': ('https://blog.google/technology/ai/rss/', '全球AI', 'Google'),
 'Google DeepMind': ('https://deepmind.google/blog/rss.xml', '全球AI', 'Google DeepMind'),
 'NVIDIA Blog': ('https://blogs.nvidia.com/feed/', '全球AI', 'NVIDIA'),
}
# Google 新聞搜尋（限定權威來源）: (查詢, 語言, 預設分類)
ZH = ('hl=zh-TW&gl=TW&ceid=TW:zh-Hant'); EN = ('hl=en-US&gl=US&ceid=US:en')
GQ = [
 ('(半導體 OR 晶圓 OR 封測) site:cna.com.tw', ZH, '台灣產業'),
 ('(伺服器 OR 電子 OR 通訊 OR 面板) 產業 site:cna.com.tw', ZH, '台灣產業'),
 ('(工具機 OR 鋼鐵 OR 石化 OR 紡織 OR 汽車零組件 OR 傳產) site:cna.com.tw', ZH, '台灣產業'),
 ('(生技 OR 醫材 OR 新藥 OR 醫療器材) site:cna.com.tw', ZH, '台灣產業'),
 ('(淨零 OR 綠能 OR 儲能 OR 離岸風電 OR 太陽光電 OR 碳費) site:cna.com.tw', ZH, '台灣產業'),
 ('(農業 OR 農產品 OR 食品 OR 外銷) 農業部 site:cna.com.tw', ZH, '台灣產業'),
 ('(觀光 OR 文化內容 OR 旅宿 OR 文創) 產業 site:cna.com.tw', ZH, '台灣產業'),
 ('(零售 OR 餐飲 OR 物流 OR 服務業 OR 電商) site:cna.com.tw', ZH, '台灣產業'),
 ('(關稅 OR 供應鏈 OR 全球經濟 OR 出口) site:cna.com.tw', ZH, '全球產業趨勢'),
 ('(人工智慧 OR AI) site:cna.com.tw', ZH, '全球AI'),
 ('site:moea.gov.tw', ZH, '政策與補助'),
 ('site:sme.gov.tw', ZH, '政策與補助'),
 ('site:moeaidb.gov.tw', ZH, '政策與補助'),
 ('site:nstc.gov.tw', ZH, '政策與補助'),
 ('site:moda.gov.tw', ZH, '政策與補助'),
 ('site:ndc.gov.tw', ZH, '政策與補助'),
 ('site:ey.gov.tw (產業 OR 經濟 OR AI OR 補助)', ZH, '政策與補助'),
 ('(補助 OR 獎勵 OR 計畫 徵件) (經濟部 OR 國科會 OR 數發部) site:cna.com.tw', ZH, '政策與補助'),
 ('(補助 OR 獎勵 OR 徵件 OR 申請) (企業 OR 產業 OR 中小企業) site:cna.com.tw', ZH, '政策與補助'),
 ('(國發會 OR 國發基金 OR 產發署 OR 中小及新創企業署 OR 經濟部) 政策 site:cna.com.tw', ZH, '政策與補助'),
 ('(農業部 OR 農糧署 OR 漁業署) site:cna.com.tw', ZH, '台灣產業'),
 ('site:moa.gov.tw', ZH, '台灣產業'),
 ('(能源署 OR 台電 OR 綠電 OR 地熱 OR 氫能) site:cna.com.tw', ZH, '台灣產業'),
 ('site:itri.org.tw', ZH, '台灣產業'),
 ('site:iii.org.tw', ZH, '台灣產業'),
 ('site:tier.org.tw', ZH, '全球產業趨勢'),
 ('artificial intelligence site:apnews.com', EN, '全球AI'),
 ('AI site:reuters.com', EN, '全球AI'),
 ('(AI chips OR data center OR GPU) site:reuters.com', EN, '全球AI'),
 ('(AI regulation OR AI law OR AI Act) site:reuters.com OR site:apnews.com', EN, '全球AI'),
 ('(supply chain OR tariffs OR manufacturing) industry site:reuters.com', EN, '全球產業趨勢'),
 ('(semiconductor OR EV OR energy) industry outlook site:apnews.com', EN, '全球產業趨勢'),
 ('site:anthropic.com/news', EN, '全球AI'),
 ('site:about.fb.com/news AI', EN, '全球AI'),
]
SUB = [  # (子分類, 關鍵字)
 ('AI晶片與算力', r'AI晶片|GPU|算力|資料中心|data center|chip|Nvidia|輝達|超級電腦|HBM|AI伺服器|compute'),
 ('AI政策法規', r'法規|監管|立法|regulat|law\b|lawsuit|訴訟|AI Act|policy|政策|禁令|ban\b|safety'),
 ('模型與公司動態', r'模型|model|GPT|Gemini|Claude|Llama|OpenAI|Anthropic|DeepMind|xAI|募資|funding|valuation|估值|收購|acqui|launch|release|推出|發表'),
 ('半導體', r'半導體|晶圓|台積電|封測|封裝|IC設計|聯電|日月光|晶片|先進製程|semiconductor'),
 ('電子資通', r'伺服器|電子|通訊|5G|面板|PC|筆電|手機|鴻海|廣達|緯創|網通|光通訊|PCB|印刷電路'),
 ('生技醫療', r'生技|醫材|新藥|醫療|製藥|醫院|藥'),
 ('能源淨零', r'淨零|綠能|儲能|風電|光電|碳|電力|台電|能源|氫|減碳|ESG'),
 ('食品農業', r'農業|農產|食品|漁|畜|糧|茶|水果|農委|農業部'),
 ('文化觀光', r'觀光|旅遊|旅宿|文化|文創|影視|內容產業|遊客|航空'),
 ('傳產製造', r'工具機|鋼|石化|紡織|機械|汽車|零組件|製造|塑化|水泥|自行車|傳產'),
 ('服務業', r'零售|餐飲|物流|服務業|電商|百貨|金融|保險|銀行|房市|營建'),
]
AIK = re.compile(r'\bAI\b|人工智慧|生成式|大型語言|LLM|ChatGPT|OpenAI|Anthropic|Gemini|機器學習|machine learning|artificial intelligence|agent|代理', re.I)
INTL = re.compile(r'美國|美方|川普|歐盟|歐洲|中國|大陸|日本|南韓|韓國|印度|全球|國際|關稅|聯準會|德國|英國|東南亞|越南|墨西哥')
POLICY = re.compile(r'補助|獎勵|徵件|計畫|方案|政策|條例|行政院|經濟部|國科會|數發部|國發會|中小及新創|產發署|說明會|輔導')
NOISE = re.compile(r'今彩|大樂透|統一發票|車禍|自撞|命案|火警|詐騙|酒駕|\| 社會|\| 運動|\| 娛樂|圖輯|台股|股匯|收盤|開盤|加權指數|外資.*(買超|賣超)|匯率|新台幣.*(升|貶)|美股|殖利率|債券|期貨|ETF|漲停|跌停|除息|樂透|威力彩|房貸|天氣|颱風|地震')
REL = re.compile('|'.join(k for _, k in SUB) + r'|AI|人工智慧|產業|經濟|企業|投資|營收|出口|供應鏈|關稅|補助|新創|中小', re.I)
BAD_URL = re.compile(r'(cna\.com\.tw/(?:postwrite|business)/|kmweb\.moa|smelearning|\.pdf|\.docx?|\.xlsx?|\.odt|\.ods|/login|/apply|/download|Download\.ashx|/File/|/Attachment/|/tag/|/tags/|/category/|/topics?/|/search|/video|/videos/|/podcast|/careers|/jobs|/events?/?$|/about|faq|/privacy|/terms)', re.I)
BAD_TITLE = re.compile(r'(^\s*-|首頁$|專區|常見問答|問答集|錄取|徵才|招募|甄選|約用|採購|招標|會議紀錄|會議記錄|名單$|一覽表|下載|影音|影片|Podcast|Live Updates|Newsletter|Subscribe|Careers|Jobs at|Privacy|Cookie|Sitemap|^Home$|About us)', re.I)
def is_news(title, url): return not BAD_URL.search(url or '') and not BAD_TITLE.search(title or '')

def classify(title, desc, default):
    t = title + ' ' + (desc or '')
    if default == '全球AI' or (default is None and AIK.search(title)):
        cat = '全球AI'
    elif default == '政策與補助' or (default is None and POLICY.search(title) and re.search(r'補助|獎勵|徵件|計畫|方案|條例', title)):
        cat = '政策與補助'
    elif default:
        cat = default
    else:
        cat = '全球產業趨勢' if INTL.search(title) else '台灣產業'
    if cat == '全球AI':
        for s, k in SUB[:3]:
            if re.search(k, t, re.I): return cat, s
        return cat, 'AI應用'
    if cat == '台灣產業' and AIK.search(title): return '全球AI', 'AI應用'
    for s, k in SUB[3:]:
        if re.search(k, t, re.I): return cat, s
    return cat, '綜合'

UNIT = {'cna.com.tw':'中央社','moea.gov.tw':'經濟部','sme.gov.tw':'經濟部中小及新創企業署','moeaidb.gov.tw':'經濟部產業發展署','ida.gov.tw':'經濟部產業發展署',
 'nstc.gov.tw':'國科會','moda.gov.tw':'數位發展部','ndc.gov.tw':'國家發展委員會','ey.gov.tw':'行政院','itri.org.tw':'工研院','iii.org.tw':'資策會',
 'tier.org.tw':'台灣經濟研究院','reuters.com':'Reuters','apnews.com':'AP','techcrunch.com':'TechCrunch','theverge.com':'The Verge',
 'technologyreview.com':'MIT Technology Review','openai.com':'OpenAI','blog.google':'Google','deepmind.google':'Google DeepMind',
 'blogs.nvidia.com':'NVIDIA','nvidia.com':'NVIDIA','anthropic.com':'Anthropic','about.fb.com':'Meta','ai.meta.com':'Meta','moa.gov.tw':'農業部','mof.gov.tw':'財政部'}
ALLOWED = tuple(UNIT)
def unit_of(url, fallback=''):
    h = urllib.parse.urlparse(url).netloc.lower()
    for k, v in UNIT.items():
        if h == k or h.endswith('.' + k): return v
    return fallback or h

def fetch_page(url, title=''):
    """回傳 (原文開頭文字, 狀態, 原文發布日)"""
    try:
        r = _S.get(url, headers=UA, timeout=20)
        if r.status_code != 200: return None, f'HTTP {r.status_code}', None
        if not r.encoding or r.encoding.lower() == 'iso-8859-1': r.encoding = r.apparent_encoding
        h = r.text; pd = extract_pubdate(url, h, title)
        body = trafilatura.extract(h, include_comments=False, include_tables=False, favor_precision=True) or ''
        if len(body) < 80:
            m = re.search(r'<meta[^>]+(?:property|name)=["\'](?:og:)?description["\'][^>]+content=["\']([^"\']+)', h, re.I)
            body = (html.unescape(m.group(1)) if m else '') + ' ' + body
        return re.sub(r'\s+', ' ', body).strip()[:1500], 'ok', pd
    except Exception as e:
        return None, type(e).__name__, None

def tkey(t): return re.sub(r'[\W_]', '', re.sub(r'\|.*$|｜.*$', '', t)).lower()[:30]

def main():
    try: db = json.load(open(DB))
    except Exception: db = {}
    try: rejected = set(json.load(open('rejected.json')))
    except Exception: rejected = set()
    try: rej_urls = set(json.load(open('rejected_urls.json')))
    except Exception: rej_urls = set()
    status = {}; cand = []; cutoff = time.time() - DAYS * 86400
    for name, (u, cat, unit) in RSS.items():
        try:
            r = _S.get(u, headers=UA, timeout=25); f = feedparser.parse(r.content)
            if r.status_code != 200 or not f.entries: raise Exception(f'HTTP {r.status_code}, {len(f.entries)} 則')
            n = 0
            for e in f.entries:
                pub = e.get('published_parsed') or e.get('updated_parsed')
                if not pub or time.mktime(pub) < cutoff: continue
                if cat is None and not REL.search(e.title): continue
                cand.append(dict(title=html.unescape(e.title).strip(), url=e.link, cat=cat, unit=unit, src=name, desc=re.sub('<[^>]+>', '', e.get('summary', ''))[:300])); n += 1
            status[name] = {'ok': True, 'note': f'RSS {len(f.entries)} 則，期間內 {n} 則'}
        except Exception as ex:
            status[name] = {'ok': False, 'note': str(ex)[:120]}
    for q, lang, cat in GQ:
        n = 0
        try:
            url = f'https://news.google.com/rss/search?q={urllib.parse.quote(q + f" when:{DAYS}d")}&{lang}'
            f = feedparser.parse(_S.get(url, headers=UA, timeout=25).content)
            for e in f.entries[:25]:
                pub = e.get('published_parsed')
                if not pub or time.mktime(pub) < cutoff: continue
                t = re.sub(r'\s+[-|]\s+[^-|]+$', '', html.unescape(e.title)).strip()
                cand.append(dict(title=t, url=None, glink=e.link, cat=cat, unit='', src='Google新聞', desc='')); n += 1
            status[f'Google新聞：{q}'] = {'ok': True, 'note': f'候選 {n} 則'}
        except Exception as ex:
            status[f'Google新聞：{q}'] = {'ok': False, 'note': str(ex)[:100]}
        time.sleep(1)
    known = {tkey(v['title']) for v in db.values()} | {tkey(v.get('title_zh', '')) for v in db.values() if v.get('title_zh')} | {tkey(t) for t in rejected}
    seen = set(); todo = []
    for c in cand:
        if NOISE.search(c['title']) and c['cat'] != '全球AI': continue
        k = tkey(c['title'])
        if not k or k in seen or k in known or c.get('url') in db: continue
        seen.add(k); todo.append(c)
    # 各分類平均取，避免單一分類塞滿
    by = {}
    for c in todo: by.setdefault(c['cat'] or '_', []).append(c)
    todo = []
    while len(todo) < MAX_NEW * 2 and any(by.values()):
        for k in list(by):
            if by[k]: todo.append(by[k].pop(0))
    for c in todo:
        if c['url'] is None:
            try:
                d = gnewsdecoder(c['glink'], interval=1); c['url'] = d.get('decoded_url') if d.get('success') else None
            except Exception: c['url'] = None
    todo = [c for c in todo if c['url'] and c['url'] not in rej_urls and c['url'] not in db and unit_of(c['url']) in UNIT.values()]
    for c in todo:
        if not is_news(c['title'], c['url']): rejected.add(c['title'])
    todo = [c for c in todo if is_news(c['title'], c['url'])]
    fail_hosts = {}; no_date = 0; added = 0
    cutoff_date = (dt.date.today() - dt.timedelta(days=DAYS)).isoformat()
    with cf.ThreadPoolExecutor(8) as ex:
        for c, (body, st, pd) in zip(todo, ex.map(lambda c: fetch_page(c['url'], c['title']), todo)):
            host = urllib.parse.urlparse(c['url']).netloc
            if body is None: fail_hosts.setdefault(host, [0, st])[0] += 1; continue
            if not pd: no_date += 1; continue          # 發布日一律以原文為準，取不到不收
            if pd < cutoff_date: rejected.add(c['title']); continue
            if added >= MAX_NEW: break
            cat, sub = classify(c['title'], body[:300], c['cat'])
            en = not re.search(r'[\u4e00-\u9fff]', c['title'])
            db[c['url']] = dict(title=re.sub(r'\s*\|\s*\S{2,4}\s*$', '', c['title']), url=c['url'], date=pd, category=cat, topic=sub, unit=unit_of(c['url'], c['unit']),
                                lang='en' if en else 'zh', summary='', hint=body[:1200], summary_ok=False, added=dt.date.today().isoformat())
            added += 1
    json.dump(sorted(rejected), open('rejected.json', 'w'), ensure_ascii=False)
    json.dump(db, open(DB, 'w'), ensure_ascii=False, indent=0)
    status['_原文取不到發布日（未收錄）'] = {'ok': True, 'note': f'{no_date} 則'}
    status['_原文無法讀取的網站（未收錄）'] = {'ok': not fail_hosts, 'note': '; '.join(f'{h} {n}則({e})' for h, (n, e) in fail_hosts.items())}
    json.dump(dict(updated=dt.datetime.now().strftime('%Y-%m-%d %H:%M'), sources=status), open(STATUS, 'w'), ensure_ascii=False, indent=1)
    pend = [dict(url=u, title=v['title'], date=v['date'], unit=v['unit'], category=v['category'], topic=v['topic'], lang=v.get('lang'), hint=v.get('hint', '')) for u, v in db.items() if not v.get('summary_ok')]
    json.dump(pend, open('pending_summary.json', 'w'), ensure_ascii=False, indent=1)
    print('本次新增', added, '則；資料庫共', len(db), '則；待寫摘要（未上線）', len(pend), '則 → pending_summary.json')
if __name__ == '__main__':
    main()
