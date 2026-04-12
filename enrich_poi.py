#!/usr/bin/env python3
"""
多平台 POI 数据采集脚本 (高德 + 腾讯)
=====================================
高德搜索配额用完自动切腾讯，LBS 配额独立计算。

环境变量:
  export AMAP_KEY=你的高德Key          (必选其一)
  export TENCENT_KEY=你的腾讯位置服务Key (必选其一)

用法:
  python3 enrich_poi.py discover   — 搜索全市小区 (TODO: 未实现)
  python3 enrich_poi.py enrich     — POI 评分增强
  python3 enrich_poi.py lbs        — 仅坐标+通勤距离(不消耗搜索配额)
  python3 enrich_poi.py all        — 全部

配额:
  高德: 搜索5000/月(关键字+周边共享), LBS 150000/月
  腾讯: 搜索6000/月(关键字), 周边推荐6000/月, LBS 6000/月
"""

import requests, csv, os, sys, time, json

# ============================================================
# Key 配置 (从环境变量读取，严禁硬编码)
# ============================================================
AMAP_KEY = os.environ.get('AMAP_KEY', '')
TENCENT_KEY = os.environ.get('TENCENT_KEY', '')

if not AMAP_KEY and not TENCENT_KEY:
    print("错误: 至少设置一个 Key")
    print("  export AMAP_KEY=xxx")
    print("  export TENCENT_KEY=xxx")
    sys.exit(1)

RATE = 0.35  # 并发上限 3次/秒

# 核心坐标
LUJIAZUI = (121.5018, 31.2353)
NANJING_RD = (121.4737, 31.2304)
ZHANGJIANG = (121.5906, 31.2035)

# 配额计数
quota = {'amap_search': 0, 'amap_lbs': 0, 'tencent_search': 0, 'tencent_lbs': 0}
QUOTA_FILE = 'data/.poi_quota.json'

def load_quota():
    try:
        with open(QUOTA_FILE) as f:
            d = json.load(f)
            import datetime
            if d.get('month') == datetime.date.today().strftime('%Y-%m'):
                quota.update(d.get('counts', {}))
                print(f"  本月已用: 高德搜索{quota['amap_search']}/5000, 腾讯搜索{quota['tencent_search']}/6000")
    except: pass

def save_quota():
    import datetime
    os.makedirs('data', exist_ok=True)
    with open(QUOTA_FILE, 'w') as f:
        json.dump({'month': datetime.date.today().strftime('%Y-%m'), 'counts': quota}, f)


# ============================================================
# 高德 API
# ============================================================
def amap_get(endpoint, params):
    if not AMAP_KEY: return None
    params['key'] = AMAP_KEY
    try:
        r = requests.get(f"https://restapi.amap.com/v3/{endpoint}", params=params, timeout=10)
        d = r.json()
        if d.get('status') == '1': return d
        if d.get('infocode') == '10044':  # 超配额
            print("  [高德] 配额已耗尽")
            return None
    except: pass
    return None

def amap_search(keyword, types=''):
    if quota['amap_search'] >= 4800: return None  # 预留
    p = {'keywords': keyword, 'city': '上海', 'citylimit': 'true', 'offset': 1}
    if types: p['types'] = types
    d = amap_get('place/text', p)
    if d: quota['amap_search'] += 1
    return d

def amap_around(lng, lat, types, radius=3000):
    if quota['amap_search'] >= 4800: return None
    d = amap_get('place/around', {'location': f"{lng},{lat}", 'types': types, 'radius': radius, 'offset': 1, 'page': 1})
    if d: quota['amap_search'] += 1
    return d

def amap_geocode(address):
    d = amap_get('geocode/geo', {'address': address, 'city': '上海'})
    if d:
        quota['amap_lbs'] += 1
        if d.get('geocodes'):
            lng, lat = d['geocodes'][0]['location'].split(',')
            return float(lng), float(lat)
    return None, None

def amap_drive(olng, olat, dlng, dlat):
    d = amap_get('direction/driving', {'origin': f"{olng},{olat}", 'destination': f"{dlng},{dlat}", 'strategy': 0})
    if d:
        quota['amap_lbs'] += 1
        if d.get('route', {}).get('paths'):
            p = d['route']['paths'][0]
            return int(p.get('distance', 0)), round(int(p.get('duration', 0)) / 60, 1)
    return 0, 0

def amap_transit(olng, olat, dlng, dlat):
    d = amap_get('direction/transit/integrated', {'origin': f"{olng},{olat}", 'destination': f"{dlng},{dlat}", 'city': '上海', 'strategy': 0})
    if d:
        quota['amap_lbs'] += 1
        if d.get('route', {}).get('transits'):
            return round(int(d['route']['transits'][0].get('duration', 0)) / 60, 1)
    return 0


# ============================================================
# 腾讯 API
# ============================================================
def tencent_get(endpoint, params):
    if not TENCENT_KEY: return None
    params['key'] = TENCENT_KEY
    try:
        r = requests.get(f"https://apis.map.qq.com{endpoint}", params=params, timeout=10)
        d = r.json()
        if d.get('status') == 0: return d
        if d.get('status') == 120:  # 超配额
            print("  [腾讯] 配额已耗尽")
            return None
    except: pass
    return None

def tencent_search(keyword, types=''):
    if quota['tencent_search'] >= 5500: return None
    p = {'keyword': keyword, 'boundary': 'region(上海,0)', 'page_size': 1, 'page_index': 1}
    if types: p['filter'] = f'category={types}'
    d = tencent_get('/ws/place/v1/search', p)
    if d: quota['tencent_search'] += 1
    return d

def tencent_around(lng, lat, keyword, radius=3000):
    if quota['tencent_search'] >= 5500: return None
    d = tencent_get('/ws/place/v1/explore', {
        'boundary': f'nearby({lat},{lng},{radius})',
        'keyword': keyword, 'page_size': 1, 'page_index': 1
    })
    if d: quota['tencent_search'] += 1
    return d

def tencent_geocode(address):
    d = tencent_get('/ws/geocoder/v1/', {'address': address})
    if d:
        quota['tencent_lbs'] += 1
        loc = d.get('result', {}).get('location', {})
        if loc: return loc.get('lng'), loc.get('lat')
    return None, None

def tencent_drive(olng, olat, dlng, dlat):
    d = tencent_get('/ws/direction/v1/driving/', {'from': f"{olat},{olng}", 'to': f"{dlat},{dlng}"})
    if d:
        quota['tencent_lbs'] += 1
        routes = d.get('result', {}).get('routes', [])
        if routes:
            return int(routes[0].get('distance', 0)), round(int(routes[0].get('duration', 0)) / 60, 1)
    return 0, 0

def tencent_transit(olng, olat, dlng, dlat):
    d = tencent_get('/ws/direction/v1/transit/', {'from': f"{olat},{olng}", 'to': f"{dlat},{dlng}", 'policy': 'LEAST_TIME'})
    if d:
        quota['tencent_lbs'] += 1
        routes = d.get('result', {}).get('routes', [])
        if routes:
            return round(int(routes[0].get('duration', 0)) / 60, 1)
    return 0


# ============================================================
# 统一接口 (自动选平台)
# ============================================================
def uni_geocode(address):
    """地理编码 — 优先高德(配额多)，失败用腾讯"""
    lng, lat = amap_geocode(address)
    if lng: return lng, lat, 'amap'
    lng, lat = tencent_geocode(address)
    if lng: return lng, lat, 'tencent'
    return None, None, None

def uni_drive(olng, olat, dlng, dlat):
    """驾车距离 — 优先高德"""
    d, t = amap_drive(olng, olat, dlng, dlat)
    if d > 0: return d, t
    return tencent_drive(olng, olat, dlng, dlat)

def uni_transit(olng, olat, dlng, dlat):
    """公交时间 — 优先高德"""
    t = amap_transit(olng, olat, dlng, dlat)
    if t > 0: return t
    return tencent_transit(olng, olat, dlng, dlat)

def uni_around_count(lng, lat, amap_type, tencent_keyword, radius=3000):
    """周边搜索数量 — 高德配额够用高德，否则腾讯"""
    if quota['amap_search'] < 4800:
        d = amap_around(lng, lat, amap_type, radius)
        time.sleep(RATE)
        if d: return int(d.get('count', 0))
    # Fallback: 腾讯
    d = tencent_around(lng, lat, tencent_keyword, radius)
    time.sleep(RATE)
    if d: return d.get('count', 0)
    return 0

def uni_nearest(lng, lat, amap_type, tencent_keyword, radius=10000):
    """最近POI距离+名称"""
    if quota['amap_search'] < 4800:
        d = amap_get('place/around', {
            'location': f"{lng},{lat}", 'types': amap_type,
            'radius': radius, 'sortrule': 'distance', 'offset': 1, 'page': 1
        })
        if d:
            quota['amap_search'] += 1
            if d.get('pois'):
                return int(d['pois'][0].get('distance', 99999)), d['pois'][0].get('name', '')
    # Fallback
    d = tencent_around(lng, lat, tencent_keyword, radius)
    if d and d.get('data'):
        poi = d['data'][0]
        loc = poi.get('location', {})
        if loc:
            import math
            dy = (loc['lat'] - lat) * 111320
            dx = (loc['lng'] - lng) * 111320 * math.cos(math.radians(lat))
            dist = int(math.sqrt(dx*dx + dy*dy))
            return dist, poi.get('title', '')
    return 99999, ''


# ============================================================
# 模式: LBS-only (坐标+通勤，不消耗搜索配额)
# ============================================================
def run_lbs():
    print("=" * 60)
    print("模式: LBS-only (坐标+通勤距离)")
    print("  不消耗搜索配额，仅用地理编码+路径规划")
    print("=" * 60)
    load_quota()

    with open('shanghai_communities.csv', encoding='utf-8-sig') as f:
        comms = [(r['区'], r['小区名']) for r in csv.DictReader(f)]
    seen = set(); unique = []
    for d, n in comms:
        if n not in seen: seen.add(n); unique.append((d, n))

    HEADS = ['小区名', '区', '经度_GCJ02', '纬度_GCJ02',
             '到陆家嘴驾车距离(m)', '到陆家嘴驾车时间(min)',
             '到南京东路驾车距离(m)', '到南京东路驾车时间(min)',
             '到张江驾车距离(m)', '到张江驾车时间(min)',
             '到陆家嘴公交时间(min)', '数据来源', '数据状态']

    results = []
    for i, (dist, name) in enumerate(unique):
        if i % 50 == 0:
            print(f"  {i}/{len(unique)}", flush=True)

        row = {'小区名': name, '区': dist}
        lng, lat, src = uni_geocode(f"上海市{dist}{name}")
        time.sleep(RATE)

        if not lng:
            row['数据状态'] = '未找到'
            results.append(row)
            continue

        row['经度_GCJ02'] = lng; row['纬度_GCJ02'] = lat

        dd, dt = uni_drive(lng, lat, *LUJIAZUI); time.sleep(RATE)
        row['到陆家嘴驾车距离(m)'] = dd; row['到陆家嘴驾车时间(min)'] = dt

        dd, dt = uni_drive(lng, lat, *NANJING_RD); time.sleep(RATE)
        row['到南京东路驾车距离(m)'] = dd; row['到南京东路驾车时间(min)'] = dt

        dd, dt = uni_drive(lng, lat, *ZHANGJIANG); time.sleep(RATE)
        row['到张江驾车距离(m)'] = dd; row['到张江驾车时间(min)'] = dt

        row['到陆家嘴公交时间(min)'] = uni_transit(lng, lat, *LUJIAZUI); time.sleep(RATE)
        row['数据来源'] = src; row['数据状态'] = 'LBS实测'
        results.append(row)

    with open('shanghai_gaode_poi.csv', 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=HEADS, extrasaction='ignore')
        w.writeheader(); w.writerows(results)

    save_quota()
    real = sum(1 for r in results if r.get('数据状态') == 'LBS实测')
    print(f"\n完成: {real}/{len(unique)} 实测")
    print(f"配额: 高德LBS {quota['amap_lbs']}, 腾讯LBS {quota['tencent_lbs']}")


# ============================================================
# 模式: 完整 POI 增强 (消耗搜索配额)
# ============================================================
def run_enrich():
    print("=" * 60)
    print("模式: 完整 POI 增强 (搜索+LBS)")
    print("=" * 60)
    load_quota()
    print(f"  高德搜索配额: {5000-quota['amap_search']} 剩余")
    print(f"  腾讯搜索配额: {6000-quota['tencent_search']} 剩余")

    with open('shanghai_communities.csv', encoding='utf-8-sig') as f:
        comms = [(r['区'], r['小区名']) for r in csv.DictReader(f)]
    seen = set(); unique = []
    for d, n in comms:
        if n not in seen: seen.add(n); unique.append((d, n))

    total_search = (5000 - quota['amap_search']) + (6000 - quota['tencent_search'])
    max_comms = total_search // 18
    print(f"  总搜索余量: {total_search}, 最多处理: {max_comms} 个小区")

    HEADS = ['小区名', '区', '高德地址', '经度_GCJ02', '纬度_GCJ02',
             '1km内地铁站数', '最近地铁站', '最近地铁距离(m)', '3km内地铁站数',
             '500m内公交站数', '3km内高架入口数', '最近高架入口距离(m)',
             '到陆家嘴驾车距离(m)', '到陆家嘴驾车时间(min)',
             '到南京东路驾车距离(m)', '到南京东路驾车时间(min)',
             '到张江驾车距离(m)', '到张江驾车时间(min)',
             '到陆家嘴公交时间(min)',
             '3km内商场数', '最近商场距离(m)', '最近商场名',
             '1km内超市菜场数', '最近菜场距离(m)', '最近菜场名',
             '3km内小学数', '最近小学距离(m)', '最近小学名',
             '3km内中学数', '最近中学距离(m)', '最近中学名',
             '5km内医院数', '最近三甲医院距离(m)', '最近三甲医院名',
             '3km内公园数', '最近公园距离(m)', '最近公园名',
             '数据来源', '数据状态']

    results = []
    for i, (dist, name) in enumerate(unique[:max_comms]):
        if i % 20 == 0:
            print(f"  {i}/{max_comms} (高德搜索{quota['amap_search']}, 腾讯搜索{quota['tencent_search']})", flush=True)

        row = {'小区名': name, '区': dist}
        lng, lat, src = uni_geocode(f"上海市{dist}{name}")
        time.sleep(RATE)
        if not lng:
            row['数据状态'] = '未找到'; results.append(row); continue

        row['经度_GCJ02'] = lng; row['纬度_GCJ02'] = lat

        # 搜索类
        row['1km内地铁站数'] = uni_around_count(lng, lat, '150500', '地铁站', 1000)
        row['3km内地铁站数'] = uni_around_count(lng, lat, '150500', '地铁站', 3000)
        d, n = uni_nearest(lng, lat, '150500', '地铁站'); row['最近地铁距离(m)'] = d; row['最近地铁站'] = n
        row['500m内公交站数'] = uni_around_count(lng, lat, '150700', '公交站', 500)
        row['3km内高架入口数'] = uni_around_count(lng, lat, '190301|190302', '高架入口', 3000)
        d, _ = uni_nearest(lng, lat, '190301|190302', '高架入口'); row['最近高架入口距离(m)'] = d

        # LBS: 驾车/公交
        dd, dt = uni_drive(lng, lat, *LUJIAZUI); time.sleep(RATE)
        row['到陆家嘴驾车距离(m)'] = dd; row['到陆家嘴驾车时间(min)'] = dt
        dd, dt = uni_drive(lng, lat, *NANJING_RD); time.sleep(RATE)
        row['到南京东路驾车距离(m)'] = dd; row['到南京东路驾车时间(min)'] = dt
        dd, dt = uni_drive(lng, lat, *ZHANGJIANG); time.sleep(RATE)
        row['到张江驾车距离(m)'] = dd; row['到张江驾车时间(min)'] = dt
        row['到陆家嘴公交时间(min)'] = uni_transit(lng, lat, *LUJIAZUI); time.sleep(RATE)

        # 商业
        row['3km内商场数'] = uni_around_count(lng, lat, '060100', '商场购物中心', 3000)
        d, n = uni_nearest(lng, lat, '060100', '商场'); row['最近商场距离(m)'] = d; row['最近商场名'] = n
        row['1km内超市菜场数'] = uni_around_count(lng, lat, '060400|080600', '超市菜场', 1000)
        d, n = uni_nearest(lng, lat, '080600', '菜市场'); row['最近菜场距离(m)'] = d; row['最近菜场名'] = n

        # 教育
        row['3km内小学数'] = uni_around_count(lng, lat, '141203', '小学', 3000)
        d, n = uni_nearest(lng, lat, '141203', '小学'); row['最近小学距离(m)'] = d; row['最近小学名'] = n
        row['3km内中学数'] = uni_around_count(lng, lat, '141204', '中学', 3000)
        d, n = uni_nearest(lng, lat, '141204', '中学'); row['最近中学距离(m)'] = d; row['最近中学名'] = n

        # 医疗
        row['5km内医院数'] = uni_around_count(lng, lat, '090100', '医院', 5000)
        d, n = uni_nearest(lng, lat, '090101', '三甲医院'); row['最近三甲医院距离(m)'] = d; row['最近三甲医院名'] = n

        # 公园
        row['3km内公园数'] = uni_around_count(lng, lat, '110101', '公园', 3000)
        d, n = uni_nearest(lng, lat, '110101', '公园'); row['最近公园距离(m)'] = d; row['最近公园名'] = n

        row['数据来源'] = src; row['数据状态'] = '实测'
        results.append(row)

    # Placeholder for remaining
    processed = {r['小区名'] for r in results}
    for dist, name in unique:
        if name not in processed:
            results.append({'小区名': name, '区': dist, '数据状态': 'placeholder_待补充'})

    with open('shanghai_gaode_poi.csv', 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=HEADS, extrasaction='ignore')
        w.writeheader(); w.writerows(results)

    save_quota()
    real = sum(1 for r in results if r.get('数据状态') == '实测')
    print(f"\n完成: {real} 实测, {len(unique)-real} placeholder")


# ============================================================
# 入口
# ============================================================
def main():
    print("上海小区 POI 数据采集 (高德+腾讯双平台)")
    print(f"  高德 Key: {'✓' if AMAP_KEY else '✗'}")
    print(f"  腾讯 Key: {'✓' if TENCENT_KEY else '✗'}")

    if len(sys.argv) < 2:
        print("\n用法:")
        print("  python3 enrich_poi.py lbs      — 仅坐标+通勤(不耗搜索配额)")
        print("  python3 enrich_poi.py enrich   — 完整POI增强(消耗搜索配额)")
        return

    mode = sys.argv[1]
    if mode == 'lbs': run_lbs()
    elif mode == 'enrich': run_enrich()
    elif mode == 'all': run_lbs(); run_enrich()
    else: print(f"未知模式: {mode}")

if __name__ == "__main__":
    main()
