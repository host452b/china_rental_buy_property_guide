#!/usr/bin/env python3
"""
高德开放平台 + 贝壳开放平台 数据采集/增强脚本
=============================================
Step 1: 高德搜小区 → 小区名/经纬度/行政区/板块
Step 2: 高德搜周边 → 地铁/医院/商场/学校/菜场 配套评分
Step 3: 贝壳拿成交 → 近期成交价/挂牌价/租金

环境变量:
  export AMAP_KEY=你的高德Web服务API密钥
  export BEIKE_APP_ID=你的贝壳开放平台AppID      (可选)
  export BEIKE_APP_SECRET=你的贝壳开放平台Secret  (可选)

申请:
  高德: https://lbs.amap.com/ → 控制台 → 应用管理 → 创建应用 → Web服务
  贝壳: https://open.ke.com/ → 注册开发者 → 创建应用 → 成交案例库API
"""

import requests
import csv
import os
import sys
import time
import json
import hashlib

# ============================================================
# 配置 (从环境变量读取)
# ============================================================
AMAP_KEY = os.environ.get('AMAP_KEY', '')
BEIKE_APP_ID = os.environ.get('BEIKE_APP_ID', '')
BEIKE_APP_SECRET = os.environ.get('BEIKE_APP_SECRET', '')

BASE_URL_AMAP = "https://restapi.amap.com/v3"
BASE_URL_BEIKE = "https://open.ke.com"

INPUT_FILE = "shanghai_communities.csv"
OUTPUT_FILE = "shanghai_communities.csv"
RATE_LIMIT = 0.25  # 每请求间隔(秒)

# POI 类型编码
POI = {
    'community': '120000',  # 住宅小区
    'metro':     '150500',  # 地铁站
    'hospital':  '090100',  # 综合医院
    'mall':      '060100',  # 购物中心
    'supermarket':'060400', # 超市
    'market':    '080600',  # 菜市场
    'school':    '141200',  # 学校
    'primary':   '141203',  # 小学
    'middle':    '141204',  # 中学
}


# ============================================================
# 高德 API
# ============================================================
def amap_get(endpoint, params):
    """通用高德请求"""
    params['key'] = AMAP_KEY
    try:
        resp = requests.get(f"{BASE_URL_AMAP}/{endpoint}", params=params, timeout=8)
        data = resp.json()
        if data.get('status') == '1':
            return data
    except Exception as e:
        print(f"  [AMAP ERROR] {e}")
    return None


def amap_search_communities(district, page=1):
    """搜索某区的所有住宅小区"""
    data = amap_get("place/text", {
        'types': POI['community'],
        'city': '上海',
        'citylimit': 'true',
        'keywords': district,
        'offset': 25,
        'page': page,
    })
    if data and data.get('pois'):
        return [{
            'name': p['name'],
            'lng': float(p['location'].split(',')[0]),
            'lat': float(p['location'].split(',')[1]),
            'address': p.get('address', ''),
            'district': p.get('adname', ''),
            'business': p.get('business', ''),  # 板块
        } for p in data['pois']], int(data.get('count', 0))
    return [], 0


def amap_geocode(address):
    """地址 → 经纬度"""
    data = amap_get("geocode/geo", {'address': address, 'city': '上海'})
    if data and data.get('geocodes'):
        loc = data['geocodes'][0]['location']
        lng, lat = loc.split(',')
        return float(lng), float(lat)
    return None, None


def amap_around(lng, lat, poi_type, radius=3000):
    """查询周边 POI 数量"""
    data = amap_get("place/around", {
        'location': f"{lng},{lat}",
        'types': poi_type,
        'radius': radius,
        'offset': 1,
        'page': 1,
    })
    return int(data.get('count', 0)) if data else 0


def amap_nearest(lng, lat, poi_type, radius=5000):
    """查找最近 POI 的距离(米)"""
    data = amap_get("place/around", {
        'location': f"{lng},{lat}",
        'types': poi_type,
        'radius': radius,
        'sortrule': 'distance',
        'offset': 1,
        'page': 1,
    })
    if data and data.get('pois'):
        return int(data['pois'][0].get('distance', 9999))
    return 9999


# ============================================================
# 贝壳开放平台 API
# ============================================================
def beike_sign(params):
    """贝壳签名算法"""
    if not BEIKE_APP_SECRET:
        return ''
    sorted_str = '&'.join(f"{k}={v}" for k, v in sorted(params.items()))
    sign_str = f"{sorted_str}&app_secret={BEIKE_APP_SECRET}"
    return hashlib.md5(sign_str.encode()).hexdigest().upper()


def beike_get_deals(community_name, city_code="310000"):
    """从贝壳获取小区成交数据"""
    if not BEIKE_APP_ID:
        return None

    params = {
        'app_id': BEIKE_APP_ID,
        'timestamp': str(int(time.time())),
        'community_name': community_name,
        'city_code': city_code,
    }
    params['sign'] = beike_sign(params)

    try:
        resp = requests.get(f"{BASE_URL_BEIKE}/api/deal/list", params=params, timeout=8)
        data = resp.json()
        if data.get('errno') == 0:
            deals = data.get('data', {}).get('list', [])
            if deals:
                prices = [d['unit_price'] for d in deals if d.get('unit_price')]
                return {
                    'avg_deal_price': int(sum(prices) / len(prices)) if prices else 0,
                    'deal_count': len(deals),
                    'latest_deal': deals[0] if deals else None,
                }
    except Exception as e:
        print(f"  [BEIKE ERROR] {e}")
    return None


# ============================================================
# 评分函数
# ============================================================
def score_metro(count_1km, dist_m):
    if count_1km >= 4: return 10.0
    if count_1km >= 2: return 9.0
    if count_1km >= 1: return 8.0
    if dist_m <= 1500: return 7.0
    if dist_m <= 2000: return 6.0
    if dist_m <= 3000: return 5.0
    if dist_m <= 5000: return 3.5
    return 2.0

def score_medical(h_5km, h3a_5km):
    return round(min(10, max(1, h_5km * 1.0 + h3a_5km * 2.0)), 1)

def score_commercial(mall_3km):
    return round(min(10, max(1, mall_3km * 1.2)), 1)

def score_grocery(market_1km, super_1km):
    t = market_1km + super_1km
    if t >= 8: return 10.0
    if t >= 5: return 9.0
    if t >= 3: return 8.0
    if t >= 1: return 6.5
    return 4.0

def score_education(school_3km, primary_3km):
    return round(min(10, max(1, school_3km * 0.3 + primary_3km * 1.0)), 1)


# ============================================================
# 模式 1: 从高德发现新小区
# ============================================================
def discover_communities():
    """从高德搜索上海所有区的住宅小区"""
    print("=" * 60)
    print("模式 1: 高德小区发现")
    print("=" * 60)

    districts = ['黄浦区','徐汇区','长宁区','静安区','普陀区','虹口区','杨浦区',
                 '浦东新区','闵行区','宝山区','嘉定区','松江区','青浦区','奉贤区','金山区','崇明区']

    all_communities = []
    for dist in districts:
        print(f"\n▶ {dist}")
        page = 1
        while True:
            communities, total = amap_search_communities(dist, page)
            if not communities:
                break
            all_communities.extend(communities)
            print(f"  Page {page}: {len(communities)} ({len(all_communities)}/{total})")
            page += 1
            time.sleep(RATE_LIMIT)
            if page > 40:  # 每区最多 1000 个
                break

    # 保存发现的小区
    output = "data/gaode_communities_raw.csv"
    os.makedirs("data", exist_ok=True)
    if all_communities:
        keys = ['name', 'lng', 'lat', 'address', 'district', 'business']
        with open(output, 'w', newline='', encoding='utf-8-sig') as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(all_communities)
    print(f"\n发现 {len(all_communities)} 个小区 → {output}")
    return all_communities


# ============================================================
# 模式 2: 增强现有数据的 POI 评分
# ============================================================
def enrich_scores():
    """为现有 CSV 中的小区增强 POI 评分"""
    print("=" * 60)
    print("模式 2: POI 评分增强")
    print("=" * 60)

    with open(INPUT_FILE, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    print(f"读取 {len(rows)} 个小区")
    updated = 0
    api_calls = 0

    for i, row in enumerate(rows):
        lng = float(row.get('经度', 0) or 0)
        lat = float(row.get('纬度', 0) or 0)

        if lng == 0 or lat == 0:
            # 尝试地理编码
            name = row.get('小区名', '')
            district = row.get('区', '')
            lng, lat = amap_geocode(f"上海市{district}{name}")
            api_calls += 1
            time.sleep(RATE_LIMIT)
            if lng and lat:
                row['经度'] = round(lng, 6)
                row['纬度'] = round(lat, 6)
            else:
                continue

        if i % 50 == 0:
            print(f"  进度: {i}/{len(rows)} ({updated} updated, {api_calls} API calls)")

        # POI 查询 (9 个请求/小区)
        metro_1km = amap_around(lng, lat, POI['metro'], 1000); api_calls += 1; time.sleep(RATE_LIMIT)
        metro_dist = amap_nearest(lng, lat, POI['metro'], 5000); api_calls += 1; time.sleep(RATE_LIMIT)
        hosp_5km = amap_around(lng, lat, POI['hospital'], 5000); api_calls += 1; time.sleep(RATE_LIMIT)
        mall_3km = amap_around(lng, lat, POI['mall'], 3000); api_calls += 1; time.sleep(RATE_LIMIT)
        market_1km = amap_around(lng, lat, POI['market'], 1000); api_calls += 1; time.sleep(RATE_LIMIT)
        super_1km = amap_around(lng, lat, POI['supermarket'], 1000); api_calls += 1; time.sleep(RATE_LIMIT)
        school_3km = amap_around(lng, lat, POI['school'], 3000); api_calls += 1; time.sleep(RATE_LIMIT)
        primary_3km = amap_around(lng, lat, POI['primary'], 3000); api_calls += 1; time.sleep(RATE_LIMIT)

        row['交通可达性(地铁)'] = score_metro(metro_1km, metro_dist)
        row['医疗水平'] = score_medical(hosp_5km, 0)
        row['5km商业综合指数'] = score_commercial(mall_3km)
        row['买菜便利度'] = score_grocery(market_1km, super_1km)
        row['教育资源指数'] = score_education(school_3km, primary_3km)

        src = row.get('数据来源', '')
        if '高德' not in src:
            row['数据来源'] = f"高德POI+{src}" if src else '高德POI'

        updated += 1

        if api_calls >= 4500:
            print(f"\n⚠️ 接近日限额 ({api_calls})，停止。明天继续。")
            break

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    print(f"\n完成: {updated} updated, {api_calls} API calls → {OUTPUT_FILE}")


# ============================================================
# 模式 3: 贝壳成交数据增强
# ============================================================
def enrich_beike():
    """从贝壳开放平台获取成交数据"""
    if not BEIKE_APP_ID:
        print("未配置 BEIKE_APP_ID，跳过贝壳数据。")
        print("申请: https://open.ke.com/")
        return

    print("=" * 60)
    print("模式 3: 贝壳成交数据增强")
    print("=" * 60)

    with open(INPUT_FILE, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    updated = 0
    for i, row in enumerate(rows):
        name = row.get('小区名', '')
        if i % 50 == 0:
            print(f"  进度: {i}/{len(rows)}")

        deal_data = beike_get_deals(name)
        time.sleep(RATE_LIMIT)

        if deal_data and deal_data['avg_deal_price'] > 0:
            row['2026成交均价'] = deal_data['avg_deal_price']
            src = row.get('数据来源', '')
            if '贝壳' not in src:
                row['数据来源'] = f"贝壳成交+{src}" if src else '贝壳成交'
            updated += 1

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    print(f"\n贝壳更新: {updated} 个小区 → {OUTPUT_FILE}")


# ============================================================
# 入口
# ============================================================
def main():
    print("上海小区数据增强工具")
    print(f"  AMAP_KEY: {'✓ 已配置' if AMAP_KEY else '✗ 未配置'}")
    print(f"  BEIKE_APP_ID: {'✓ 已配置' if BEIKE_APP_ID else '✗ 未配置 (可选)'}")
    print()

    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        print("用法:")
        print("  python enrich_with_gaode.py discover  — 从高德发现新小区")
        print("  python enrich_with_gaode.py enrich    — POI 评分增强")
        print("  python enrich_with_gaode.py beike     — 贝壳成交数据")
        print("  python enrich_with_gaode.py all       — 全部执行")
        return

    if mode in ('discover', 'all'):
        discover_communities()
    if mode in ('enrich', 'all'):
        enrich_scores()
    if mode in ('beike', 'all'):
        enrich_beike()


if __name__ == "__main__":
    main()
