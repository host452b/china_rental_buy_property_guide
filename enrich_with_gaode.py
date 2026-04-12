#!/usr/bin/env python3
"""
高德开放平台 + 贝壳开放平台 数据采集/增强脚本
=============================================
Step 1: 高德搜小区 → 小区名/经纬度/行政区/板块
Step 2: 高德搜周边 → 地铁/医院/商场/学校/菜场 配套评分
Step 3: 贝壳拿成交 → 近期成交价/挂牌价/租金

高德已开通服务:
  - 地理编码API       → 小区地址→经纬度
  - 逆地理编码API     → 经纬度→行政区/板块
  - 关键字搜索API     → 按区搜索住宅小区POI
  - 周边搜索API       → 搜周边地铁/医院/商场等
  - ID查询API         → POI详情
  - 输入提示API       → 小区名模糊匹配
  - 行政区划查询API   → 获取上海16区边界
  - 路径规划API       → 计算通勤距离/时间
  - 坐标转换API       → WGS84↔GCJ02
  - 天气查询API       → 可选：旅游场景用
  - 多边形搜索API     → 按板块边界搜索小区

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
RATE_LIMIT = 0.35  # 每请求间隔(秒), 并发上限3次/秒

# ============================================================
# 高德配额限制 (严格遵循)
# ============================================================
# 基础搜索服务: 关键字/周边/多边形/ID/输入提示 共享 5,000次/月
# 基础LBS服务: 地理编码/逆地理编码/路径规划/行政区 共享 150,000次/月
# 并发上限: 3次/秒 (所有服务)
QUOTA_SEARCH_MONTHLY = 5000   # 搜索类月配额
QUOTA_LBS_MONTHLY = 150000    # LBS类月配额
QUOTA_FILE = "data/.amap_quota.json"  # 配额计数文件

_search_calls = 0  # 本次运行搜索调用计数
_lbs_calls = 0     # 本次运行LBS调用计数

def load_quota():
    """加载本月已用配额"""
    global _search_calls, _lbs_calls
    try:
        with open(QUOTA_FILE, 'r') as f:
            data = json.load(f)
            import datetime
            if data.get('month') == datetime.date.today().strftime('%Y-%m'):
                _search_calls = data.get('search', 0)
                _lbs_calls = data.get('lbs', 0)
                print(f"  本月已用: 搜索 {_search_calls}/{QUOTA_SEARCH_MONTHLY}, LBS {_lbs_calls}/{QUOTA_LBS_MONTHLY}")
    except:
        pass

def save_quota():
    """保存配额计数"""
    import datetime
    os.makedirs("data", exist_ok=True)
    with open(QUOTA_FILE, 'w') as f:
        json.dump({
            'month': datetime.date.today().strftime('%Y-%m'),
            'search': _search_calls,
            'lbs': _lbs_calls,
        }, f)

def check_search_quota(n=1):
    """检查搜索配额是否够用"""
    global _search_calls
    if _search_calls + n > QUOTA_SEARCH_MONTHLY:
        print(f"\n⛔ 搜索配额已用完: {_search_calls}/{QUOTA_SEARCH_MONTHLY}")
        print("  下月重置。或升级配额: https://lbs.amap.com/")
        save_quota()
        return False
    return True

def check_lbs_quota(n=1):
    """检查LBS配额是否够用"""
    global _lbs_calls
    if _lbs_calls + n > QUOTA_LBS_MONTHLY:
        print(f"\n⛔ LBS配额已用完: {_lbs_calls}/{QUOTA_LBS_MONTHLY}")
        save_quota()
        return False
    return True

def track_search():
    global _search_calls
    _search_calls += 1

def track_lbs():
    global _lbs_calls
    _lbs_calls += 1

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
    """关键字搜索API: 搜索某区的住宅小区 [消耗搜索配额]"""
    if not check_search_quota(): return [], 0
    track_search()
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
    """地理编码API: 地址 → 经纬度 [消耗LBS配额]"""
    if not check_lbs_quota(): return None, None
    track_lbs()
    data = amap_get("geocode/geo", {'address': address, 'city': '上海'})
    if data and data.get('geocodes'):
        loc = data['geocodes'][0]['location']
        lng, lat = loc.split(',')
        return float(lng), float(lat)
    return None, None


def amap_regeo(lng, lat):
    """逆地理编码API: 经纬度 → 行政区/板块/地址"""
    data = amap_get("geocode/regeo", {
        'location': f"{lng},{lat}",
        'extensions': 'all',
    })
    if data and data.get('regeocode'):
        rg = data['regeocode']
        addr = rg.get('formatted_address', '')
        comp = rg.get('addressComponent', {})
        return {
            'address': addr,
            'district': comp.get('district', ''),
            'township': comp.get('township', ''),  # 街道/镇 ≈ 板块
            'neighborhood': comp.get('neighborhood', {}).get('name', ''),
            'building': comp.get('building', {}).get('name', ''),
        }
    return {}


def amap_input_tips(keyword, city='上海'):
    """输入提示API: 小区名模糊搜索，快速匹配"""
    data = amap_get("assistant/inputtips", {
        'keywords': keyword,
        'city': city,
        'datatype': 'poi',
    })
    if data and data.get('tips'):
        return [{
            'name': t.get('name', ''),
            'district': t.get('district', ''),
            'address': t.get('address', ''),
            'location': t.get('location', ''),
            'id': t.get('id', ''),
        } for t in data['tips'] if t.get('location')]
    return []


def amap_district(keywords='上海', subdistrict=1):
    """行政区划查询API: 获取上海16区列表及边界"""
    data = amap_get("config/district", {
        'keywords': keywords,
        'subdistrict': subdistrict,
        'extensions': 'base',
    })
    if data and data.get('districts'):
        districts = data['districts'][0].get('districts', [])
        return [{
            'name': d['name'],
            'adcode': d['adcode'],
            'center': d['center'],
            'level': d['level'],
        } for d in districts]
    return []


def amap_driving_distance(origin_lng, origin_lat, dest_lng, dest_lat):
    """路径规划API: 驾车距离和时间 (用于通勤评估)"""
    data = amap_get("direction/driving", {
        'origin': f"{origin_lng},{origin_lat}",
        'destination': f"{dest_lng},{dest_lat}",
        'strategy': 0,
    })
    if data and data.get('route', {}).get('paths'):
        path = data['route']['paths'][0]
        return {
            'distance_m': int(path.get('distance', 0)),
            'duration_s': int(path.get('duration', 0)),
        }
    return {'distance_m': 0, 'duration_s': 0}


def amap_transit_distance(origin_lng, origin_lat, dest_lng, dest_lat):
    """路径规划API: 公交/地铁距离和时间"""
    data = amap_get("direction/transit/integrated", {
        'origin': f"{origin_lng},{origin_lat}",
        'destination': f"{dest_lng},{dest_lat}",
        'city': '上海',
        'strategy': 0,
    })
    if data and data.get('route', {}).get('transits'):
        t = data['route']['transits'][0]
        return {
            'distance_m': int(data['route'].get('distance', 0)),
            'duration_s': int(t.get('duration', 0)),
            'walking_distance_m': int(t.get('walking_distance', 0)),
        }
    return {'distance_m': 0, 'duration_s': 0, 'walking_distance_m': 0}


def amap_polygon_search(polygon, poi_type, page=1):
    """多边形搜索API: 在指定区域边界内搜索POI"""
    data = amap_get("place/polygon", {
        'polygon': polygon,  # "lng1,lat1|lng2,lat2|lng3,lat3|..."
        'types': poi_type,
        'offset': 25,
        'page': page,
    })
    if data and data.get('pois'):
        return [{
            'name': p['name'],
            'location': p['location'],
            'address': p.get('address', ''),
        } for p in data['pois']], int(data.get('count', 0))
    return [], 0


def amap_poi_detail(poi_id):
    """ID查询API: 获取POI详情"""
    data = amap_get("place/detail", {
        'id': poi_id,
    })
    if data and data.get('pois'):
        return data['pois'][0]
    return {}


def amap_around(lng, lat, poi_type, radius=3000):
    """周边搜索API: 查询周边POI数量 [消耗搜索配额]"""
    if not check_search_quota(): return 0
    track_search()
    data = amap_get("place/around", {
        'location': f"{lng},{lat}",
        'types': poi_type,
        'radius': radius,
        'offset': 1,
        'page': 1,
    })
    return int(data.get('count', 0)) if data else 0


def amap_nearest(lng, lat, poi_type, radius=5000):
    """周边搜索API: 最近POI距离 [消耗搜索配额]"""
    if not check_search_quota(): return 9999
    track_search()
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
    """为现有 CSV 中的小区增强 POI 评分

    配额预算:
      搜索类 5000次/月, 每小区需 8次搜索 → 最多 625 个小区/月
      LBS类 150000次/月, 地理编码不限

    优化策略:
      - 跳过已有高德数据的小区
      - 合并地铁搜索(1次搜周边代替2次)
      - 每次运行自动保存进度
    """
    print("=" * 60)
    print("模式 2: POI 评分增强")
    print("=" * 60)

    load_quota()

    # 配额预算
    search_remaining = QUOTA_SEARCH_MONTHLY - _search_calls
    max_communities = search_remaining // 7  # 每小区约7次搜索(优化后)
    print(f"  搜索配额剩余: {search_remaining}/{QUOTA_SEARCH_MONTHLY}")
    print(f"  本次最多处理: {max_communities} 个小区")

    with open(INPUT_FILE, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)

    # 跳过已有高德数据的小区
    todo = [r for r in rows if '高德' not in r.get('数据来源', '')]
    print(f"  总计: {len(rows)}, 待处理: {len(todo)}, 已有高德数据: {len(rows)-len(todo)}")

    if max_communities <= 0:
        print("  ⛔ 搜索配额已用完，下月再试。")
        return

    updated = 0
    for row in todo[:max_communities]:
        lng = float(row.get('经度', 0) or 0)
        lat = float(row.get('纬度', 0) or 0)

        if lng == 0 or lat == 0:
            name = row.get('小区名', '')
            district = row.get('区', '')
            lng, lat = amap_geocode(f"上海市{district}{name}")
            time.sleep(RATE_LIMIT)
            if lng and lat:
                row['经度'] = round(lng, 6)
                row['纬度'] = round(lat, 6)
            else:
                continue

        if updated % 50 == 0:
            print(f"  进度: {updated}/{min(len(todo), max_communities)} (搜索{_search_calls}/{QUOTA_SEARCH_MONTHLY})")

        # 优化: 7次搜索/小区 (合并超市+菜场为一次)
        metro_1km = amap_around(lng, lat, POI['metro'], 1000); time.sleep(RATE_LIMIT)
        metro_dist = amap_nearest(lng, lat, POI['metro'], 5000); time.sleep(RATE_LIMIT)
        hosp_5km = amap_around(lng, lat, POI['hospital'], 5000); time.sleep(RATE_LIMIT)
        mall_3km = amap_around(lng, lat, POI['mall'], 3000); time.sleep(RATE_LIMIT)
        # 合并: 菜场+超市 一次搜索
        grocery_1km = amap_around(lng, lat, f"{POI['market']}|{POI['supermarket']}", 1000); time.sleep(RATE_LIMIT)
        school_3km = amap_around(lng, lat, POI['school'], 3000); time.sleep(RATE_LIMIT)
        primary_3km = amap_around(lng, lat, POI['primary'], 3000); time.sleep(RATE_LIMIT)

        if not check_search_quota():
            break

        row['交通可达性(地铁)'] = score_metro(metro_1km, metro_dist)
        row['医疗水平'] = score_medical(hosp_5km, 0)
        row['5km商业综合指数'] = score_commercial(mall_3km)
        row['买菜便利度'] = score_grocery(grocery_1km // 2, grocery_1km - grocery_1km // 2)
        row['教育资源指数'] = score_education(school_3km, primary_3km)

        src = row.get('数据来源', '')
        row['数据来源'] = f"高德POI+{src}" if src and '高德' not in src else '高德POI'
        updated += 1

    # 保存
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

    save_quota()
    print(f"\n完成: {updated} 小区更新")
    print(f"  搜索配额: {_search_calls}/{QUOTA_SEARCH_MONTHLY}")
    print(f"  LBS配额: {_lbs_calls}/{QUOTA_LBS_MONTHLY}")
    print(f"  输出: {OUTPUT_FILE}")


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
