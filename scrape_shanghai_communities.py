#!/usr/bin/env python3
"""
上海全量小区数据采集脚本
========================
用于从贝壳/链家/房天下等平台扩展种子数据到全量 ~26,000 个小区。

使用方法:
  pip install requests beautifulsoup4 lxml pandas
  python scrape_shanghai_communities.py

数据来源:
  1. 贝壳找房 (ke.com) - 小区列表、均价、经纬度
  2. 房天下 (fang.com) - 新房交付日历、历史价格
  3. 安居客 (anjuke.com) - 租金数据
  4. 高德地图 API - POI 数据（医院/学校/商场/地铁站距离）

注意:
  - 请自备代理 IP，各平台有反爬策略
  - 建议按区分批采集，每次间隔 2-5 秒
  - 贝壳小区页面每页约 30 个，每区约 100-300 页
"""

import requests
import time
import json
import csv
import random
import os
import re
from urllib.parse import quote

# ============================================================
# 配置
# ============================================================
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept-Language': 'zh-CN,zh;q=0.9',
}

BEIKE_DISTRICT_CODES = {
    "黄浦区": "huangpu",
    "徐汇区": "xuhui",
    "长宁区": "changning",
    "静安区": "jingan",
    "普陀区": "putuo",
    "杨浦区": "yangpu",
    "虹口区": "hongkou",
    "浦东新区": "pudong",
    "闵行区": "minhang",
    "宝山区": "baoshan",
    "嘉定区": "jiading",
    "松江区": "songjiang",
    "青浦区": "qingpu",
    "奉贤区": "fengxian",
    "金山区": "jinshan",
    "崇明区": "chongming",
}

# 高德地图 API key (需自行申请: https://lbs.amap.com/)
AMAP_KEY = os.environ.get("AMAP_KEY", "YOUR_AMAP_KEY_HERE")

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# 1. 贝壳小区列表采集
# ============================================================
def fetch_beike_communities(district_code, page=1):
    url = f"https://sh.ke.com/xiaoqu/{district_code}/pg{page}/"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'lxml')
        communities = []
        items = soup.select('.listContent .xiaoquListItem')
        for item in items:
            try:
                name = item.select_one('.title a').text.strip()
                link = item.select_one('.title a')['href']
                price_el = item.select_one('.totalPrice span')
                price = int(price_el.text) if price_el else 0
                area_el = item.select_one('.positionInfo a:nth-of-type(2)')
                area = area_el.text.strip() if area_el else ""
                communities.append({'name': name, 'url': link, 'price': price, 'area': area})
            except:
                continue
        return communities
    except Exception as e:
        print(f"  [ERROR] {e}")
        return []


def fetch_beike_community_detail(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'lxml')
        info = {}
        for item in soup.select('.xiaoquInfoItem'):
            label = item.select_one('.xiaoquInfoLabel')
            value = item.select_one('.xiaoquInfoContent')
            if label and value:
                info[label.text.strip()] = value.text.strip()
        script_text = str(soup)
        lng_match = re.search(r"longitude['\"]?\s*[:=]\s*['\"]?([\d.]+)", script_text)
        lat_match = re.search(r"latitude['\"]?\s*[:=]\s*['\"]?([\d.]+)", script_text)
        if lng_match:
            info['经度'] = float(lng_match.group(1))
        if lat_match:
            info['纬度'] = float(lat_match.group(1))
        return info
    except Exception as e:
        print(f"  [ERROR] detail: {e}")
        return {}


# ============================================================
# 2. 房天下新房交付日历
# ============================================================
def fetch_fang_new_deliveries(year=2026):
    url = f"https://sh.newhouse.fang.com/house/livindate/{year}.htm"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, 'lxml')
        deliveries = []
        items = soup.select('.nhouse_list li')
        for item in items:
            try:
                name = item.select_one('.nlcd_name a').text.strip()
                district = item.select_one('.tag').text.strip() if item.select_one('.tag') else ""
                price_el = item.select_one('.nhouse_price span')
                price = price_el.text.strip() if price_el else ""
                deliveries.append({'name': name, 'district': district, 'price': price, 'delivery_year': year})
            except:
                continue
        return deliveries
    except Exception as e:
        print(f"  [ERROR] fang: {e}")
        return []


# ============================================================
# 3. 高德 POI 查询 (周边配套)
# ============================================================
def fetch_nearby_poi(lat, lng, poi_type, radius=3000):
    if AMAP_KEY == "YOUR_AMAP_KEY_HERE":
        return -1
    url = (f"https://restapi.amap.com/v3/place/around?"
           f"key={AMAP_KEY}&location={lng},{lat}&radius={radius}"
           f"&types={poi_type}&offset=1&page=1")
    try:
        resp = requests.get(url, timeout=5)
        data = resp.json()
        return int(data.get('count', 0))
    except:
        return -1


def calculate_poi_scores(lat, lng):
    scores = {}
    metro_count = fetch_nearby_poi(lat, lng, "150500", 1000)
    if metro_count >= 0:
        scores['交通可达性_地铁'] = min(10, metro_count * 2.5)
    hospital_count = fetch_nearby_poi(lat, lng, "090100", 5000)
    if hospital_count >= 0:
        scores['医疗水平'] = min(10, hospital_count * 1.5)
    mall_count = fetch_nearby_poi(lat, lng, "060100", 3000)
    if mall_count >= 0:
        scores['商业综合指数'] = min(10, mall_count * 0.8)
    market_count = fetch_nearby_poi(lat, lng, "080600", 1000)
    if market_count >= 0:
        scores['买菜便利度'] = min(10, market_count * 1.2)
    school_count = fetch_nearby_poi(lat, lng, "141200", 3000)
    if school_count >= 0:
        scores['教育资源'] = min(10, school_count * 0.5)
    return scores


# ============================================================
# 主程序
# ============================================================
def main():
    all_communities = []
    print("=" * 60)
    print("上海全量小区数据采集")
    print("=" * 60)

    for district_name, district_code in BEIKE_DISTRICT_CODES.items():
        print(f"\n>>> {district_name} ({district_code})")
        page = 1
        district_communities = []
        while True:
            print(f"  Page {page}...", end=" ")
            communities = fetch_beike_communities(district_code, page)
            if not communities:
                print("done.")
                break
            print(f"{len(communities)} communities")
            district_communities.extend(communities)
            page += 1
            time.sleep(random.uniform(2, 5))
            if page > 500:
                break

        for i, comm in enumerate(district_communities):
            if i % 50 == 0:
                print(f"  Fetching details: {i}/{len(district_communities)}")
            detail = fetch_beike_community_detail(comm['url'])
            comm.update(detail)
            comm['district'] = district_name
            time.sleep(random.uniform(1, 3))

        all_communities.extend(district_communities)
        print(f"  OK {district_name}: {len(district_communities)} communities")

    output_file = os.path.join(OUTPUT_DIR, "shanghai_all_communities_raw.csv")
    if all_communities:
        keys = sorted(set().union(*(c.keys() for c in all_communities)))
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_communities)
        print(f"\nSaved {len(all_communities)} communities to {output_file}")

    print("\n>>> Fetching 2024-2026 new deliveries...")
    new_deliveries = []
    for year in [2024, 2025, 2026]:
        deliveries = fetch_fang_new_deliveries(year)
        new_deliveries.extend(deliveries)
        print(f"  {year}: {len(deliveries)} projects")
        time.sleep(2)

    new_file = os.path.join(OUTPUT_DIR, "shanghai_new_deliveries_2024_2026.csv")
    if new_deliveries:
        keys = sorted(set().union(*(d.keys() for d in new_deliveries)))
        with open(new_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(new_deliveries)
        print(f"  Saved {len(new_deliveries)} new deliveries to {new_file}")

    print(f"\nTotal: {len(all_communities)} communities + {len(new_deliveries)} new builds")
    print("Next: configure AMAP_KEY and run enrich_scores.py")


if __name__ == "__main__":
    main()
