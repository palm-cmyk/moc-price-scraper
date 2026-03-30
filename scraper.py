import re
import json
import os
import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# ==========================================
# ตั้งค่าโฟลเดอร์
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_FILE = os.path.join(BASE_DIR, 'item_mapping.json')
CURRENT_PRICE_FILE = os.path.join(BASE_DIR, 'market_prices.json')
FIREBASE_KEY = os.path.join(BASE_DIR, 'firebase-key.json')

# เชื่อมต่อ Firebase
try:
    cred = credentials.Certificate(FIREBASE_KEY)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("✅ เชื่อมต่อ Firebase สำเร็จ!\n")
except Exception as e:
    print(f"❌ ไม่สามารถเชื่อมต่อ Firebase ได้ ตรวจสอบไฟล์กุญแจ: {e}")

# ==========================================
# ลิงก์และหมวดหมู่
# ==========================================
MOC_URLS = {
    "เนื้อสัตว์": "https://www.moc.go.th/th/content/category/detail/id/311/iid/3048",
    "สัตว์น้ำ": "https://www.moc.go.th/th/content/category/detail/id/311/iid/3053",
    "ผลไม้": "https://www.moc.go.th/th/content/category/detail/id/311/iid/3055",
    "ผักสด": "https://www.moc.go.th/th/content/category/detail/id/311/iid/3054",
    "พืชอาหาร": "https://www.moc.go.th/th/content/category/detail/id/311/iid/3060",
    "พืชน้ำมันและน้ำมันพืช": "https://www.moc.go.th/th/content/category/detail/id/311/iid/3061",
    "ราคาขายปลีกข้าวสาร": "https://www.moc.go.th/th/content/category/detail/id/311/iid/3062",
    "อาหารสัตว์และวัตถุดิบอาหารสัตว์": "https://www.moc.go.th/th/content/category/detail/id/311/iid/9485",
    "ราคาขายส่งข้าว ผลิตภัณฑ์ข้าวและกระสอบป่าน": "https://www.moc.go.th/th/content/category/detail/id/311/iid/9486",
    "ราคาขายส่งข้าวสารให้ร้านขายปลีก": "https://www.moc.go.th/th/content/category/detail/id/311/iid/9487"
}

CATEGORY_PREFIX = {
    "เนื้อสัตว์": "m",
    "สัตว์น้ำ": "f",
    "ผลไม้": "fr",
    "ผักสด": "v",
    "พืชอาหาร": "p",
    "พืชน้ำมันและน้ำมันพืช": "o",
    "ราคาขายปลีกข้าวสาร": "r",
    "อาหารสัตว์และวัตถุดิบอาหารสัตว์": "a",
    "ราคาขายส่งข้าว ผลิตภัณฑ์ข้าวและกระสอบป่าน": "wr",
    "ราคาขายส่งข้าวสารให้ร้านขายปลีก": "ws"
}

# ==========================================
# Unit Normalization
# ==========================================
BULK_RULES = [
    (r'3\s*กล่อง.*?10\s*กก',   30.0,    'บาท/กก.'),
    (r'100\s*กก',               100.0,   'บาท/กก.'),
    (r'15\s*กก',                15.0,    'บาท/กก.'),
    (r'10\s*กก',                10.0,    'บาท/กก.'),
    (r'ตัน',                    1000.0,  'บาท/กก.'),
]

UNIT_CLEAN = {
    'บาท/ กก.':   'บาท/กก.',
    'บาท /กก.':   'บาท/กก.',
    'บาท/ กก':    'บาท/กก.',
    'บาท/กก':     'บาท/กก.',
    'บาท/ 10 กำ': 'บาท/10กำ',
    'บาท/10 กำ':  'บาท/10กำ',
    'บาท/ กำ':    'บาท/กำ',
    '-':           'บาท/กก.',
    '':            'บาท/กก.',
}

NAME_RENAME = {
    "สุกรชำแหละ ทั่วไป":"หมูชำแหละ ทั่วไป","สุกรชำแหละ ฟาร์ม":"หมูชำแหละ ฟาร์ม",
    "สุกรชำแหละ มันแข็ง":"หมูมันแข็ง","สุกรชำแหละ เนื้อสัน สันนอก":"หมูสันนอก",
    "สุกรชำแหละ เนื้อสัน สันใน":"หมูสันใน","สุกรชำแหละ เนื้อสามชั้น":"หมูสามชั้น",
    "สุกรชำแหละ เนื้อแดง สะโพก":"หมูสะโพก",
    "สุกรชำแหละ เนื้อแดง สะโพก (ตัดแต่ง)":"หมูสะโพก (ตัดแต่ง)",
    "สุกรชำแหละ เนื้อแดง สะโพก (ไม่ได้ตัดแต่ง)":"หมูสะโพก (ไม่ตัดแต่ง)",
    "สุกรชำแหละ เนื้อแดง ไหล่":"หมูไหล่",
    "สุกรชำแหละ เนื้อแดง ไหล่ (ตัดแต่ง)":"หมูไหล่ (ตัดแต่ง)",
    "สุกรชำแหละ เนื้อแดง ไหล่ (ไม่ได้ตัดแต่ง)":"หมูไหล่ (ไม่ตัดแต่ง)",
    "สุกรมีชีวิต ทั่วไป":"หมูมีชีวิต ทั่วไป","สุกรมีชีวิต ฟาร์ม":"หมูมีชีวิต ฟาร์ม",
    "ลูกสุกรซีพี (นน. 16 กก./ตัว)":"ลูกหมูซีพี (นน. 16 กก./ตัว)",
    "เนื้อโค ส่วนน่อง":"เนื้อวัวน่อง","เนื้อโค สะโพก":"เนื้อวัวสะโพก",
    "เนื้อโค สันนอก":"เนื้อวัวสันนอก","เนื้อโค สันใน":"เนื้อวัวสันใน",
    "เนื้อโค สามชั้น":"เนื้อวัวสามชั้น",
    "ไก่สดชำแหละ แข้ง ขา ตีน":"ขาตีนไก่","ไก่สดชำแหละ เครื่องใน":"เครื่องในไก่",
    "ไก่สดชำแหละ โครงกระดูก":"โครงไก่","ไก่สดชำแหละ น่อง สะโพก":"น่องสะโพกไก่",
    "ไก่สดชำแหละ น่อง":"น่องไก่","ไก่สดชำแหละ เนื้อสันใน":"สันในไก่",
    "ไก่สดชำแหละ เนื้ออก (ติดหนัง)":"อกไก่ติดหนัง","ไก่สดชำแหละ เนื้ออก (เนื้อล้วน)":"อกไก่ล้วน",
    "ไก่สดชำแหละ ปีก ทั้งปีก (ปีกเต็ม)":"ปีกไก่","ไก่สดชำแหละ ปีกบน":"ปีกบนไก่",
    "ไก่สดชำแหละ สะโพก":"สะโพกไก่","ไก่สดชำแหละ หนัง":"หนังไก่",
    "ไก่สดทั้งตัว (ไม่รวมเครื่องใน)":"ไก่ทั้งตัว (ไม่รวมเครื่องใน)",
    "ไก่สดทั้งตัว (รวมเครื่องใน)":"ไก่ทั้งตัว (รวมเครื่องใน)",
    "ไก่มีชีวิต (ซี.พี.)":"ไก่มีชีวิต (ซีพี)","ไก่มีชีวิต (ทั่วไป)":"ไก่มีชีวิต ทั่วไป",
    "น้ำมันปาล์มบริสุทธิ์ สเตอรีน (ราคาซื้อขายทั่วไปส่งมอบถึงผู้ซื้อ)":"น้ำมันปาล์มบริสุทธิ์ สเตอรีน",
    "น้ำมันปาล์มบริสุทธิ์ โอลีอีน (ราคาซื้อขายทั่วไปส่งมอบถึงผู้ซื้อ)":"น้ำมันปาล์มบริสุทธิ์ โอลีอีน",
    "น้ำมันปาล์มสำเร็จรูป (ราคา VAT โรงกลั่นส่งมอบถึงยี่ปั๊ว) บรรจุปี๊บ 13.75 ลิตร (12.50 กก.) ตราทับทิม":"น้ำมันปาล์มสำเร็จรูป 13.75 ลิตร (12.50 กก.) ตราทับทิม",
    "น้ำมันปาล์มสำเร็จรูป (ราคารวม VAT โรงกลั่นส่งมอบถึงยี่ปั๊ว) บรรจุปีบ 13.75 ลิตร (12.50 กก.) ตราเกสร":"น้ำมันปาล์มสำเร็จรูป 13.75 ลิตร (12.50 กก.) ตราเกสร",
    "น้ำมันปาล์มสำเร็จรูป (ราคารวม VAT โรงกลั่นส่งมอบถึงยี่ปั๊ว) บรรจุปีบ 13.75 ลิตร (12.50 กก.) ตรามรกต":"น้ำมันปาล์มสำเร็จรูป 13.75 ลิตร (12.50 กก.) ตรามรกต",
    "น้ำมันปาล์มสำเร็จรูป (ราคารวม VAT โรงกลั่นส่งมอบถึงยี่ปั๊ว) บรรจุปีบ 13.75 ลิตร (12.50 กก.) ตราหยก":"น้ำมันปาล์มสำเร็จรูป 13.75 ลิตร (12.50 กก.) ตราหยก",
    "น้ำมันปาล์มสำเร็จรูป (ราคารวม VAT โรงกลั่นส่งมอบถึงยี่ปั๊ว) บรรจุปีบ 13.75 ลิตร (12.50 กก.) ตราโอลีน":"น้ำมันปาล์มสำเร็จรูป 13.75 ลิตร (12.50 กก.) ตราโอลีน",
    "น้ำมันปาล์มสำเร็จรูป (ราคารวม VAT โรงกลั่นส่งมอบถึงยี่ปั๊ว) บรรจุปีบ 13.75 ลิตร (12.50 กก.)":"น้ำมันปาล์มสำเร็จรูป 13.75 ลิตร (12.50 กก.)",
}

def normalize_item(item_id: str, item: dict) -> dict:
    result = dict(item)
    result['name'] = NAME_RENAME.get(result.get('name', ''), result.get('name', ''))
    unit = (item.get('unit') or '').strip()

    for pattern, divisor, new_unit in BULK_RULES:
        if re.search(pattern, unit, re.IGNORECASE):
            for field in ('price', 'min_price', 'max_price',
                          'start_month_price', 'start_year_price'):
                val = result.get(field)
                if val:
                    result[field] = round(val / divisor, 4)
            result['unit'] = new_unit
            return result

    if unit in UNIT_CLEAN:
        result['unit'] = UNIT_CLEAN[unit]
    return result

def normalize_all_items(items: dict) -> dict:
    fixed = {}
    bulk_count = 0
    unit_count = 0

    for item_id, item in items.items():
        original_unit = (item.get('unit') or '').strip()
        result = normalize_item(item_id, item)
        fixed[item_id] = result

        if result.get('price') != item.get('price'):
            bulk_count += 1
        elif result.get('unit') != original_unit:
            unit_count += 1

    print(f"Unit normalization: {bulk_count} bulk price fixes, {unit_count} unit string fixes")
    return fixed

def load_mapping():
    if os.path.exists(MAPPING_FILE):
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_mapping(mapping_data):
    with open(MAPPING_FILE, 'w', encoding='utf-8') as f:
        json.dump(mapping_data, f, ensure_ascii=False, indent=4)

def get_driver():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--window-size=1920,1080') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    return webdriver.Chrome(options=options)

# ==========================================
# Main scraper
# ==========================================

def scrape_moc_daily_prices():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] เริ่มภารกิจกวาดราคา (พร้อมอัปโหลดขึ้น Cloud)...")

    item_mapping = load_mapping()
    all_scraped_items = {}
    official_update_date = None
    driver = get_driver()

    try:
        links = list(MOC_URLS.items())
        for i, (category_name, url) in enumerate(links):

            if i > 0:
                print("รีเซ็ตบราวเซอร์เพื่อป้องกันหน้าเว็บค้าง...")
                driver.quit()
                time.sleep(2)
                driver = get_driver()

            print(f"\n[{i+1}/{len(links)}] กำลังโหลดหมวดหมู่: {category_name}...")

            cat_retail_count = 0
            cat_wholesale_count = 0

            try:
                driver.get(url)
                time.sleep(5)
                driver.refresh()
            except Exception as e:
                print(f"โหลดหน้าเว็บไม่สำเร็จ: {e}")
                continue

            wait_time = 40 if category_name == "พืชน้ำมันและน้ำมันพืช" else 10
            time.sleep(wait_time)

            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            frames_to_scrape = [None] + iframes

            found_in_category = False
            iframe_counter = 0

            for frame in frames_to_scrape:
                try:
                    driver.switch_to.default_content()
                    if frame is not None:
                        time.sleep(2)
                        driver.switch_to.frame(frame)
                        iframe_counter += 1
                        wait = 30 if category_name == "พืชน้ำมันและน้ำมันพืช" else 10
                        time.sleep(wait)

                        driver.execute_script("try { $('table').DataTable().destroy(); } catch(e) {}")
                        time.sleep(2)

                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        page_text = soup.get_text()

                        if not official_update_date:
                            date_match = re.search(r'ข้อมูล ณ วันที่\s*(\d{2}/\d{2}/\d{4})', page_text)
                            if date_match:
                                official_update_date = date_match.group(1)

                        RETAIL_FIRST_CATEGORIES = {'ผลไม้', 'ผักสด', 'เนื้อสัตว์'}
                        if category_name in RETAIL_FIRST_CATEGORIES:
                            table_type = "ราคาปลีก" if iframe_counter == 1 else "ราคาส่ง"
                        elif "ราคาส่ง" in page_text or "ขายส่ง" in page_text:
                            table_type = "ราคาส่ง"
                        elif "ราคาปลีก" in page_text or "ขายปลีก" in page_text:
                            table_type = "ราคาปลีก"
                        else:
                            table_type = "ราคาปลีก" if iframe_counter == 1 else "ราคาส่ง"

                    else:
                        table_type = "หน้าหลัก"
                        time.sleep(2)
                        soup = BeautifulSoup(driver.page_source, 'html.parser')

                    rows = soup.find_all('tr')
                    if len(rows) < 2:
                        continue

                    page_number = 1
                    previous_first_item = None

                    while True:
                        if page_number > 1:
                            time.sleep(4)
                            if frame is not None:
                                driver.execute_script("try { $('table').DataTable().destroy(); } catch(e) {}")
                                time.sleep(2)
                            soup = BeautifulSoup(driver.page_source, 'html.parser')
                            rows = soup.find_all('tr')

                        current_first_item = None
                        has_data = False

                        for row in rows:
                            cols = row.find_all(['td', 'th'])
                            if not cols:
                                continue
                            
                            first_col_text = cols[0].get_text(strip=True)
                            if "ลำดับ" in first_col_text or "รายการ" in first_col_text:
                                continue

                            offset = 0
                            if len(cols) >= 5 and not first_col_text.isdigit():
                                offset = 1
                            elif len(cols) >= 3 and not first_col_text.isdigit() and cols[1].get_text(strip=True).isdigit():
                                offset = 1
                                
                            if len(cols) >= 3 + offset:
                                item_name = cols[1 + offset].get_text(" ", strip=True)
                                range_text = cols[2 + offset].get_text(strip=True)
                                
                                # 🧹 อาบน้ำให้ข้อความ: ล้างขยะ \xa0, \u200b และยุบช่องว่าง
                                item_name = item_name.replace('\xa0', ' ').replace('\u200b', '')
                                item_name = re.sub(r'\s+', ' ', item_name).strip()
                                
                                if not item_name or "รายการ" in item_name:
                                    continue

                                item_name = NAME_RENAME.get(item_name, item_name)

                                if not current_first_item:
                                    current_first_item = item_name

                                range_numbers = re.findall(r'\d+\.?\d*', range_text.replace(',', ''))
                                if not range_numbers:
                                    continue
                                    
                                min_price = float(range_numbers[0])
                                max_price = float(range_numbers[1]) if len(range_numbers) >= 2 else min_price

                                if len(cols) > 3 + offset:
                                    avg_price_text = cols[3 + offset].get_text(strip=True)
                                    avg_match = re.search(r'\d+\.?\d*', avg_price_text.replace(',', ''))
                                    
                                    if avg_match and "-" not in avg_price_text:
                                        avg_price = float(avg_match.group())
                                    else:
                                        avg_price = (min_price + max_price) / 2.0
                                else:
                                    avg_price = (min_price + max_price) / 2.0

                                unit_text = cols[4 + offset].get_text(strip=True) if len(cols) > 4 + offset else "หน่วย"

                                if item_name not in item_mapping:
                                    prefix = CATEGORY_PREFIX.get(category_name, "x")
                                    count_in_cat = sum(1 for v in item_mapping.values() if v.startswith(prefix))
                                    item_mapping[item_name] = f"{prefix}{count_in_cat + 1}"

                                item_id_base = item_mapping[item_name]
                                item_id = f"{item_id_base}_r" if table_type == "ราคาปลีก" else item_id_base
                                
                                all_scraped_items[item_id] = {
                                    "name": item_name,
                                    "price": avg_price,
                                    "min_price": min_price,
                                    "max_price": max_price,
                                    "unit": unit_text,
                                    "category": category_name,
                                    "type": table_type
                                }
                                
                                if table_type == "ราคาปลีก":
                                    cat_retail_count += 1
                                else:
                                    cat_wholesale_count += 1
                                    
                                found_in_category = True
                                has_data = True

                        if current_first_item == previous_first_item or not has_data:
                            break

                        previous_first_item = current_first_item

                        try:
                            next_btns = driver.find_elements(
                                By.XPATH,
                                "//a[contains(text(), 'Next') or contains(text(), 'ถัดไป') or contains(@aria-label, 'Next')]"
                            )
                            valid_btn_found = False
                            for btn in next_btns:
                                try:
                                    parent_class = btn.find_element(By.XPATH, "..").get_attribute("class") or ""
                                    btn_class = btn.get_attribute("class") or ""
                                    if (
                                        "disabled" not in parent_class.lower()
                                        and "disabled" not in btn_class.lower()
                                        and btn.is_displayed()
                                    ):
                                        driver.execute_script("arguments[0].click();", btn)
                                        page_number += 1
                                        time.sleep(3)
                                        valid_btn_found = True
                                        break
                                except Exception:
                                    continue
                            if not valid_btn_found:
                                break
                        except Exception:
                            break

                except Exception:
                    continue

            if found_in_category:
                print(f"ดึง '{category_name}' สำเร็จ ✅ (ปลีก: {cat_retail_count} รายการ, ส่ง: {cat_wholesale_count} รายการ)")
            else:
                print(f"ดึง '{category_name}' ไม่สำเร็จ — retry ครั้งที่ 2...")
                try:
                    driver.quit()
                    time.sleep(3)
                    driver = get_driver()
                    driver.get(url)
                    time.sleep(5)
                    driver.refresh()
                    time.sleep(30)

                    iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    frames_to_scrape = [None] + iframes
                    found_in_category = False
                    iframe_counter = 0
                    cat_retail_count = 0
                    cat_wholesale_count = 0

                    for frame in frames_to_scrape:
                        try:
                            driver.switch_to.default_content()
                            if frame is not None:
                                time.sleep(2)
                                driver.switch_to.frame(frame)
                                iframe_counter += 1
                                time.sleep(10)
                                
                                driver.execute_script("try { $('table').DataTable().destroy(); } catch(e) {}")
                                time.sleep(2)
                                
                                soup = BeautifulSoup(driver.page_source, 'html.parser')
                                page_text = soup.get_text()
                                RETAIL_FIRST_CATEGORIES = {'ผลไม้', 'ผักสด', 'เนื้อสัตว์'}
                                if category_name in RETAIL_FIRST_CATEGORIES:
                                    table_type = "ราคาปลีก" if iframe_counter == 1 else "ราคาส่ง"
                                elif "ราคาส่ง" in page_text or "ขายส่ง" in page_text:
                                    table_type = "ราคาส่ง"
                                elif "ราคาปลีก" in page_text or "ขายปลีก" in page_text:
                                    table_type = "ราคาปลีก"
                                else:
                                    table_type = "ราคาปลีก" if iframe_counter == 1 else "ราคาส่ง"
                            else:
                                table_type = "หน้าหลัก"
                                time.sleep(2)
                                soup = BeautifulSoup(driver.page_source, 'html.parser')

                            rows = soup.find_all('tr')
                            if len(rows) < 2:
                                continue

                            for row in rows:
                                cols = row.find_all(['td', 'th'])
                                if not cols:
                                    continue
                                
                                first_col_text = cols[0].get_text(strip=True)
                                if "ลำดับ" in first_col_text or "รายการ" in first_col_text:
                                    continue

                                offset = 0
                                if len(cols) >= 5 and not first_col_text.isdigit():
                                    offset = 1
                                elif len(cols) >= 3 and not first_col_text.isdigit() and cols[1].get_text(strip=True).isdigit():
                                    offset = 1
                                
                                if len(cols) >= 3 + offset:
                                    item_name = cols[1 + offset].get_text(" ", strip=True)
                                    range_text = cols[2 + offset].get_text(strip=True)
                                    
                                    # 🧹 อาบน้ำให้ข้อความ (รอบ Retry)
                                    item_name = item_name.replace('\xa0', ' ').replace('\u200b', '')
                                    item_name = re.sub(r'\s+', ' ', item_name).strip()
                                    
                                    if not item_name or "รายการ" in item_name:
                                        continue

                                    item_name = NAME_RENAME.get(item_name, item_name)

                                    if not current_first_item:
                                        current_first_item = item_name

                                    range_numbers = re.findall(r'\d+\.?\d*', range_text.replace(',', ''))
                                    if not range_numbers:
                                        continue
                                        
                                    min_price = float(range_numbers[0])
                                    max_price = float(range_numbers[1]) if len(range_numbers) >= 2 else min_price

                                    if len(cols) > 3 + offset:
                                        avg_price_text = cols[3 + offset].get_text(strip=True)
                                        avg_match = re.search(r'\d+\.?\d*', avg_price_text.replace(',', ''))
                                        
                                        if avg_match and "-" not in avg_price_text:
                                            avg_price = float(avg_match.group())
                                        else:
                                            avg_price = (min_price + max_price) / 2.0
                                    else:
                                        avg_price = (min_price + max_price) / 2.0

                                    unit_text = cols[4 + offset].get_text(strip=True) if len(cols) > 4 + offset else "หน่วย"

                                    if item_name not in item_mapping:
                                        prefix = CATEGORY_PREFIX.get(category_name, "x")
                                        count_in_cat = sum(1 for v in item_mapping.values() if v.startswith(prefix))
                                        item_mapping[item_name] = f"{prefix}{count_in_cat + 1}"

                                    item_id_base = item_mapping[item_name]
                                    item_id = f"{item_id_base}_r" if table_type == "ราคาปลีก" else item_id_base
                                    
                                    all_scraped_items[item_id] = {
                                        "name": item_name,
                                        "price": avg_price,
                                        "min_price": min_price,
                                        "max_price": max_price,
                                        "unit": unit_text,
                                        "category": category_name,
                                        "type": table_type
                                    }
                                    
                                    if table_type == "ราคาปลีก":
                                        cat_retail_count += 1
                                    else:
                                        cat_wholesale_count += 1
                                        
                                    found_in_category = True

                        except Exception:
                            continue

                    if found_in_category:
                        print(f"retry '{category_name}' สำเร็จ ✅ (ปลีก: {cat_retail_count} รายการ, ส่ง: {cat_wholesale_count} รายการ)")
                    else:
                        print(f"retry '{category_name}' ยังไม่สำเร็จ ❌ — ข้ามไป")
                except Exception as e:
                    print(f"retry error: {e}")

        # ==========================================
        # ประมวลผลประวัติราคา (อัปเดตระบบทดแทนถ้าขาดหาย)
        # ==========================================
        if all_scraped_items:
            save_mapping(item_mapping)

            print("กำลัง normalize units...")
            all_scraped_items = normalize_all_items(all_scraped_items)

            final_date_str = (
                f"ข้อมูล ณ วันที่ {official_update_date}"
                if official_update_date
                else "อ้างอิงตามประกาศล่าสุดของกระทรวง"
            )
            timestamp_str = f"{final_date_str} (ซิงค์ขึ้น Cloud เวลา {datetime.now().strftime('%H:%M')})"

            print("กำลังประมวลผลราคาประวัติศาสตร์ (ต้นเดือน/ต้นปี)...")
            today = datetime.now()
            year_doc_id = f"year_{today.year}"
            month_doc_id = f"month_{today.year}_{today.month:02d}"
            history_ref = db.collection('market_data_history')

            def get_or_update_history(doc_id, current_items, period_name):
                doc_ref = history_ref.document(doc_id)
                doc = doc_ref.get()
                
                if not doc.exists:
                    doc_ref.set({"items": current_items})
                    print(f"สร้างฐานข้อมูลราคา{period_name}ใหม่เรียบร้อย! ({doc_id})")
                    return current_items
                else:
                    raw_history = doc.to_dict().get("items", {})
                    history_data = {k: normalize_item(k, v) for k, v in raw_history.items()}
                    
                    needs_update = False
                    for item_id, item_info in current_items.items():
                        if item_id not in raw_history:
                            raw_history[item_id] = item_info
                            history_data[item_id] = item_info
                            needs_update = True
                            
                    if needs_update:
                        doc_ref.update({"items": raw_history})
                        print(f"อัปเดตเพิ่มรายการใหม่ลงในฐานข้อมูล{period_name} ({doc_id})")
                        
                    return history_data

            year_data = get_or_update_history(year_doc_id, all_scraped_items, "ต้นปี")
            month_data = get_or_update_history(month_doc_id, all_scraped_items, "ต้นเดือน")

            for item_id, item_info in all_scraped_items.items():
                s_year_price = year_data.get(item_id, {}).get("price", item_info["price"])
                s_month_price = month_data.get(item_id, {}).get("price", item_info["price"])
                item_info["start_year_price"] = s_year_price
                item_info["start_month_price"] = s_month_price

            # ==========================================
            # Item Count Guard - ป้องกัน silent failure
            # ==========================================
            prev_doc = db.collection('market_data').document('latest').get()
            if prev_doc.exists:
                prev_count = len(prev_doc.to_dict().get('items', {}))
                new_count = len(all_scraped_items)
                drop_pct = (prev_count - new_count) / max(prev_count, 1)
                if drop_pct > 0.20:
                    raise Exception(
                        f"จำนวนสินค้าหายไป {int(drop_pct * 100)}%: "
                        f"{prev_count} -> {new_count} รายการ "
                        f"(อาจเป็นเพราะ MOC เปลี่ยน Schema) - ยกเลิกการอัปโหลด"
                    )

            # Category guard
            REQUIRED_PREFIXES = {'m', 'f', 'fr', 'v', 'p', 'o', 'a', 'ws'}
            scraped_prefixes = set()
            for item_id in all_scraped_items:
                base = item_id.rstrip('_r').rstrip('0123456789')
                scraped_prefixes.add(base)
            missing_cats = REQUIRED_PREFIXES - scraped_prefixes
            if missing_cats:
                if len(missing_cats) > 3:
                    raise Exception(
                        f"หมวดหมู่หายไปทั้งหมด: {missing_cats} — ยกเลิกการอัปโหลด"
                    )
                print(f"⚠️  หมวดที่ดึงไม่ได้: {missing_cats} — อัปโหลดหมวดที่เหลือต่อไป")

            # ==========================================
            # เตรียม payload และอัปโหลด
            # ==========================================
            now_utc = datetime.now(timezone.utc)

            market_data = {
                "updated_at": timestamp_str,
                "scraped_at": now_utc,
                "official_update_date": official_update_date or "",
                "item_count": len(all_scraped_items),
                "scrape_version": "1.3.0",
                "items": all_scraped_items
            }

            with open(CURRENT_PRICE_FILE, 'w', encoding='utf-8') as f:
                json.dump({**market_data, "scraped_at": now_utc.isoformat()}, f, ensure_ascii=False, indent=4)

            print("กำลังอัปโหลดข้อมูลล่าสุด (พร้อมประวัติ) ขึ้น Firebase...")
            try:
                doc_ref = db.collection('market_data').document('latest')
                doc_ref.set(market_data)
                print(f"อัปโหลดขึ้น Firebase สำเร็จ! {len(all_scraped_items)} รายการ | scraped_at: {now_utc.strftime('%H:%M UTC')}")
            except Exception as firebase_e:
                print(f"อัปโหลด Firebase ล้มเหลว: {firebase_e}")
                raise

        else:
            print("ไม่พบข้อมูลใดๆ เลยในวันนี้")

    except Exception as e:
        print(f"พลาดท่า: {e}")
        raise
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_moc_daily_prices()
