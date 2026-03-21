import re
import json
import os
import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# 🟢 นำเข้า Firebase Admin
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# ==========================================
# 📂 ตั้งค่าโฟลเดอร์
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_FILE = os.path.join(BASE_DIR, 'item_mapping.json') 
CURRENT_PRICE_FILE = os.path.join(BASE_DIR, 'market_prices.json')
FIREBASE_KEY = os.path.join(BASE_DIR, 'firebase-key.json')

# 🟢 เชื่อมต่อ Firebase
try:
    cred = credentials.Certificate(FIREBASE_KEY)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("☁️ เชื่อมต่อ Firebase สำเร็จแล้ว!")
except Exception as e:
    print(f"❌ ไม่สามารถเชื่อมต่อ Firebase ได้ ตรวจสอบไฟล์กุญแจ: {e}")

# ==========================================
# 🔗 ลิงก์และหมวดหมู่
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
    "เนื้อสัตว์": "m", "สัตว์น้ำ": "f", "ผลไม้": "fr", "ผักสด": "v",
    "พืชอาหาร": "p", "พืชน้ำมันและน้ำมันพืช": "o", "ราคาขายปลีกข้าวสาร": "r",
    "อาหารสัตว์และวัตถุดิบอาหารสัตว์": "a", "ราคาขายส่งข้าว ผลิตภัณฑ์ข้าวและกระสอบป่าน": "wr",
    "ราคาขายส่งข้าวสารให้ร้านขายปลีก": "ws" 
}

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

def scrape_moc_daily_prices():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🤖 เริ่มภารกิจกวาดราคา (พร้อมอัปโหลดขึ้น Cloud)...")

    item_mapping = load_mapping()
    all_scraped_items = {} 
    
    official_update_date = None
    driver = get_driver() 

    try:
        links = list(MOC_URLS.items())
        for i, (category_name, url) in enumerate(links):
            
            if i > 0:
                print("🔄 รีเซ็ตบราวเซอร์เพื่อป้องกันหน้าเว็บค้าง...")
                driver.quit()
                time.sleep(2)
                driver = get_driver()

            print(f"\n🚀 [{i+1}/{len(links)}] กำลังโหลดหมวดหมู่: {category_name}...")
            
            try:
                driver.get(url)
                time.sleep(5)
                driver.refresh()
            except Exception as e:
                print(f"❌ โหลดหน้าเว็บไม่สำเร็จ: {e}")
                continue
                
            time.sleep(15) 
            
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
                        time.sleep(10) 
                        
                        soup = BeautifulSoup(driver.page_source, 'html.parser')
                        page_text = soup.get_text()
                        
                        if not official_update_date:
                            date_match = re.search(r'ข้อมูล ณ วันที่\s*(\d{2}/\d{2}/\d{4})', page_text)
                            if date_match:
                                official_update_date = date_match.group(1)

                        if "ราคาส่ง" in page_text or "ขายส่ง" in page_text:
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
                    if len(rows) < 2: continue

                    page_number = 1
                    previous_first_item = None 
                    
                    while True:
                        if page_number > 1:
                            time.sleep(4) 
                            soup = BeautifulSoup(driver.page_source, 'html.parser')
                            rows = soup.find_all('tr')
                        
                        current_first_item = None
                        has_data = False
                        
                        for row in rows:
                            cols = row.find_all(['td', 'th'])
                            if len(cols) >= 4:
                                item_name = cols[1].get_text(" ", strip=True)
                                
                                range_text = cols[2].get_text(strip=True)
                                avg_price_text = cols[3].get_text(strip=True) if len(cols) > 3 else range_text
                                unit_text = cols[4].get_text(strip=True) if len(cols) > 4 else "หน่วย"
                                
                                avg_match = re.search(r'\d+\.?\d*', avg_price_text.replace(',', ''))
                                
                                if item_name and avg_match and "รายการ" not in item_name:
                                    if not current_first_item:
                                        current_first_item = item_name 
                                        
                                    avg_price = float(avg_match.group())
                                    
                                    min_price = avg_price
                                    max_price = avg_price
                                    range_numbers = re.findall(r'\d+\.?\d*', range_text.replace(',', ''))
                                    
                                    if len(range_numbers) >= 2:
                                        min_price = float(range_numbers[0])
                                        max_price = float(range_numbers[1])
                                    elif len(range_numbers) == 1:
                                        min_price = float(range_numbers[0])
                                        max_price = float(range_numbers[0])
                                        
                                    if item_name not in item_mapping:
                                        prefix = CATEGORY_PREFIX.get(category_name, "x") 
                                        count_in_cat = sum(1 for v in item_mapping.values() if v.startswith(prefix))
                                        item_mapping[item_name] = f"{prefix}{count_in_cat + 1}"
                                    
                                    item_id = item_mapping[item_name]
                                    all_scraped_items[item_id] = {
                                        "name": item_name,
                                        "price": avg_price,
                                        "min_price": min_price,
                                        "max_price": max_price,
                                        "unit": unit_text,
                                        "category": category_name,
                                        "type": table_type
                                    }
                                    found_in_category = True
                                    has_data = True

                        if current_first_item == previous_first_item or not has_data:
                            break
                        
                        previous_first_item = current_first_item 

                        try:
                            next_btns = driver.find_elements(By.XPATH, "//a[contains(text(), 'Next') or contains(text(), 'ถัดไป') or contains(@aria-label, 'Next')]")
                            valid_btn_found = False
                            for btn in next_btns:
                                try:
                                    parent_class = btn.find_element(By.XPATH, "..").get_attribute("class") or ""
                                    btn_class = btn.get_attribute("class") or ""
                                    if "disabled" not in parent_class.lower() and "disabled" not in btn_class.lower() and btn.is_displayed():
                                        driver.execute_script("arguments[0].click();", btn)
                                        page_number += 1
                                        time.sleep(3)
                                        valid_btn_found = True
                                        break
                                except: continue
                            if not valid_btn_found: break 
                        except: break
                            
                except Exception as e: continue
                    
            if found_in_category:
                print(f"✅ ดึง '{category_name}' สำเร็จ")
            else:
                print(f"❌ ดึง '{category_name}' ไม่สำเร็จ")

        # ==========================================
        # 💾 ประมวลผลประวัติราคา และ อัปโหลดขึ้น Firebase
        # ==========================================
        if all_scraped_items:
            save_mapping(item_mapping) 
            final_date_str = f"ข้อมูล ณ วันที่ {official_update_date}" if official_update_date else "อ้างอิงตามประกาศล่าสุดของกระทรวง"
            timestamp_str = f"{final_date_str} (ซิงค์ขึ้น Cloud เวลา {datetime.now().strftime('%H:%M')})"
            
            # 🟢 เริ่มระบบจัดการราคาต้นเดือน/ต้นปี ผ่าน Firebase
            print("📊 กำลังประมวลผลราคาประวัติศาสตร์ (ต้นเดือน/ต้นปี)...")
            today = datetime.now()
            year_doc_id = f"year_{today.year}"
            month_doc_id = f"month_{today.year}_{today.month:02d}"
            history_ref = db.collection('market_data_history')

            # 1. เช็คว่ามีราคาต้นปีบันทึกไว้หรือยัง? ถ้ายังให้สร้างใหม่
            year_doc = history_ref.document(year_doc_id).get()
            if not year_doc.exists:
                history_ref.document(year_doc_id).set({"items": all_scraped_items})
                year_data = all_scraped_items
                print("🆕 สร้างฐานข้อมูลราคาต้นปีใหม่เรียบร้อย!")
            else:
                year_data = year_doc.to_dict().get("items", {})

            # 2. เช็คว่ามีราคาต้นเดือนบันทึกไว้หรือยัง? ถ้ายังให้สร้างใหม่
            month_doc = history_ref.document(month_doc_id).get()
            if not month_doc.exists:
                history_ref.document(month_doc_id).set({"items": all_scraped_items})
                month_data = all_scraped_items
                print("🆕 สร้างฐานข้อมูลราคาต้นเดือนใหม่เรียบร้อย!")
            else:
                month_data = month_doc.to_dict().get("items", {})

            # 3. เอาข้อมูลประวัติศาสตร์ มาเสียบรวมกับราคาของวันนี้
            for item_id, item_info in all_scraped_items.items():
                s_year_price = year_data.get(item_id, {}).get("price", item_info["price"])
                s_month_price = month_data.get(item_id, {}).get("price", item_info["price"])
                
                item_info["start_year_price"] = s_year_price
                item_info["start_month_price"] = s_month_price
            
            # เตรียมแพ็คเกจข้อมูลเตรียมส่ง
            # ── 🛡️ Item Count Guard — ป้องกัน silent failure ──────────
            try:
                prev_doc = db.collection('market_data').document('latest').get()
                if prev_doc.exists:
                    prev_count = len(prev_doc.to_dict().get('items', {}))
                    new_count  = len(all_scraped_items)
                    drop_pct   = (prev_count - new_count) / max(prev_count, 1)
                    if drop_pct > 0.20:
                        raise Exception(
                            f"🚨 จำนวนสินค้าหายไป {int(drop_pct*100)}%: "
                            f"{prev_count} → {new_count} รายการ "
                            f"(อาจเป็นเพราะ MOC เปลี่ยน Schema) — ยกเลิกการอัปโหลด"
                        )
            except Exception as guard_err:
                print(str(guard_err))
                raise  # ทำให้ GitHub Actions mark job เป็น FAILED → ส่ง LINE alert

            # ── เตรียมแพ็คเกจข้อมูลเตรียมส่ง ─────────────────────────
            # ✅ ใหม่ (ลบ → ออกทั้งหมด)
            now_utc = datetime.now(timezone.utc)

            market_data = {
                "updated_at":           timestamp_str,
                "scraped_at":           now_utc,
                "official_update_date": official_update_date or "",
                "item_count":           len(all_scraped_items),
                "scrape_version":       "1.1.0",
                "items":                all_scraped_items
            }


            # บันทึกลงเครื่อง (เผื่อดูย้อนหลัง)
            # หมายเหตุ: json.dump บันทึก scraped_at เป็น string ได้ปกติ
            market_data_for_json = {**market_data, "scraped_at": now_utc.isoformat()}
            with open(CURRENT_PRICE_FILE, 'w', encoding='utf-8') as f:
                json.dump(market_data_for_json, f, ensure_ascii=False, indent=4)

            # อัปโหลดขึ้น Firebase
            print("🚀 กำลังอัปโหลดข้อมูลล่าสุด (พร้อมประวัติ) ขึ้น Firebase...")
            try:
                doc_ref = db.collection('market_data').document('latest')
                doc_ref.set(market_data)  # Firestore เก็บ datetime object เป็น Timestamp อัตโนมัติ
                print(f"✅ อัปโหลดขึ้น Firebase สำเร็จ! {len(all_scraped_items)} รายการ "
                      f"| scraped_at: {now_utc.strftime('%H:%M UTC')}")
            except Exception as firebase_e:
                print(f"❌ อัปโหลด Firebase ล้มเหลว: {firebase_e}")
                raise  # 🆕 raise ให้ GitHub Actions รู้ว่า job พัง
```

---

## สรุปสิ่งที่เปลี่ยน

| จุด | เดิม | ใหม่ |
|---|---|---|
| `scraped_at` | ไม่มี | `datetime` object → Firestore Timestamp ✅ |
| `updated_at` | string เดิม | **คงไว้** — ไม่แตะ backward compatibility |
| Firebase write fail | `print` แล้วเงียบ | `raise` → GitHub Actions FAILED → LINE alert ✅ |
| Item count drop | ไม่ตรวจ | ตรวจ drop >20% → หยุด + raise ✅ |
| JSON local file | `datetime` object crash | แยก `market_data_for_json` ก่อน dump ✅ |

---

## ลำดับการ Deploy
```
วันนี้   →  แก้ scraper.py 2 จุดตามด้านบน
วันนี้   →  เพิ่ม LINE_NOTIFY_TOKEN ใน GitHub Secrets
วันนี้   →  กด workflow_dispatch รันทดสอบ
ตรวจสอบ →  Firestore: market_data/latest ต้องมีฟิลด์ scraped_at เป็น Timestamp
ตรวจสอบ →  LINE ได้รับข้อความ ✅ สำเร็จ หรือ 🚨 ล้มเหลว
ถัดไป    →  ทำ Frontend freshness indicator (รอให้ scraper รันจริงอย่างน้อย 1 ครั้งก่อน)
                
        else:
            print("\n⚠️ ไม่พบข้อมูลใดๆ เลยในวันนี้")

    except Exception as e:
        print(f"❌ พลาดท่า: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_moc_daily_prices()
