import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# ==========================================
# ตั้งค่าโฟลเดอร์และการเชื่อมต่อ Firebase
# ==========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FIREBASE_KEY = os.path.join(BASE_DIR, 'firebase-key.json')

try:
    cred = credentials.Certificate(FIREBASE_KEY)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("✅ เชื่อมต่อ Firebase สำเร็จ!\n")
except Exception as e:
    print(f"❌ ไม่สามารถเชื่อมต่อ Firebase ได้: {e}")
    exit()

def check_market_history():
    print("==================================================")
    print("📊 สรุปข้อมูลในคอลเลกชัน 'market_data_history'")
    print("==================================================")
    
    # ดึงเอกสารทั้งหมดใน market_data_history
    history_ref = db.collection('market_data_history')
    docs = list(history_ref.stream())
    
    if not docs:
        print("⚠️ ยังไม่มีข้อมูลประวัติราคา (เดือน/ปี) ในระบบเลยครับ")
        return

    # เก็บข้อมูลไว้ใช้ค้นหาต่อ
    all_history_data = {}

    for doc in docs:
        data = doc.to_dict()
        items = data.get('items', {})
        all_history_data[doc.id] = items
        
        print(f"📁 เอกสาร (Document): {doc.id}")
        print(f"   📌 จำนวนสินค้าที่ถูกบันทึกฐานราคา: {len(items)} รายการ")
        
        # สุ่มตัวอย่างสินค้ามาให้ดู 3 รายการแรก
        sample_keys = list(items.keys())[:3]
        if sample_keys:
            print("   🔍 ตัวอย่างรายการที่บันทึกไว้:")
            for k in sample_keys:
                item_info = items[k]
                name = item_info.get('name', 'ไม่มีชื่อ')
                price = item_info.get('price', 0)
                unit = item_info.get('unit', '')
                print(f"      - {name} | {price:.2f} บาท/{unit}")
        print("-" * 50)

    # ==========================================
    # ระบบค้นหาและเปรียบเทียบประวัติของสินค้า
    # ==========================================
    while True:
        print("\n🔎 พิมพ์ชื่อสินค้าเพื่อดูประวัติราคา (เช่น 'ปาล์ม', 'หมู')")
        keyword = input("👉 คำค้นหา (หรือกด Enter ปล่อยว่างเพื่อออก): ").strip()
        
        if not keyword:
            print("👋 ออกจากระบบตรวจสอบ")
            break
            
        found_any = False
        print(f"\nผลการค้นหาสำหรับ: '{keyword}'")
        print("=" * 40)
        
        # ค้นหาคำในทุกเอกสาร
        for doc_id, items in all_history_data.items():
            matched_items = []
            for item_id, info in items.items():
                if keyword.lower() in info.get('name', '').lower():
                    matched_items.append(info)
            
            if matched_items:
                found_any = True
                print(f"📅 ในฐานข้อมูล: {doc_id}")
                for match in matched_items:
                    name = match.get('name', '')
                    price = match.get('price', 0)
                    min_p = match.get('min_price', 0)
                    max_p = match.get('max_price', 0)
                    unit = match.get('unit', '')
                    print(f"   🔹 {name}")
                    print(f"      ราคาเฉลี่ย: {price:.2f} ฿/{unit} (ช่วงราคา: {min_p:.2f} - {max_p:.2f})")
                print("-" * 30)
                
        if not found_any:
            print(f"❌ ไม่พบสินค้าที่มีคำว่า '{keyword}' ในประวัติเลยครับ")

if __name__ == "__main__":
    check_market_history()
