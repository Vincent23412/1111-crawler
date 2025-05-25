#import sys
#import io

# Replace sys.stdout and sys.stderr with UTF-8 wrappers.
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import requests
import pandas as pd
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from datetime import datetime
import os 

import re

def sanitize_filename(s):
    # Replace any invalid character with an underscore
    return re.sub(r'[\\/*?:"<>|]', '_', s)

# Create a safe directory for storing CSV files (change to your path)
# 此程式為為抓取新竹縣市104職缺, 此縣有1超額職類 (over 150 pages), 會用另外程式抓取, 之後需合併
# 每月抓取時記得更改以下路徑到指定年月的檔案夾 ... /03_2025/, 第27行, 第914行 
# 取得目前這個 Python 檔案的所在目錄
current_dir = os.path.dirname(os.path.abspath(__file__))

base_dir = os.path.join(current_dir, "Hsinchu")

os.makedirs(base_dir, exist_ok=True)

# Optionally, create a logs subdirectory within base_dir:
log_dir = os.path.join(base_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

# Construct the log filename with a full path:
log_filename = os.path.join(log_dir, f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logging.info("Logging is configured. Log file saved to: " + log_filename)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding="utf-8"),
        logging.StreamHandler()
    ]
)

class JobScraper:
    def __init__(self):
        self.city_codes = {
            # "台北市": "6001001000"
            # "新北市": "6001002000",
            # "台中市": "6001008000",
            # "台南市": "6001014000",
            # "桃園市": "6001005000",
            # "高雄市": "6001016000",
            "新竹縣市": "6001006000",
            # "彰化縣": "6001010000",
            # "雲林縣": "6001012000",
            # "屏東縣": "6001018000"
         }

        self.job_codes = {
            "儲備幹部": "2001001002",
            "經營管理主管": "2001001001",
            "主管特別助理": "2001001003",
            "副總經理": "2001001004",
            "總經理": "2001001005",
            "執行長": "2001001006",
            "營運長": "2001001007",
            "人力資源人員": "2001002002",
            "人力資源助理": "2001002005",
            "人力資源主管": "2001002001",
            "教育訓練人員": "2001002003",
            "人力／外勞仲介": "2001002004",
            "招募顧問": "2001002006",
            "行政助理": "2002001012",
            "行政人員": "2002001003",
            "行政主管": "2002001001",
            "工讀生": "2002001011",
            "秘書": "2002001005",
            "總務": "2002001004",
            "總務主管": "2002001002",
            "櫃檯接待人員": "2002001010",
            "資料輸入人員": "2002001006",
            "總機": "2002001009",
            "文件管理師": "2002001007",
            "圖書管理人員": "2002001008",
            "律師": "2002002002",
            "法務": "2002002004",
            "法務助理": "2002002008",
            "法務主管": "2002002001",
            "商標／專利人員": "2002002005",
            "專利工程師": "2002002009",
            "專利師": "2002002010",
            "法遵人員": "2002002011",
            "工商登記人員": "2002002007",
            "代書／地政士": "2002002003",
            "其他法律專業人員": "2002002006",
            "銀行辦事員": "2003002007",
            "證券營業員": "2003002019",
            "理財專員": "2003002006",
            "金融交易員": "2003002003",
            "金融營業員": "2003002005",
            "金融承銷員": "2003002004",
            "金融研究員": "2003002002",
            "金融主管": "2003002001",
            "保險業務／經紀人": "2003002010",
            "保險主管": "2003002018",
            "融資／信用業務人員": "2003002011",
            "核保／保險內勤人員": "2003002013",
            "理賠人員": "2003002014",
            "股務人員": "2003002016",
            "催收人員": "2003002012",
            "券商後線人員": "2003002015",
            "統計精算人員": "2003002008",
            "投資經理人": "2003002017",
            "風險管理人員": "2003002020",
            "不動產估價師": "2003002009",
            "記帳／出納／一般會計": "2003001006",
            "主辦會計": "2003001004",
            "成本會計": "2003001005",
            "財務會計助理": "2003001010",
            "財務分析／財務人員": "2003001003",
            "財務或會計主管": "2003001001",
            "稽核人員": "2003001008",
            "稽核主管": "2003001011",
            "會計師": "2003001002",
            "查帳／審計人員": "2003001007",
            "財務長": "2003001012",
            "記帳士": "2003001013",
            "稅務人員": "2003001009",
            "社群行銷": "2004001014",
            "行銷企劃": "2004001005",
            "行銷助理": "2004001012",
            "行銷主管": "2004001002",
            "網站行銷企劃": "2004001007",
            "產品行銷企劃": "2004001004",
            "活動企劃": "2004001006",
            "廣告文案／企劃": "2004001009",
            "廣告企劃主管": "2004001001",
            "不動產／商場開發人員": "2004001011",
            "市場調查／市場分析": "2004001010",
            "神秘客": "2004001015",
            "媒體公關人員／主管": "2004001003",
            "公關助理": "2004001016",
            "媒體公關／宣傳採買": "2004001008",
            "媒體或出版主管": "2004001013",
            "行銷總監": "2004001017",
            "數位行銷": "2004001018",
            "電商行銷": "2004001019",
            "遊戲企劃": "2004002005",
            "網站企劃": "2004002006",
            "傳播媒體企劃": "2004002003",
            "出版企劃": "2004002004",
            "產品企劃": "2004002002",
            "產品企劃主管": "2004002001",
            "專案經理": "2004003006",
            "專案助理": "2004003007",
            "產品管理師": "2004003005",
            "永續管理師": "2004003008",
            "專案管理主管": "2004003001",
            "軟體專案管理師": "2004003003",
            "營運管理師／系統整合／ERP專案師": "2004003002",
            "其他專案管理師": "2004003004",
            "電話客服": "2005001004",
            "客服主管": "2005001001",
            "文字客服": "2005001006",
            "電訪人員": "2005001007",
            "其他客服人員": "2005001005",
            #"門市／店員／專櫃人員": "2005002004",
            "店長／賣場管理人員": "2005002001",
            "售票／收銀人員": "2005002005",
            "連鎖店管理人員": "2005002002",
            "商化人員": "2005002006",
            "國內業務": "2005003004",
            "國外業務": "2005003005",
            "國內業務主管": "2005003001",
            "國外業務主管": "2005003002",
            "內勤業務": "2005003016",
            "業務助理": "2005003013",
            "醫藥業務代表": "2005003008",
            "電話行銷人員": "2005003007",
            "不動產經紀人": "2005003009",
            "汽車銷售人員": "2005003010",
            "廣告AE業務": "2005003006",
            "通路開發人員": "2005003015",
            "專案業務主管": "2005003003",
            "產品事業處主管": "2005003014",
            "傳銷人員": "2005003011",
            "駐校代表": "2005003012",
            "國貿人員": "2005004001",
            "國貿助理": "2005004004",
            "船務／報關人員": "2005004002",
            "保稅人員": "2005004003",
            "餐廚助手": "2006001008",
            "餐飲服務生": "2006001001",
            "咖啡師": "2006001012",
            "茶師": "2006001017",
            "調酒師／吧台人員": "2006001007",
            "洗碗人員": "2006001010",
            "食品技師": "2006001013",
            "西餐廚師": "2006001003",
            "中餐廚師": "2006001002",
            "日式廚師": "2006001011",
            "其他類廚師": "2006001004",
            "麵包師": "2006001005",
            "麵包學徒": "2006001014",
            "西點／蛋糕師": "2006001006",
            "侍酒師": "2006001015",
            "食品衛生管理師": "2006001009",
            "行政主廚": "2006001016",
            "房務": "2006002009",
            "飯店工作人員": "2006002003",
            "領隊": "2006002006",
            "導遊": "2006002007",
            "導覽員": "2006002010",
            "地勤人員": "2006002005",
            "空服員": "2006002004",
            "OP／旅行社人員": "2006002008",
            "飯店或餐廳主管": "2006002002",
            "旅遊休閒類主管": "2006002001",
            "美容師": "2006003001",
            "美容助理": "2006003008",
            "美甲師": "2006003007",
            "美甲助理": "2006003015",
            "美髮師": "2006003002",
            "美髮助理": "2006003009",
            "寵物美容師": "2006003006",
            "寵物美容助理": "2006003011",
            "寵物保姆": "2006003012",
            "美療／芳療師": "2006003005",
            "醫美諮詢師": "2006003013",
            "整體造型師": "2006003003",
            "美睫師": "2006003010",
            "紋繡師": "2006003014",
            "美姿美儀人員": "2006003004",
            "美容主管": "2006003016",
            "彩妝師": "2006003017",
            "iOS工程師": "2007001013",
            "Android工程師": "2007001014",
            "前端工程師": "2007001015",
            "後端工程師": "2007001016",
            "全端工程師": "2007001017",
            "數據分析師": "2007001018",
            "軟體工程師": "2007001004",
            "軟體助理工程師": "2007001019",
            "軟體專案主管": "2007001001",
            "系統分析師": "2007001007",
            "資料科學家": "2007001021",
            "資料工程師": "2007001022",
            "AI工程師": "2007001020",
            "演算法工程師": "2007001012",
            "韌體工程師": "2007001005",
            "電玩程式設計師": "2007001008",
            "Internet程式設計師": "2007001006",
            "資訊助理": "2007001010",
            "區塊鏈工程師": "2007001023",
            "BIOS工程師": "2007001011",
            "通訊軟體工程師": "2007001003",
            "電子商務技術主管": "2007001002",
            "其他資訊專業人員": "2007001009",
            "系統工程師": "2007002006",
            "網路管理工程師": "2007002005",
            "資安工程師": "2007002009",
            "資訊設備管制人員": "2007002007",
            "雲端工程師": "2007002010",
            "網路安全分析師": "2007002008",
            "MES工程師": "2007002004",
            "MIS程式設計師": "2007002003",
            "資料庫管理人員": "2007002002",
            "'MIS／網管主管'": "2007002001",
            "資安主管": "2007002011",
            "領班": "2010001001",
            "作業員／包裝員": "2010001002",
            "銑床人員": "2010001007",
            "車床人員": "2010001006",
            "CNC機台操作人員": "2010001004",
            "CNC電腦程式編排人員": "2010001005",
            "手工包裝工及有關工作者": "2010001028",
            "塑膠射出技術人員": "2010001011",
            "塑膠模具技術人員": "2010001009",
            "機加工技術人員": "2010001010",
            "機械裝配員": "2010001023",
            "線切割技術員": "2010001035",
            "雷射操作技術員": "2010001036",
            "精密拋光技術人員": "2010001034",
            "其他機械操作員": "2010001029",
            "沖壓模具技術人員": "2010001008",
            "焊接人員": "2010001016",
            "板金技術員": "2010001014",
            "塗裝技術人員": "2010001039",
            "電機工程技術員": "2010001003",
            "電機設備裝配員": "2010001024",
            "紡織工務": "2010001017",
            "噴漆人員": "2010001013",
            "打版人員": "2010001018",
            "鍋爐操作技術人員": "2010001038",
            "壓鑄模具技術人員": "2010001032",
            "鑄造／鍛造模具技術人員": "2010001030",
            "電鍍／表面處理技術人員": "2010001033",
            "粉末冶金模具技術人員": "2010001031",
            "PCB技術人員": "2010001015",
            "推土機操作員": "2010001026",
            "吊車司機": "2010001027",
            "車縫人員": "2010001020",
            "染整人員": "2010001037",
            "製鞋人員": "2010001019",
            "印刷技術人員": "2010001012",
            "珠寶及貴金屬技術員": "2010001022",
            "樂器製造及調音技術員": "2010001021",
            "農業及林業設備操作員": "2010001025",
            "挖土機司機": "2010001040",
            "FAE工程師": "2010002016",
            "客服工程師": "2010002017",
            "產維修人員": "2010002005",
            "產品售後技術服務": "2010002001",
            "業務支援工程師": "2010002002",
            "空調冷凍技術人員": "2010002006",
            "電機裝修工": "2010002011",
            "電子設備裝修工": "2010002012",
            "通信測試維修人員": "2010002004",
            "電信及電力線路架設工": "2010002014",
            "精密儀器製造工及修理工": "2010002015",
            "電話及電報機裝修工": "2010002013",
            "電腦組裝／測試": "2010002003",
            "汽車檢驗員": "2010002018",
            "汽車學徒": "2010002019",
            "機車學徒": "2010002020",
            "汽車／機車引擎技術人員": "2010002007",
            "其他汽車／機車技術維修人員": "2010002008",
            "飛機裝修工": "2010002009",
            "農業及工業用機器裝修工": "2010002010",
            "倉管": "2011001004",
            "採購助理": "2011001006",
            "採購人員": "2011001003",
            "採購主管": "2011001001",
            "物管／資材": "2011001005",
            "資材主管": "2011001002",
            "倉儲物流人員": "2011002009",
            "快遞": "2011002003",
            "外送員": "2011002011",
            "小客車／計程車及小貨車司機": "2011002005",
            "大貨車及其他類司機": "2011002006",
            "運輸交通人員": "2011002002",
            "運輸物流類主管": "2011002001",
            "主管司機": "2011002014",
            "堆高機人員": "2011002012",
            "隨車人員": "2011002013",
            "船長／大副／船員": "2011002010",
            "飛安人員": "2011002008",
            "飛行機師": "2011002007",
            "鐵路車輛駕駛員": "2011002004",
            "聯結車司機": "2011002015",
            "工務人員／助理": "2012001010",
            "土木技師／工程師": "2012001004",
            "結構技師／工程師": "2012001005",
            "水技師／工程師": "2012001008",
            "建築師": "2012001002",
            "內業工程師": "2012001009",
            "營建主管": "2012001001",
            "土地開發人員": "2012001014",
            "水電工程師": "2012001011",
            "水利技師／工程師": "2012001006",
            "估算人員": "2012001012",
            "工程配管繪圖": "2012001007",
            "發包人員": "2012001013",
            "都市／交通規劃人員": "2012001003",
            "工地監工／主任": "2012002002",
            "木工": "2012002006",
            "木工學徒": "2012002016",
            "水電工": "2012002013",
            "水電學徒": "2012002015",
            "營造工程師": "2012002001",
            "建築物清潔工": "2012002009",
            "建築物電力系統維修工": "2012002003",
            "金屬建材架構人員": "2012002010",
            "油漆工": "2012002008",
            "混凝土工": "2012002005",
            "泥水工": "2012002007",
            "泥水小工及有關工作者": "2012002011",
            "防水施工人員": "2012002014",
            "砌磚工及砌石工": "2012002004",
            "其他營建構造工": "2012002012",
            "室內設計師": "2012003006",
            "室內設計助理": "2012003008",
            "軟裝設計師": "2012003009",
            "建築設計師": "2012003002",
            "景觀設計師": "2012003007",
            "水電及其他工程繪圖人員": "2012003003",
            "消防繪圖人員": "2012003010",
            "機械設計工程師": "2012003004",
            "CAD／CAM工程師": "2012003001",
            "量測／儀校人員": "2012003005",
            "平面設計／美編": "2013001005",
            "美編助理": "2013001019",
            "視覺設計師": "2013001017",
            "產品設計師": "2013001020",
            "UI設計師": "2013001015",
            "UX設計師": "2013001016",
            "網頁設計師": "2013001006",
            "工業設計師": "2013001010",
            "多媒體動畫設計師": "2013001004",
            "多媒體開發主管": "2013001001",
            "展場／櫥窗佈置人員": "2013001003",
            "服裝／皮包／鞋類設計": "2013001009",
            "服裝設計助理": "2013001018",
            "電腦繪圖人員": "2013001012",
            "設計助理": "2013001013",
            "美術設計": "2013001007",
            "商業設計": "2013001008",
            "廣告設計": "2013001002",
            "包裝設計": "2013001011",
            "織品設計": "2013001014",
            "攝影師": "2013002012",
            "攝影助理": "2013002015",
            "模特兒": "2013002004",
            "直播主": "2013002018",
            "演員": "2013002002",
            "主持人": "2013002017",
            "剪輯師": "2013002019",
            "剪輯助理": "2013002020",
            "影音企劃": "2013002021",
            "影片製作技術人員": "2013002010",
            "經紀人": "2013002022",
            "製片": "2013002023",
            "製片助理": "2013002024",
            "編劇": "2013002025",
            "節目製作人員": "2013002001",
            "節目助理": "2013002014",
            "視聽工程人員": "2013002016",
            "燈光／音響師": "2013002011",
            "導演導播": "2013002003",
            "音樂家／作曲／歌唱及演奏家": "2013002005",
            "舞蹈指導與舞蹈家": "2013002006",
            "藝術指導 ／藝術總監": "2013002007",
            "播音／配音人員": "2013002009",
            "電台工作人員": "2013002008",
            "其他娛樂事業人員": "2013002013",
            "特效師": "2013002026",
            "3D建模師": "2013002027",
            "英文翻譯": "2014001002",
            "日文翻譯": "2014001003",
            "韓文翻譯": "2014001007",
            "越南翻譯": "2014001008",
            "印尼翻譯": "2014001009",
            "泰文翻譯": "2014001010",
            "菲律賓翻譯": "2014001011",
            "德文翻譯": "2014001012",
            "西班牙文翻譯": "2014001013",
            "法文翻譯": "2014001014",
            "其他翻譯": "2014001004",
            "文編／校對／文字工作者": "2014001005",
            "編輯助理": "2014001015",
            "技術文件／說明書編譯": "2014001001",
            "排版人員": "2014001006",
            "記者": "2014002001",
            "其他傳媒工作": "2014002002",
            "藥師": "2015001005",
            "藥師助理": "2015001015",
            "護理師及護士": "2015001004",
            "專科護理師": "2015001021",
            "居家護理師": "2015001026",
            "研究護理師": "2015001027",
            "護理長": "2015001028",
            "營養師": "2015001006",
            "健康管理師": "2015001025",
            "物理治療師": "2015001019",
            "勞工健康服務護理人員": "2015001024",
            "職能治療師": "2015001018",
            "驗光師": "2015001011",
            "醫事放射師": "2015001016",
            "醫師": "2015001001",
            "醫事檢驗師": "2015001003",
            "獸醫師": "2015001007",
            "心理師": "2015001023",
            "聽力師": "2015001029",
            "語言治療師": "2015001020",
            "呼吸治療師": "2015001017",
            "牙醫師": "2015001002",
            "牙體技術師": "2015001022",
            "中醫師": "2015001009",
            "治療師": "2015001013",
            "麻醉醫師": "2015001010",
            "公共衛生醫師": "2015001008",
            "復健技術師": "2015001012",
            "其他醫療人員": "2015001014",
            "診所助理": "2015002005",
            "牙醫助理": "2015002006",
            "獸醫助理": "2015002015",
            "照顧服務員": "2015002002",
            "照顧實務指導員": "2015002011",
            "居家服務督導員": "2015002012",
            "安心服務員": "2015002010",
            "個案管理師": "2015002014",
            "專任管理人員": "2015002013",
            "醫院行政管理人員": "2015002001",
            "放射性設備使用技術員": "2015002007",
            "醫療設備控制人員": "2015002008",
            "按摩／推拿師": "2015002004",
            "其他醫療從業人員": "2015002009",
            "研究助理": "2016001013",
            "生物學研究員": "2016001011",
            "統計學研究員": "2016001007",
            "心理學研究員": "2016001010",
            "物理學研究員": "2016001001",
            "化學研究員": "2016001004",
            "數學研究員": "2016001006",
            "應用科學研究員": "2016001012",
            "地質與地球科學研究員": "2016001005",
            "氣象學研究員": "2016001003",
            "哲學／歷史／政治研究員": "2016001009",
            "社會／人類學研究員": "2016001008",
            "天文學研究員": "2016001002",
            "其他研究人員": "2016001014",
            "英文老師": "2016002024",
            "日文老師": "2016002025",
            "韓文老師": "2016002026",
            "語文補習班老師": "2016002011",
            "補習班導師／管理人員": "2016002001",
            "健身教練": "2016002027",
            "運動教練": "2016002019",
            "家教": "2016002028",
            "教保員": "2016002020",
            "托育員": "2016002023",
            "安親班老師": "2016002008",
            "幼教班老師": "2016002007",
            "幼兒園園長": "2016002029",
            "社工": "2016002018",
            "就業服務員": "2016002030",
            "講師": "2016002021",
            "助教": "2016002003",
            "教授／副教授／助理教授": "2016002002",
            "升學補習班老師": "2016002009",
            "數理補習班老師": "2016002022",
            "電腦補習班老師": "2016002010",
            "中文老師": "2016002031",
            "作文老師": "2016002032",
            "數學老師": "2016002033",
            "美術老師": "2016002013",
            "音樂老師": "2016002014",
            "游泳教練": "2016002034",
            "鋼琴老師": "2016002035",
            "珠心算老師": "2016002012",
            "中等學校教師": "2016002004",
            "國小學校教師": "2016002005",
            "特殊教育教師": "2016002006",
            "其他才藝類老師": "2016002016",
            "其他補習班老師": "2016002015",
            "其他類講師": "2016002017",
            "助理工程師": "2008001023",
            "工程助理": "2008001024",
            "機械工程師": "2008001006",
            "電子工程師": "2008001009",
            "電力工程師": "2008001030",
            "電源工程師": "2008001013",
            "數位IC設計工程師": "2008001015",
            "類比IC設計工程師": "2008001014",
            "IC佈局工程師": "2008001022",
            "半導體工程師": "2008001016",
            "光學工程師": "2008001019",
            "熱傳工程師": "2008001028",
            "零件工程師": "2008001010",
            "光電工程師": "2008001018",
            "光電工程研發主管": "2008001002",
            "RF通訊工程師": "2008001021",
            "電信／通訊系統工程師": "2008001020",
            "通訊工程研發主管": "2008001003",
            "太陽能技術工程師": "2008001027",
            "PCB佈線工程師": "2008001012",
            "硬體研發工程師": "2008001011",
            "硬體工程研發主管": "2008001001",
            "電子產品系統工程師": "2008001026",
            "微機電工程師": "2008001017",
            "聲學／噪音工程師": "2008001029",
            "機電技師／工程師": "2008001008",
            "電機師／工程師": "2008001005",
            "其他特殊工程師": "2008001025",
            "其他工程研發主管": "2008001004",
            "材料研發人員": "2008002004",
            "化工化學工程師": "2008002001",
            "實驗化驗人員": "2008002005",
            "特用化學工程師": "2008002003",
            "紡織化學工程師": "2008002002",
            "生物科技研發人員": "2008003003",
            "醫藥研發人員": "2008003002",
            "醫療器材研發工程師": "2008003007",
            "食品研發人員": "2008003001",
            "化學工程研發人員": "2008003004",
            "病理藥理人員": "2008003005",
            "農藝／畜產研究人員": "2008003006",
            "生管": "2009001004",
            "生管助理": "2009001005",
            "生產管理主管": "2009001001",
            "工業工程師／生產線規劃": "2009001003",
            "廠長": "2009001006",
            "工廠主管": "2009001002",
            "自動控制工程師": "2009002003",
            "生產設備工程師": "2009002002",
            "SMT工程師": "2009002004",
            "半導體製程工程師": "2009002005",
            "半導體設備工程師": "2009002007",
            "LCD製程工程師": "2009002006",
            "LCD設備工程師": "2009002008",
            "生產技術／製程工程師": "2009002001",
            "軟韌體測試工程師": "2009003007",
            "可靠度工程師": "2009003003",
            "測試人員": "2009003005",
            "硬體測試工程師": "2009003004",
            "'EMC／電子安規工程師'": "2009003008",
            "'IC封裝／測試工程師'": "2009003006",
            "品管／檢驗人員": "2009003010",
            "品管／品保工程師": "2009003002",
            "'ISO／品保人員'": "2009003009",
            "品管／品保主管": "2009003001",
            "故障分析工程師": "2009003011",
            "廠務": "2009004006",
            "廠務助理": "2009004007",
            "職業安全衛生管理員": "2009004008",
            "職業安全衛生管理師": "2009004001",
            "環境工程人員／工程師": "2009004003",
            "安全／衛生相關檢驗人員": "2009004002",
            "公共衛生人員": "2009004005",
            "防火及建築檢驗人員": "2009004004",
            "救生員": "2017001004",
            "消防專業人員": "2017001002",
            "消防員": "2017001003",
            "志願役軍官／士官／士兵": "2017001001",
            "保全人員／警衛": "2017002001",
            "保全技術人員": "2017002002",
            "總幹事": "2017002005",
            "社區秘書": "2017002006",
            "大樓管理員": "2017002003",
            "其他保安服務工作": "2017002004",
            "農藝作物栽培工作者": "2018001001",
            "一般動物飼育工作者": "2018001002",
            "水產養殖工作者": "2018001004",
            "林木伐運工作者": "2018001003",
            "清潔工／資源回收人員": "2018002012",
            "家庭代工": "2018002007",
            "家事服務人員": "2018002013",
            "加油員": "2018002010",
            "顧問": "2018002001",
            "花藝／園藝人員": "2018002008",
            "花藝助理": "2018002015",
            "汽車美容人員": "2018002014",
            "生鮮人員": "2018002009",
            "派報生／傳單派送": "2018002011",
            "公家機關相關人員": "2018002005",
            "生命禮儀師": "2018002003",
            "志工": "2018002002",
            "藝術品／珠寶鑑價／拍賣顧問": "2018002006",
            "星象占卜人員": "2018002004"
        }

        self.session = self._create_session()
        self._init_headers()
        self.base_wait_time = 6
        
    def _init_headers(self):
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Host': 'www.104.com.tw',
            'Origin': 'https://www.104.com.tw',
            'Referer': 'https://www.104.com.tw/jobs/search/',
            'User-Agent': self._get_random_ua()
        }

    def _get_random_ua(self):
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0'
        ]
        return random.choice(user_agents)

    def _create_session(self):
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _init_session(self):
        try:
            self.session.get(
                'https://www.104.com.tw/jobs/search/',
                headers=self.headers,
                timeout=10
            )
            return True
        except Exception as e:
            logging.error(f"Error initializing session: {str(e)}")
            return False

    def _exponential_backoff(self, attempt):
        # The backoff delay now starts with self.base_wait_time (which is 5 seconds now)
        return min(self.base_wait_time * (2 ** attempt) + random.uniform(0, 1), 60)

    def get_request(self, url, params=None, headers=None, attempt=0):
        if not headers:
            headers = self.headers

        try:
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            try:
                json_data = response.json()
            except ValueError as json_err:
                logging.error(f"Error decoding JSON for URL: {url} with params: {params}. Error: {json_err}")
                return None

            # Ensure that the returned data is a dictionary.
            if not isinstance(json_data, dict):
                logging.warning(f"Expected JSON object to be a dict, got {type(json_data)} instead. Data: {json_data}")
                return None

            return json_data

        except requests.exceptions.RequestException as e:
            # Calculate the wait time using exponential backoff.
            wait_time = self._exponential_backoff(attempt)
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                retry_after = e.response.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait_time = float(retry_after)
                    except ValueError:
                        # If conversion fails, we stick with the exponential backoff value.
                        pass

            logging.error(f"Request failed for URL: {url} with params: {params}. Error: {str(e)}")
            if attempt < 5:
                logging.info(f"Retrying in {wait_time:.2f} seconds... (Attempt {attempt + 1}/5)")
                time.sleep(wait_time)
                return self.get_request(url, params, headers, attempt + 1)
            else:
                logging.error(f"Max retries reached for URL: {url}")
                return None
   
    def fetch_jobs(self, city_code, job_code):
        # Use the new API endpoint.
        url = 'https://www.104.com.tw/jobs/search/api/jobs'
        all_jobs = []
        # Determine the human-readable job category (e.g., "儲備幹部")
        job_name = next((key for key, value in self.job_codes.items() if value == job_code), None)

        if not self._init_session():
            logging.error("Failed to initialize session")
            return []

        # Set a fallback maximum; these will be updated from the API metadata.
        max_pages = 150
        expected_page_size = None  # Will be determined from page 1
        total_pages = max_pages    # Default if metadata is missing

        for page in range(1, max_pages + 1):
            # Build the query parameters based on the browser's request.
            params = {
                'area': city_code,
                'jobcat': job_code,
                'jobsource': 'index_s',
                'mode': 's',
                'order': '16',
                'page': str(page),
                'pagesize': '20',
                'searchJobs': '1'
            }

            logging.info(f"Fetching page {page} for job_code {job_code} ({job_name}) in city_code {city_code}...")
            response = self.get_request(url, params=params)

            if not response or 'data' not in response:
                logging.warning(f"Unexpected response format: {response}")
                break

            # The new API returns a list of jobs under the 'data' key and
            # metadata (including pagination info) under 'metadata'.
            jobs = response['data']

            # On page 1, extract the pagination metadata.
            if page == 1:
                metadata = response.get('metadata', {})
                pagination = metadata.get('pagination', {})
                expected_page_size = pagination.get('count', len(jobs))
                total_pages = pagination.get('lastPage', max_pages)
                total_count = pagination.get('total')
                logging.info(f"Detected expected page size: {expected_page_size}, total jobs: {total_count}, total pages: {total_pages}")

            # If no jobs are returned, break out.
            if not jobs:
                logging.info("No jobs found on this page. Ending pagination.")
                break

            # (optional) Only use the "fewer jobs" check as a fallback when metadata isn't available.
            #if total_pages == max_pages and expected_page_size is not None and len(jobs) < expected_page_size:
            #    logging.info("Fewer jobs than expected on this page; assuming this is the last page.")
            #    all_jobs.extend(jobs)
            #    break

            # (Optional) If the page returns fewer jobs than expected, assume it's the last page.
            #if expected_page_size is not None and len(jobs) < expected_page_size:
            #    logging.info("Fewer jobs than expected on this page; assuming this is the last page.")
            #    all_jobs.extend(jobs)
            #    break

            # Process each job on the current page.
            for job in jobs:
                # Store the UI (searched) category for reference.
                job['JobCat'] = job_name

                # Extract the unique job detail code from the job's link.
                link_data = job.get('link', {})
                apply_analyze = link_data.get('applyAnalyze', '')
                if apply_analyze:
                    # Example: if apply_analyze is like "https://www.104.com.tw/job/82p52?...", extract "82p52"
                    extracted_job_code = apply_analyze.split('/')[6].split('?')[0]
                    job['code'] = extracted_job_code
                else:
                    logging.warning(f"Missing 'applyAnalyze' in job link: {link_data}")
                    continue

                # Set up headers for fetching job details.
                header = {
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Encoding': 'gzip, deflate, br, zstd',
                    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Connection': 'keep-alive',
                    'Host': 'www.104.com.tw',
                    'Referer': 'https://www.104.com.tw/',
                    'User-Agent': self._get_random_ua()
                }
                # Construct the URL for job detail retrieval.
                job_detail_url = f"https://www.104.com.tw/job/ajax/content/{job['code']}"
                rep = self.get_request(job_detail_url, headers=header)

                if rep and 'data' in rep:
                    # Store conditions and job categories from the job detail.
                    job['condition'] = rep['data'].get('condition', {})
                    job['jobCategory'] = rep['data']['jobDetail'].get('jobCategory', {})

                    # Fetch company details.
                    cust_url = rep['data']['header'].get('custUrl', None)
                    if cust_url:
                        company_code = cust_url.split('/')[-1]  # Extract the company code.
                        company_url = f"https://www.104.com.tw/company/ajax/content/{company_code}"
                        company_response = self.get_request(company_url)
                        if company_response and 'data' in company_response:
                            job['company_employees'] = company_response['data'].get('empNo', 'N/A')
                            job['company_capital'] = company_response['data'].get('capital', 'N/A')
                        else:
                            logging.warning(f"Failed to fetch company details for company_code: {company_code}")
                    else:
                        logging.warning(f"Missing 'custUrl' in response header: {rep['data']['header']}")
                    
                    # Add an extra delay here to throttle company detail requests.
                    time.sleep(random.uniform(0.5, 1.5))
                
                else:
                    logging.warning(f"Failed to fetch job details for code: {job['code']}")

            # Add the processed jobs from this page to the overall list.
            all_jobs.extend(jobs)
            time.sleep(random.uniform(2, 5))

            # If we've reached the last page based on the metadata, stop looping.
            if page >= total_pages:
                logging.info("Reached the last page based on pagination metadata.")
                break

        total_fetched = len(all_jobs)
        logging.info(f"Total jobs fetched for job_code {job_code} ({job_name}): {len(all_jobs)}")
        
        # Store the count in a dictionary attribute (initialize if necessary)
        if not hasattr(self, 'extraction_counts'):
            self.extraction_counts = {}
        if job_name not in self.extraction_counts:
            self.extraction_counts[job_name] = {}
        # Using city_code or city_name based on your needs. Here, we'll assume city_name is available:
        self.extraction_counts[job_name][city_code] = total_fetched
        
        return all_jobs
    
    def save_job_to_csv(self, jobs, city_name, job_name):
    # '''
    #     Save a single job to a CSV file. Each job is stored as a separate CSV.
    # '''
        try:
            if not jobs:
                logging.warning(f"No jobs found for {city_name} - {job_name}, skipping save.")
                return
        
            # Ensure the directory exists (change to your path)
            base_dir = "C:/Users/whcnt/OneDrive/RA2024/104_JobData_new/05_2025/抓取原檔_縣市/Hsinchu"
            os.makedirs(base_dir, exist_ok=True)

            # Sanitize the city and job names
            safe_city_name = sanitize_filename(city_name)
            safe_job_name = sanitize_filename(job_name)
        
            # Construct the file name using sanitized strings
            filename = f'jobs_{safe_city_name}_{safe_job_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            file_path = os.path.join(base_dir, filename)
        
            # Construct full file path (original)
            #filename = f'jobs_{city_name}_{job_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            #file_path = os.path.join(base_dir, filename)

            # Convert job list to DataFrame and save as CSV
            df = pd.DataFrame(jobs)
            df.to_csv(file_path, index=False, encoding='utf-8-sig')

            logging.info(f"Saved {len(jobs)} jobs in {city_name} to file: {file_path}")
            print(f"File successfully saved: {file_path}")  # Additional debugging confirmation

        except Exception as e:
            logging.error(f"Error saving jobs to CSV: {str(e)}")
            
    def generate_metadata_summary(self):
        
        #For each job category (row) and each city (column),
        #retrieve the total number of jobs available (from API metadata on page 1).
        
        summary = {}  # {job_category: {city: total_count, ...}, ...}
        base_api_url = 'https://www.104.com.tw/jobs/search/api/jobs'
        
        for job_name, job_code in self.job_codes.items():
            summary[job_name] = {}
            for city_name, city_code in self.city_codes.items():
                params = {
                    'area': city_code,
                    'jobcat': job_code,
                    'jobsource': 'index_s',
                    'mode': 's',
                    'order': '16',
                    'page': '1',        # Only need the first page for metadata
                    'pagesize': '20',     # The pagesize used by the API
                    'searchJobs': '1'
                }
                logging.info(f"Retrieving metadata for {city_name} - {job_name}")
                response = self.get_request(base_api_url, params=params)
                
                # Debug: log the keys in the API response to verify its structure.
                if response is not None:
                    logging.info(f"Response keys for {city_name} - {job_name}: {list(response.keys())}")
                else:
                    logging.warning(f"Received no response for {city_name} - {job_name}")
            
                total_count = 0
                if response and 'metadata' in response:
                    metadata = response.get('metadata', {})
                    pagination = metadata.get('pagination', {})
                    total_count = pagination.get('total', 0)
                    logging.info(f"Metadata for {city_name} - {job_name}: total={total_count}")
                else:
                    logging.warning(f"Metadata missing for {city_name} - {job_name}")
                    total_count = 0  # Use a default value if metadata is missing.
                summary[job_name][city_name] = total_count
                
                # Add a wait time to throttle the API requests
                #time.sleep(random.uniform(1, 2.0))
        
        # Convert the summary dictionary to a DataFrame.
        # Rows will be job categories; columns will be cities.
        df_meta = pd.DataFrame(summary).T  # transpose to get job categories as rows
        # Save the metadata summary to CSV.
        meta_filename = f'metadata_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        meta_filepath = os.path.join(base_dir, meta_filename)
        df_meta.to_csv(meta_filepath, encoding='utf-8-sig')
        logging.info(f"Metadata summary saved to {meta_filepath}")
        return df_meta

    def generate_extraction_summary(self):
        summary = {}  # {job_category: {city: extracted_count, ...}, ...}

        # Loop over all defined job categories and cities.
        for job_name, job_code in self.job_codes.items():
            summary[job_name] = {}
            for city_name, city_code in self.city_codes.items():
                # Look up the stored count; if not found, default to 0.
                count = 0
                if hasattr(self, 'extraction_counts'):
                    count = self.extraction_counts.get(job_name, {}).get(city_code, 0)
                summary[job_name][city_name] = count
                logging.info(f"Summary: {city_name} - {job_name}: {count} jobs")

        # Convert the summary dictionary to a DataFrame.
        # Rows: job categories; Columns: cities.
        df_extract = pd.DataFrame(summary).T

        # Save the extraction summary to CSV using self.base_dir if available.
        extract_filename = f'extraction_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        extract_filepath = os.path.join(base_dir, extract_filename)
        df_extract.to_csv(extract_filepath, encoding='utf-8-sig')
        
        #extract_filename = f'extraction_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        #save_dir = self.base_dir if hasattr(self, 'base_dir') and self.base_dir else os.getcwd()
        #extract_filepath = os.path.join(save_dir, extract_filename)
        #df_extract.to_csv(extract_filepath, encoding='utf-8-sig')
        logging.info(f"Extraction summary saved to {extract_filepath}")
        return df_extract
  
    def run(self):
        for city_name, city_code in self.city_codes.items():
            all_jobs = []

            for job_name, job_code in self.job_codes.items():
                logging.info(f"Fetching data for {city_name} - {job_name}")
                try:
                    jobs = self.fetch_jobs(city_code, job_code)
                    if jobs:
                        all_jobs.extend(jobs)
                        self.save_job_to_csv(jobs, city_name, job_name)
                        time.sleep(random.uniform(20, 30))
                except Exception as e:
                    logging.error(f"Error processing {city_name} - {job_name}: {str(e)}")
                    continue

            if all_jobs:
                filename = f'./job_104_data_{city_name}_{job_name}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                df = pd.DataFrame(all_jobs)
                # folder_path ="c:/Users/wenha/Dropbox/Python/104_codes/CSV"
                # csv_file_path = os.path.join(folder_path,f'./job_104_data_{city_name}_{job_name}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv')
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                # SAVE TO 'CSV' file folder
                
                logging.info(f"Successfully saved data to {filename}")
                
if __name__ == "__main__":
    scraper = JobScraper()
    #scraper.run()
    
    # Step 1: Generate and save the summary count table (metadata summary)
    logging.info("Generating summary count table from API metadata...")
    metadata_summary_df = scraper.generate_metadata_summary()
    logging.info("Metadata summary table generated and saved.")

    # Optional: Ask the user if detailed extraction should be run.
    proceed = input("Do you want to proceed with the detailed extraction process? (y/n): ").strip().lower()
    
    if proceed == 'y':
        #Step 2: Run the detailed extraction process.
        logging.info("Starting detailed extraction process...")
        scraper.run()
        
        # Step 3: Generate and save the extraction summary based on detailed extraction
        logging.info("Generating extraction summary from detailed extraction counts...")
        extraction_summary_df = scraper.generate_extraction_summary()
        logging.info("Extraction summary generated and saved.")
        
    else:
        logging.info("Detailed extraction process skipped. Exiting now.")

