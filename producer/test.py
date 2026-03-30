import requests
import xml.etree.cElementTree as ET
from dotenv import load_dotenv
import os
from urllib.parse import unquote,quote
from requests.exceptions import RequestException

load_dotenv()

raw_api_key=os.getenv('HOSPITAL_API_KEY')
decoded_key=unquote(raw_api_key)

BASE_URL="http://apis.data.go.kr/B552657/ErmctInfoInqireService"

url=f"{BASE_URL}/getEmrrmRltmUsefulSckbdInfoInqire"
params={
    "serviceKey":decoded_key,
    "STAGE1":"",
    "STAGE2":"",
    "numOfRows":"10",
    "pageNo":"1"
}

try:
    r=requests.get(url,params=params,timeout=10)
    r.raise_for_status()
    root=ET.fromstring(r.content)
    
    
    total=root.findtext(".//totalCount","0")
    print(f"전체 병원수 :{total}")
    
    item=root.find(".//item")
    if item is not None:
        for child in item:
             print(f"{child.tag}: {child.text}")
    else:
        print("데이터 없음 ! ->STAGE1이 필수일수도 ")
        
except RequestException as e:
    print(f"네트워크 에러 -> {e}")
except ET.ParseError:
    print("XML파싱 에러")