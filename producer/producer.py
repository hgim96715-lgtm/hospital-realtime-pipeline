import os
import json
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import unquote   
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()


KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
API_KEY          = unquote(os.getenv("HOSPITAL_API_KEY", ""))  
BASE_URL         = "http://apis.data.go.kr/B552657/ErmctInfoInqireService"
POLL_INTERVAL    = 300   
NUM_OF_ROWS      = 1000
SUCCESS_CODES    = {"00", "0000"} 


def parse_int(val, default=None) -> int | None:
    """None 또는 빈 문자열을 안전하게 int 변환. 실패 시 None 반환"""
    try:
        return int(val) if val else default
    except (ValueError, TypeError):
        return default

# 공백이 있는 xml발견 함수 추가    
def parse_yn(val:str)->str:
    if not val:
        return ""
    v=val.strip()
    return v if v in ('Y','N') else ""

# xml 하나씩 확인해가면서 저거야함 debug ->홈페이지랑 다름
# 새로 추가: debug_xml()
def debug_xml(root, max_items=1):
    items = root.findall(".//item")
    print(f"[DEBUG] 전체 {len(items)}건")
    for item in items[:max_items]:
        for child in item:
            print(f"  {child.tag}: {repr(child.text)}")


# API 호출 
def fetch_xml(path: str, extra_params: dict = None) -> ET.Element | None:
    """공통 API 호출 + XML 파싱"""
    if extra_params is None:
        extra_params = {}

    params = {
        "serviceKey": API_KEY,
        "numOfRows":  NUM_OF_ROWS,
        "pageNo":    1,
        **extra_params
    }
    try:
        r = requests.get(f"{BASE_URL}/{path}", params=params, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        
        result_code = root.findtext(".//resultCode", "")
        if result_code not in SUCCESS_CODES:
            msg = root.findtext(".//resultMsg", "알 수 없음")
            print(f"[API 에러] {path}: {result_code} - {msg}")
            return None
        return root

    except requests.exceptions.Timeout:
        print(f"[타임아웃] {path}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[네트워크 에러] {path}: {e}")
        return None
    except ET.ParseError as e:
        print(f"[XML 파싱 에러] {path}: {e}")
        return None


def fetch_realtime_beds() -> dict:
    root = fetch_xml("getEmrrmRltmUsefulSckbdInfoInqire")
    if root is None:
        return {}

    result = {}
    #debug_xml(root) # 디버그용
    for item in root.findall(".//item"):
        hpid = item.findtext("hpid", "")
        if not hpid:
            continue
        result[hpid] = {
            "hpid":       hpid,
            "hpname":     item.findtext("dutyName", ""), #홈페이지 dutyname 실제 dutyName
            "hvec":       parse_int(item.findtext("hvec")),
            "hvoc":       parse_int(item.findtext("hvoc")),
            "hvgc" : parse_int(item.findtext("hvgc")),
            "hvctayn":    parse_yn(item.findtext("hvctayn")),
            "hvmriayn" : parse_yn(item.findtext("hvmriayn")),
            "hvangioayn": parse_yn(item.findtext("hvangioayn")), 
            "hvventiayn":  parse_yn(item.findtext("hvventiayn")),
            "duty_tel": item.findtext("dutyTel3","")
        }
    print(f"[가용병상] {len(result)}곳 병원 수집되었습니다.")
    return result




def fetch_messages(hpids: list) -> dict:
    result = {}
    for hpid in hpids:
        root = fetch_xml("getEmrrmSrsillDissMsgInqire", {"hpid": hpid})
        if root is None:
            result[hpid] = ""
            continue
        result[hpid] = root.findtext(".//symBlkMsg", "")
        time.sleep(0.3) # 너무 빨리 소진되어 증가 시키기 0.1 ->0.3 
    print(f"[메시지] {len(result)}곳 병원 수집 완료")
    return result


def fetch_messages_mock(hpids: list) -> dict:
    """
    메시지 API Mocking — 429 Too Many Requests 발생 시 임시 대체
    개발계정 트래픽 초과(1,000회/일) 시 가짜 데이터로 파이프라인 테스트..대체 안하면 오류나고 다른 검사때까진 Mock으로 대체
    실제 배포 시 fetch_messages() 로 교체
    """
    import random
    mock_messages = [
        "응급실 병상 여유 있습니다.",
        "현재 응급실 포화 상태입니다. (중증 환자 외 수용 불가)",
        "CT/MRI 장비 점검 중. 관련 환자 수용 불가.",
        "소아응급환자 정상 수용 가능합니다.",
        "응급수술 진행 중. 외상환자 수용 지연 (대기 2시간 예상)",
        "",   # 메시지 없는 병원도 있음
        "",
        "",
    ]
    result = {hpid: random.choice(mock_messages) for hpid in hpids}
    print(f"[메시지 Mock] {len(result)}개 병원 (가짜 데이터)")
    return result


# er_realtime 테이블에 넣기 위한 합치기 
def merge_data(beds: dict, messages: dict) -> list:
    merged = []
    now = datetime.now().isoformat()

    for hpid, bed_data in beds.items():
        row = {
            **bed_data,
            "notice_msg": messages.get(hpid, ""),
            "data_type":  "er_realtime",
            "created_at": now
        }
        merged.append(row)
    return merged


# Producer
class HospitalProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(
                v, ensure_ascii=False).encode("utf-8"),
            acks="all",
            retries=3,
        )
        print(f"[Hospital Producer] Kafka 연결: {KAFKA_BOOTSTRAP}")


        self.cached_messages   = {}
        self.last_msg_fetch_time = 0
        self.MSG_POLL_INTERVAL = 3600 

    def _send(self, topic: str, data: dict):
        try:
            self.producer.send(topic, value=data)
        except KafkaError as e:
            print(f"[Kafka 발행 에러] {topic}: {e}")

    def collect_and_publish(self):
        current_time = time.time()
        print(f"\n[수집 시작] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


        beds = fetch_realtime_beds()
        if not beds:
            print("[경고] 가용병상 데이터 없음")
            return 0



        if current_time - self.last_msg_fetch_time > self.MSG_POLL_INTERVAL:
            print("[메시지 갱신] 1시간 경과. API를 새로 호출합니다...")
            # self.cached_messages     = fetch_messages(list(beds.keys()))
            self.cached_messages   = fetch_messages_mock(list(beds.keys()))
            self.last_msg_fetch_time = current_time
        else:
            time_left = int(self.MSG_POLL_INTERVAL - (current_time - self.last_msg_fetch_time))
            print(f"[메시지 캐시] 기존 메시지 재사용 (다음 갱신: {time_left // 60}분 후)")

        # 병합
        merged = merge_data(beds, self.cached_messages)

        # 발행
        for row in merged:
            self._send("er-realtime", row)

        self.producer.flush()
        print(f"[발행 완료] {len(merged)}건 → er-realtime 토픽")
        return len(merged)

    def run(self):
        print(f"🚑 Hospital Producer 가동 시작 (수집 주기: {POLL_INTERVAL}초)\n")
        while True:
            try:
                count = self.collect_and_publish()
                if count == 0:
                    print("[경고] 발행된 데이터가 없습니다. API 상태를 확인하세요.")
                print(f"[대기] {POLL_INTERVAL}초 후 다음 주기를 시작합니다...\n")
                time.sleep(POLL_INTERVAL)
            except KeyboardInterrupt:
                print("\n[종료] Producer 프로세스를 안전하게 중지합니다.")
                self.producer.close()
                break
            except Exception as e:
                print(f"[예외 발생] {e}")
                time.sleep(60)


if __name__ == "__main__":
    
    hp = HospitalProducer()
    hp.run()