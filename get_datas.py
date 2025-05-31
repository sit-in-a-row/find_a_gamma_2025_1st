import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial

BASE_URL = "https://www.deribit.com/api/v2"

# 1. 전체 옵션 리스트 수집 (미만기 + 만기 포함)
def get_all_option_instruments(currency="BTC"):
    url_live = f"{BASE_URL}/public/get_instruments?currency={currency}&kind=option&expired=false"
    url_expired = f"{BASE_URL}/public/get_instruments?currency={currency}&kind=option&expired=true"

    instruments = []
    for url in [url_live, url_expired]:
        try:
            res = requests.get(url)
            res.raise_for_status()
            instruments += res.json()['result']
            print(f"[OK] 수집 완료: {url}")
        except Exception as e:
            print(f"[ERROR] 옵션 리스트 수집 실패: {url} | {e}")
    print(f"[INFO] 총 옵션 수집됨: {len(instruments)}개")
    return instruments

# 2. 거래 발생 여부 확인
def get_trades_for_instrument(instrument_name, start_ts, end_ts):
    url = f"{BASE_URL}/public/get_last_trades_by_instrument_and_time"
    params = {
        "instrument_name": instrument_name,
        "start_timestamp": start_ts,
        "end_timestamp": end_ts,
        "count": 1000,
        "include_old": True
    }
    try:
        res = requests.get(url, params=params)
        res.raise_for_status()
        return res.json().get("result", {}).get("trades", [])
    except Exception as e:
        print(f"[ERROR] {instrument_name} | {e}")
        return []

# 3. 최근 N일간 거래 있었던 옵션만 필터링
def filter_traded_instruments(instruments, days_back=30):
    today = datetime.utcnow()
    start = today - timedelta(days=days_back)
    traded = []
    for inst in tqdm(instruments, desc="거래 필터링 중"):
        trades = get_trades_for_instrument(
            inst["instrument_name"],
            int(start.timestamp() * 1000),
            int(today.timestamp() * 1000)
        )
        if trades:
            traded.append(inst)
            # print(f"[KEEP] 거래 감지: {inst['instrument_name']} ({len(trades)}건)")
        # else:
        #     print(f"[SKIP] 거래 없음: {inst['instrument_name']}")
    return traded

# 4. 옵션 하나에 대해 전체 기간 트레이드 수집
def collect_all_trades(instrument, start_date, end_date):
    data = []
    curr_date = start_date
    name = instrument['instrument_name']
    while curr_date < end_date:
        try:
            start_ts = int(curr_date.timestamp() * 1000)
            end_ts = int((curr_date + timedelta(days=1)).timestamp() * 1000)
            trades = get_trades_for_instrument(name, start_ts, end_ts)
            for t in trades:
                data.append({
                    "instrument_name": name,
                    "expiration": datetime.utcfromtimestamp(instrument["expiration_timestamp"] / 1000),
                    "strike": instrument["strike"],
                    "option_type": instrument["option_type"],
                    "price": t["price"],
                    "amount": t["amount"],
                    "direction": t["direction"],
                    "timestamp": datetime.utcfromtimestamp(t["timestamp"] / 1000)
                })
            curr_date += timedelta(days=1)
            time.sleep(0.1)  # API rate 제한 대응
        except Exception as e:
            print(f"[FAIL] {name} @ {curr_date.date()} | {e}")
            curr_date += timedelta(days=1)
    return data

# 5. 단일 옵션을 병렬로 수집하고 파일 저장
def worker_collect_and_save(inst, start_date, end_date, output_dir):
    try:
        trades = collect_all_trades(inst, start_date, end_date)
        if trades:
            df = pd.DataFrame(trades)
            filename = inst["instrument_name"].replace("/", "_")
            df.to_csv(f"{output_dir}/{filename}.csv", index=False)
            print(f"[SAVED] {filename}.csv ({len(df)} rows)")
        else:
            print(f"[SKIP] {inst['instrument_name']} — No trades")
    except Exception as e:
        print(f"[ERROR] {inst['instrument_name']} | {e}")

# 6. 전체 병렬 수집 실행
def run_parallel_collection(start_date="2024-05-01", days_back=30, processes=4, output_dir="deribit_split"):
    os.makedirs(output_dir, exist_ok=True)
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.utcnow()

    print(f"\n[STEP 1] 옵션 리스트 수집")
    instruments = get_all_option_instruments()

    print(f"\n[STEP 2] 최근 {days_back}일 거래된 옵션 필터링")
    traded_instruments = filter_traded_instruments(instruments, days_back=days_back)

    print(f"\n[STEP 3] 병렬 수집 시작 ({len(traded_instruments)}개 옵션 대상, CPU {processes}개 사용)")
    with Pool(processes=processes) as pool:
        job = partial(worker_collect_and_save, start_date=start_date, end_date=end_date, output_dir=output_dir)
        pool.map(job, traded_instruments)

    print(f"\n[COMPLETE] 모든 수집 완료. 저장 경로: ./{output_dir}/")

if __name__ == "__main__":
    run_parallel_collection(
        start_date="2024-05-01",
        days_back=30,
        processes=4,
        output_dir="deribit_split"
    )