from  pykrx import stock
import cx_Oracle
import time
import datetime
import numpy as np
import pandas as pd

# 데이터베이스 연결
dsn_tns = cx_Oracle.makedsn('203.253.20.160', 1521, service_name='DLIDB3')
conn = cx_Oracle.connect(user='SSU_DOO', password='tndtlf!004', dsn=dsn_tns)
cursor = conn.cursor()

#입력쿼리
query_insert_tc_investor = "INSERT INTO tc_investor (TC_DATE, T_ID, TRUST, FUND, ORGAN, FOREIGNER, INDI) VALUES ( :1,:2,:3,:4,:5,:6,:7)"
query_insert_tc_ohlcv = "INSERT INTO tc_ohlcv (TC_DATE, T_ID, OPEN, HIGH, CLOSE, VOLUME, PRICE_TRANS, PCT_UPDOWN) VALUES ( :1,:2,:3,:4,:5,:6,:7,:8)"

#입력대상 날짜 범위 확인
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
 #금일 16이 이후면 체크하는 어제날짜가 오늘로 변경됨
if datetime.datetime.now().hour > 16:
    yesterday = today
yesterday_str = yesterday.strftime('%Y%m%d')
# 최종입력일 조회
cursor.execute("select TO_CHAR(max(TC_DATE),'YYYYMMDD') from tc_investor")

for row in cursor.fetchall():
    last_day_str = row[0]

last_day_date = datetime.datetime.strptime(last_day_str, "%Y%m%d")
last_day_date = last_day_date + datetime.timedelta(days=1) #최종입력일 + 1일
last_day_str  = last_day_date.strftime('%Y%m%d')
yesterday_date = datetime.datetime.strptime(yesterday_str, "%Y%m%d")
diff = yesterday_date - last_day_date
int_diff_days = diff.days

## 강제 배정시 기간 설정
last_day_str  = "20030101"
yesterday_str = "20120131"
int_diff_days = 0

total_insert_count = 0
#최종입력일 어제 보다 이전일 때만 입력
if int_diff_days >= 0:
    print("입력대상존재함!!!")
    cursor.execute("SELECT t.T_ID,t.market from TICKER t WHERE  1=1 and not exists (select * from TC_OHLCV tc where tc.T_ID=t.t_id and to_char(tc.tc_date,'YYYYMMDD') BETWEEN :1 and :2)",(last_day_str,yesterday_str))
    for row in cursor.fetchall():
        print(row[0],row[1])
        #01. ohlcv 입력
        df = pd.DataFrame()
        if row[1] == 'KOSPI' or row[1] == 'KOSDAQ':
            df = stock.get_market_ohlcv(last_day_str,yesterday_str,row[0],"d")
        else:
            df = stock.get_etf_ohlcv_by_date(last_day_str,yesterday_str,row[0],"d")
        df.fillna(0, inplace=True) #nan값 0으로 전체 변경
        time.sleep(0.2)
        for idx, df_row in df.iterrows():
            t_in_data = ()
            try:
                if row[1] == 'KOSPI' or row[1] == 'KOSDAQ':
                    t_in_data = (idx,row[0],df_row['시가'],df_row['고가'],df_row['저가'],df_row['종가'],df_row['거래량'],df_row['등락률'])
                else: #ETF
                    t_in_data = (idx,row[0],df_row['시가'],df_row['고가'],df_row['저가'],df_row['종가'],df_row['거래량'],df_row['기초지수'])
                insert_day_str  = idx.strftime('%Y%m%d')
                cursor.execute("select count(*) from tc_ohlcv where  to_char(TC_DATE,'YYYYMMDD') = :1 and T_ID=:2",(insert_day_str,row[0]))
                row_count = cursor.fetchone()
                #print(insert_day_str,row[0],row_count)
                if row_count[0] < 1:
                    cursor.execute(query_insert_tc_ohlcv, t_in_data)
                    conn.commit() 
                else:
                    print("Tc_ohlcv Duplicated!!!")
            except cx_Oracle.Error as error:
                print(f"Oracle error: {error}")
                conn.rollback()
                break

        #02. investor 입력
        df = pd.DataFrame()
        if row[1] == 'KOSPI' or row[1] == 'KOSDAQ':
            df = stock.get_market_trading_volume_by_date(last_day_str,yesterday_str,row[0],detail=True)
            df.fillna(0, inplace=True) #nan값 0으로 전체 변경
            time.sleep(0.2)

            for idx, df_row in df.iterrows():
                t_in_data = ()
                try:
                    total_organ = df_row['금융투자'] + df_row['보험'] + df_row['투신'] + df_row['사모'] + df_row['은행'] + df_row['기타금융'] + df_row['연기금']
                    t_in_data = (idx,row[0],int(df_row['투신']),int(df_row['연기금']),int(total_organ),int(df_row['외국인']),int(df_row['개인']))

                    insert_day_str  = idx.strftime('%Y%m%d')
                    cursor.execute("select count(*) from tc_investor where  to_char(TC_DATE,'YYYYMMDD') = :1 and T_ID=:2",(insert_day_str,row[0]))
                    row_count = cursor.fetchone()
                    #print(insert_day_str,row[0],row_count)
                    if row_count[0] < 1:
                        cursor.execute(query_insert_tc_investor, t_in_data)
                        conn.commit() 
                    else:
                        print("Tc_investor Duplicated!!!")                    
                except cx_Oracle.Error as error:
                    print(f"Oracle error: {error}")
                    conn.rollback()
                    break
        total_insert_count = total_insert_count + 1 #입력대상 개수
        print(total_insert_count)
    ##end if

# 입력 데이터 확인
print("입력 ticker 수:" + str(total_insert_count) + "입니다!")

# 연결 종료
cursor.close()
conn.close()
