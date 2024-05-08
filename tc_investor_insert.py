from  pykrx import stock
import cx_Oracle
import time
import datetime
import numpy as np

dsn_tns = cx_Oracle.makedsn('203.253.20.160', 1521, service_name='DLIDB3')
conn = cx_Oracle.connect(user='SSU_DOO', password='tndtlf!004', dsn=dsn_tns)

# 데이터베이스 연결
cursor = conn.cursor()
#입력쿼리
query = "INSERT INTO tc_investor (TC_DATE, T_ID, TRUST, FUND, ORGAN, FOREIGNER, INDI) VALUES ( :1,:2,:3,:4,:5,:6,:7)"

today = datetime.date.today()
today_str = today.strftime('%Y%m%d')
yesterday = today - datetime.timedelta(days=1)
if datetime.datetime.now().hour > 16: #금일 16이 이후면 체크하는 어제날짜가 오늘로 변경됨
    yesterday = today

yesterday_str = yesterday.strftime('%Y%m%d')

# 최종입력일 조회
cursor.execute("select TO_CHAR(max(TC_DATE),'YYYYMMDD') from tc_investor")
for row in cursor.fetchall():
    last_day_str = row[0]
    last_day_str = "20240424"

last_day_date = datetime.datetime.strptime(last_day_str, "%Y%m%d")
last_day_date = last_day_date + datetime.timedelta(days=1) #최종입력일 + 1일
last_day_str  = last_day_date.strftime('%Y%m%d')
yesterday_date = datetime.datetime.strptime(yesterday_str, "%Y%m%d")
diff = yesterday_date - last_day_date
int_diff_days = diff.days
last_day_str  = "20240426"
yesterday_str = "20240430"
int_diff_days = 0


#최종입력일 어제 보다 이전일 때만 입력
if int_diff_days >= 0:
    #cursor.execute("select T_ID from TICKER WHERE  market in ('KOSPI','KOSDAQ') and T_ID='005930'")
    cursor.execute("SELECT t.T_ID from TICKER t WHERE  t.market in ('KOSPI','KOSDAQ') and not exists (select * from TC_INVESTOR tc where tc.T_ID=t.t_id and to_char(tc.tc_date,'YYYYMMDD') BETWEEN :1 and :2)",(last_day_str,yesterday_str))

    for row in cursor.fetchall():
        print("ticker:",row[0])
        df = stock.get_market_trading_volume_by_date(last_day_str,yesterday_str,row[0],detail=True)
        df.fillna(0, inplace=True) #nan값 0으로 전체 변경
        time.sleep(0.2)
        #print(df)
        for idx, df_row in df.iterrows():
            
            total_organ = df_row['금융투자'] + df_row['보험'] + df_row['투신'] + df_row['사모'] + df_row['은행'] + df_row['기타금융'] + df_row['연기금']
            t_in_data = (idx,row[0],int(df_row['투신']),int(df_row['연기금']),int(total_organ),int(df_row['외국인']),int(df_row['개인']))
            #print(t_in_data)
            try:
                cursor.execute(query, t_in_data)
            except cx_Oracle.Error as error:
                print(f"Oracle error: {error}")
            conn.commit() 
            
    ##end if

  
# 변경 사항 커밋
conn.commit()  


# 입력 데이터 조회 
print("입력데이터 조회")
#cursor.execute("select * from tc_ohlcv")
#for row in cursor.fetchall():
    #print(row)

# 연결 종료
cursor.close()
conn.close()
