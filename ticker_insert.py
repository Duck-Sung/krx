from  pykrx import stock
import datetime
import cx_Oracle

'''
# Access DB 연결 문자열 예시 (절대 경로 사용)
import pyodbc
conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    r"DBQ=D:\py_project\ssu_doo.accdb;"
)
##conn = pyodbc.connect(conn_str)
'''
dsn_tns = cx_Oracle.makedsn('203.253.20.160', 1521, service_name='DLIDB3')
conn = cx_Oracle.connect(user='SSU_DOO', password='tndtlf!004', dsn=dsn_tns)

# 데이터베이스 연결

cursor = conn.cursor()
query = "INSERT INTO ticker (t_id, t_name, market) VALUES (:1, :2, :3)"


# ticker 조회
today = datetime.date.today()
today_str = today.strftime('%Y%m%d')
#tickers = stock.get_market_ticker_list(today_str, market="KOSDAQ") #KOSDAQ,KOSPI
tickers =stock.get_etf_ticker_list(today_str) #etf용

# 데이터 입력 예제
for ticker in tickers:
    #t_in_data = (ticker,stock.get_market_ticker_name(ticker),'KOSDAQ')
    t_in_data = (ticker,stock.get_etf_ticker_name(ticker),'ETF')
    cursor.execute(query, t_in_data)
    #print(t_in_data)

# 변경 사항 커밋
conn.commit()  


# 입력 데이터 조회 
cursor.execute("SELECT * FROM ticker")

for row in cursor.fetchall():
    print(row)

# 연결 종료
cursor.close()
conn.close()
