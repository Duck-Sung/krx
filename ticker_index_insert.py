from  pykrx import stock
import datetime
import cx_Oracle
import time

dsn_tns = cx_Oracle.makedsn('203.253.20.160', 1521, service_name='DLIDB3')
conn = cx_Oracle.connect(user='SSU_DOO', password='tndtlf!004', dsn=dsn_tns)

# 데이터베이스 연결

cursor = conn.cursor()
query = "INSERT INTO TC_INDEX (tci_id, tci_name, market) VALUES (:1, :2, :3)"


# ticker 조회
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
yesterday_str = yesterday.strftime('%Y%m%d')
#tickers = stock.get_market_ticker_list(today_str, market="KOSDAQ") #KOSDAQ,KOSPI
#tickers =stock.get_etf_ticker_list(today_str) #etf용
#KOSPI
tickers = stock.get_index_ticker_list(today_str, market="KOSPI") #KOSDAQ,KOSPI,KRX,테마                                   
for ticker in tickers:
    #t_in_data = (ticker,stock.get_market_ticker_name(ticker),'KOSDAQ')
    #t_in_data = (ticker,stock.get_etf_ticker_name(ticker),'ETF')
    t_in_data = (ticker,stock.get_index_ticker_name(ticker),'KOSPI')
    cursor.execute(query, t_in_data)
    #print(t_in_data)

time.sleep(1)

#KOSDAQ
tickers = stock.get_index_ticker_list(today_str, market="KOSDAQ") #KOSDAQ,KOSPI,KRX,테마                                   
for ticker in tickers:
    t_in_data = (ticker,stock.get_index_ticker_name(ticker),'KOSDAQ')
    cursor.execute(query, t_in_data)

time.sleep(1)

#KRX
tickers = stock.get_index_ticker_list(today_str, market="KRX") #KOSDAQ,KOSPI,KRX,테마                                   
for ticker in tickers:
    t_in_data = (ticker,stock.get_index_ticker_name(ticker),'KRX')
    cursor.execute(query, t_in_data)

time.sleep(1)

#테마
tickers = stock.get_index_ticker_list(today_str, market="테마") #KOSDAQ,KOSPI,KRX,테마                                   
for ticker in tickers:
    t_in_data = (ticker,stock.get_index_ticker_name(ticker),'테마')
    cursor.execute(query, t_in_data)

time.sleep(1)

# 변경 사항 커밋
conn.commit()  


# 입력 데이터 조회 
cursor.execute("SELECT * FROM tc_index")

for row in cursor.fetchall():
    print(row)

# 연결 종료
cursor.close()
conn.close()
