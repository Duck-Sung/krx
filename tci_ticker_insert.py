from  pykrx import stock
import cx_Oracle
import time

dsn_tns = cx_Oracle.makedsn('203.253.20.160', 1521, service_name='DLIDB3')
conn = cx_Oracle.connect(user='SSU_DOO', password='tndtlf!004', dsn=dsn_tns)

# 데이터베이스 연결
cursor = conn.cursor()
#입력쿼리
query = "INSERT INTO tci_ticker (tci_id, t_id) VALUES (:1, :2)"
# index 조회
cursor.execute("select TCI_ID from TC_INDEX")

for row in cursor.fetchall():
    pdf = stock.get_index_portfolio_deposit_file(row[0])
    time.sleep(1)
    for tc in pdf:
        t_in_data = (row[0],tc)
        print(t_in_data)
        cursor.execute(query, t_in_data)
  


# 변경 사항 커밋
conn.commit()  


# 입력 데이터 조회 
cursor.execute("SELECT * FROM tci_ticker")
for row in cursor.fetchall():
    print(row)

# 연결 종료
cursor.close()
conn.close()
