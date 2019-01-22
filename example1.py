from sqlplus import *

# orders.csv 파일은 대략 이렇게 생겼다 
# orderid,customerid,employeeid,orderdate,shipperid
# 10248,90,5,1996-07-04,3
# 10249,81,6,1996-07-05,1
# 10250,34,4,1996-07-08,2
# 10251,84,3,1996-07-08,1

def cnt(rs):
    r = rs[0]
    r.cnt = len(rs)
    yield r


def orders_avg_nmonth(r):
    r0 = r.copy()
    r0.nmonth = 3
    try:
        r0.avg = round((r.cnt + r.cnt1 + r.cnt2) / 3, 1)
    except:
        r0.avg = ''
    yield r0 

    r0 = r.copy()
    r0.nmonth = 6
    try:
        r0.avg = round((r.cnt + r.cnt1 + r.cnt2 + r.cnt3 + r.cnt4 + r.cnt5) / 6, 1)
    except:
        r0.avg = ''
    yield r0 



def addmonth(date, n):
    return dmath(date, '%Y-%m', months=n)


# Map(함수 또는 dictionary, 테이블명, ...)
if __name__ == "__main__":
    # orders 라는 테이블이 있으면 지우고 시작 
    drop('orders')
    process(
        # orders.csv 파일을 로드 
        Load('orders.csv'),
        # Load('oders.csv', name='orders') 와 같음  

        # orderdate 컬럼의 1996-07-04 를 1997-07 과 같이 
        # 바꾸고 컬럼명을 yyyymm 으로 
        Map({'yyyymm': lambda r: dmath(r.orderdate, '%Y-%m-%d', '%Y-%m')},
            'orders', name='orders1'),
        
        # 월별 주문 개수를 세기 
        # group 별로 계산할때는 위와 같이 간단한 dictionary 형태로는 안됨. 
        # dictionary 는 Row 에 컬럼을 추가하거나 수정하는 역할인데, group 인 경우,
        # 어떤 줄에 작업을 하는지가 알수가 없음
        Map(cnt, 'orders1', group='yyyymm', name='orders_cnt'),

        # 과거 3개월 또는 6개월의 평균 주문 개수를 계산 
        Join(
            ['orders_cnt', '*', 'yyyymm'],
            # tuple 로 맞춰줌 
            ['orders_cnt', 'cnt as cnt1', lambda r: (addmonth(r.yyyymm, 1),)],
            ['orders_cnt', 'cnt as cnt2', lambda r: (addmonth(r.yyyymm, 2),)],
            ['orders_cnt', 'cnt as cnt3', lambda r: (addmonth(r.yyyymm, 3),)],
            ['orders_cnt', 'cnt as cnt4', lambda r: (addmonth(r.yyyymm, 4),)],
            ['orders_cnt', 'cnt as cnt5', lambda r: (addmonth(r.yyyymm, 5),)],
            name='orders_cnt2'
        ),
        # 함수명과 테이블명이 같을 필요는 없음   
        Map(orders_avg_nmonth, 'orders_cnt2', name='orders_avg_nmonth')

    )

