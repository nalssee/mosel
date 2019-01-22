from sqlplus import *

# customers.csv
# CustomerID,CustomerName,ContactName,Address,City,PostalCode,Country
# 1,Alfreds Futterkiste,Maria Anders,Obere Str. 57,Berlin,12209,Germany
# 2,Ana Trujillo Emparedados y helados,Ana Trujillo,Avda. de la Constitución 2222,México D.F.,5021,Mexico
# 3,Antonio Moreno Taquería,Antonio Moreno,Mataderos 2312,México D.F.,5023,Mexico
# 4,Around the Horn,Thomas Hardy,120 Hanover Sq.,London,WA1 1DP,UK
# 5,Berglunds snabbköp,Christina Berglund,Berguvsvägen 8,Luleå,S-958 22,Sweden

# orders.csv
# orderid,customerid,employeeid,orderdate,shipperid
# 10248,90,5,1996-07-04,3
# 10249,81,6,1996-07-05,1
# 10250,34,4,1996-07-08,2
# 10251,84,3,1996-07-08,1
# 10252,76,4,1996-07-09,2
# 10253,34,3,1996-07-10,2


def cnt(rs, n):
    # 첫번째 Row 의 month 에 n-1 을 더하면 마지막 월과 같은 경우에만 
    if dmath(rs[0].yyyymm, '%Y-%m', months=n - 1) == rs[-1].yyyymm:
        r = Row()
        r.yyyymm = rs[-1].yyyymm
        r.cnt = len(rs)
        r.n = n
        yield r


def find_best_country(rs):
    # 이보다 훨씬 나은 방법이 있음 
    d = {}
    for r in rs:
        # 대소문자 구별하니까 column 명 만들때 주의 
        d[r.Country] = d.get(r.Country, 0) + 1 
    r = Row()
    r.yyyymm = rs[0].yyyymm 
    r.best_country = max(d, key=d.get)
    yield r 


if __name__ == "__main__":
    drop('orders, customers')
    process(
        # loading 할때 바로 yyyymm 컬럼을 추가할 수도 있음 
        Load('orders.csv', 
             fn={'yyyymm': lambda r: dmath(r.orderdate, '%Y-%m-%d', '%Y-%m')}),
        Load('customers.csv'),

        # orders 와 customers 를 customerid 로 join 하기 
        Join(
            ['orders', '*', 'customerid'],
            ['customers', 'customername, country', 'CustomerID'],
            name="orders1"
        ),

        # 과거 3개월 또는 6개월간의 주문의 개수 구하기 
    
        # 들어가는 테이블과 나오는 테이블명이 같으면 여러개의 cpu 를 동시에 사용  
        Map(cnt, 'orders1', group='yyyymm', overlap=3, arg=3, name='order_cnt'),
        # overlap=3 은 overlap(3, 1) 과 동일, 1 은 jump 개수 
        # Map 에 쓰이는 함수는 항상 Row 또는 Rows 를 인자로 받지만 추가적인인자가 필요한 경우 arg 를 
        # 이용할 수 있음 
        Map(cnt, 'orders1', group='yyyymm', overlap=6, arg=6, name='order_cnt'),

        # Union 은 동일한 컬럼을 가진 여러개의 table 을 합할때 아래와 같이 사용
        # Union('table1, table2', name='table3')

        # 매월 가장 많은 주문을 넣는 나라 표시하기 
        Map(find_best_country, 'orders1', group='yyyymm', name='best_country')
         
          
    )