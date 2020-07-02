import psycopg2
import datetime
import json
import urllib.request


def get_1st_of_next_month(i):
    """
    获取下i个月的1号的日期
    :return: 返回日期
    """
    today = datetime.datetime.today()
    year = today.year
    month = today.month
    if month == 12:
        month = i
        year += 1
    else:
        month += i
    res = datetime.datetime(year, month, 1)
    return res


def send_dingding(data_Content):
    # 1、构建url
    url = "xxx"  # url为机器人的webhook
    
    # 2、构建一下请求头部
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    # 3、构建请求数据
    data = {
        "msgtype": "text",
        "text": {
            "content": data_Content
        },
        "at": {
            "atMobiles": [
                "xxxx",
            ],
            "isAtAll": False  # @全体成员（在此可设置@特定某人）
        }
    }
    
    # 4、对请求的数据进行json封装
    sendData = json.dumps(data)  # 将字典类型数据转化为json格式
    sendData = sendData.encode("utf-8")  # python3的Request要求data为byte类型
    
    # 5、发送请求
    request = urllib.request.Request(url=url, data=sendData, headers=header)
    #
    # 6、将请求发回的数据构建成为文件格式

    opener = urllib.request.urlopen(request)
    # 7、打印返回的结果
    #print(opener.read())


table_name = 'xxx'
table_date = datetime.date.today() + datetime.timedelta(days=20)
table_name = (table_name + str(table_date)[0:7])

column_start = get_1st_of_next_month(1)
column_end = get_1st_of_next_month(2)

try:
    
    connect = psycopg2.connect(host='xxxx',
                               database='xxx',
                               user='xxx',
                               password='xxxx',
                               port='5432')
    cur = connect.cursor()
    cur.execute(
        """CREATE TABLE {} PARTITION OF measurement FOR VALUES FROM ('{}') TO ('{}');""".format(table_name,
                                                                                                column_start,
                                                                                                column_end))
    cur.close()
    connect.close()
except Exception:
    send_dingding('postgresql自动创建分区出错!!!')
