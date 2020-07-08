import json
import time
import urllib.request
from collections import Counter  # 引入Counter

import MySQLdb


def send_dingding(data_Content):
    # 1、构建url
    url = "https://oapi.dingtalk.com/robot/send?access_token=3d193af514e746a68b9b9e4b4cf1435a9dc125ff20fd792c8539d3baddb24476"  # url为机器人的webhook
    
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
                "18310699089",
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
    # print(opener.read())


while True:
    count = 0
    j = []
    while count < 3:
        db = MySQLdb.connect("xxxxxxxxxxx", "xxxxxxxxxxx", "xxxxxxxxxxx",
                             "information_schema",
                             charset='utf8')
        cursor = db.cursor()
        cursor.execute("select trx_id from  INNODB_TRX;")
        trx_id = cursor.fetchall()
        if len(trx_id) == 0:  
            count = 0
            j = []
            time.sleep(10)
        else:
            for i in trx_id:  
                j.append(i[0])
            count+=1
            time.sleep(10)
        b = dict(Counter(j))
        s = [key for key, value in b.items() if value > 2]
    if len(s) > 0:  
        send_dingding('事务持续20s以上未结束,id为{}'.format(s))
        time.sleep(2)
    else:
        db.close()
