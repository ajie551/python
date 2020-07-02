# 检测活跃事务id,如有事务持续5s以上未结束.则发送警报
import MySQLdb, json, urllib.request
import time


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
    while count < 3:
        db = MySQLdb.connect("rm-2zez7u673640b68x1.mysql.rds.aliyuncs.com", "zhwdb", "3NPjzZ14YBGnCo0d",
                             "information_schema",
                             charset='utf8')
        cursor = db.cursor()
        cursor.execute("select trx_id from  INNODB_TRX;")
        trx_id = cursor.fetchall()
        j = set()
        if len(trx_id) == 0:  # 判断事务id是否为空,为空则中断进入下一次循环
            count = 0
            time.sleep(2)
        else:
            for i in trx_id:  # 不为空将事务id放入一个集合中,首次集合与自身相交,二次与新集合相交,取出持续存在的事务id
                j.add(i[0])
                s = j
            j = j & s
            count += 1
            print(count)
            time.sleep(2)
    if len(j) > 0:  # 5s内交集集合长度大于0时,发送警告信息
        send_dingding('事务持续5s未结束,id为{}'.format(j))
        time.sleep(10)
    else:
        db.close()
