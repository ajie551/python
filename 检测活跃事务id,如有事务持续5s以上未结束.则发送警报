#检测活跃事务id,如有事务持续5s以上未结束.则发送警报
import MySQLdb, json, urllib.request
import time


def send_dingding(data_Content):
    # 1、构建url
    url = "xxxxxxx"  # url为机器人的webhook

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
                "xxxxxx手机号",
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
        db = MySQLdb.connect("xxxxxx", "xxxxx", "xxxxxxx",
                             "information_schema",
                             charset='utf8')
        cursor = db.cursor()
        cursor.execute("select trx_id from  INNODB_TRX;")
        trx_id = cursor.fetchall()
        j= set()#定义空集合存储5s内事务id的交集
        print(trx_id)
        if len(trx_id) == 0:#判断事务id是否为空,为空则中断进入下一次循环
            time.sleep(2)
            continue
        else:
            for i in trx_id:#不为空将事务id放入一个集合中,首次集合与自身相交,二次与新集合相交,取出持续存在的事务id
                j.add(i[0])
            j = j & j
            count += 1
            time.sleep(2)
    if len(j) > 0:#5s内交集集合长度大于0时,发送警告信息
        send_dingding('事务持续5s未结束,id为{}'.format(j))
        time.sleep(10)
    else:
        db.close()
