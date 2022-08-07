import pytest
import pymysql

def test_select():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    for row in results:
        uid = row[0]
        name = row[1]
        print("uid=%s, name=%s" % (uid, name))
        assert uid == 1
        assert name == "hello"
    db.close()
