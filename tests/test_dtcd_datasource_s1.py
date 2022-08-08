import pytest
import pymysql

def test_demo():
    assert 100 == 100

def test_insert_and_result():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')

    #insert to DTC
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(1, 'hello') where uid = 1"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    print("affected rows: %s" % (rowsaffected))
    assert rowsaffected == 1
    cursor.close()

    #select from DTC
    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    dtclen = len(results)
    assert dtclen == 1
    dtcuid = results[0][0]
    dtcname = results[0][1]
    assert dtcuid == 1
    assert dtcname == "hello"
    cursor.close()

    db.close()

    #select from datasource
    db = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456', database='layer2')
    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    dblen = len(results)
    assert dblen == dtclen
    dbuid = results[0][0]
    dbname = results[0][1]
    assert dtcuid == dbuid
    assert dtcname == dbname
    cursor.close()

    db.close()