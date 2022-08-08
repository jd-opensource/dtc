import pytest
import pymysql

def test_insert_with_single_quotes():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(1, 'hello') where uid = 1"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    print("affected rows: %s" % (rowsaffected))
    cursor.close()
    db.close()
    assert rowsaffected == 1

'''
def test_insert_with_double_quotes():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(1, \"hello\") where uid = 1"
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()
'''    

'''
def test_insert_remove_where_cluster():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(1, \"hello\")"
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()
'''

'''
def test_insert_remove_where_cluster_without_specify_key():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource values(1, \"Jack\", \"Shanghai\", 1, 18)"
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()
'''

def test_select():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 1
    for row in results:
        uid = row[0]
        name = row[1]
        print("uid=%s, name=%s" % (uid, name))
        assert uid == 1
        assert name == "hello"
    db.close()

def test_update():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 1
    for row in results:
        uid = row[0]
        name = row[1]
        print("uid=%s, name=%s" % (uid, name))
        assert uid == 1
        assert name == "hello"
    cursor.close()

    cursor = db.cursor()
    sql = "update opensource set name = 'Lee' where uid = 1"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    print("affected rows: %s" % (rowsaffected))
    assert rowsaffected == 1
    cursor.close()

    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 1
    for row in results:
        uid = row[0]
        name = row[1]
        print("uid=%s, name=%s" % (uid, name))
        assert uid == 1
        assert name == "Lee"
    cursor.close()

    db.close()

def test_delete():
    print("----delete----")
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 1
    cursor.close()

    cursor = db.cursor()
    sql = "delete from opensource where uid = 1"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    print("affected rows: %s" % (rowsaffected))
    assert rowsaffected == 1
    cursor.close()

    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 0
    cursor.close()

    db.close()    

'''
def test_check_tablename():
'''    
