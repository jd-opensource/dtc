import pytest
import pymysql

def test_insert_with_single_quotes():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(1, 'hello')"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    print("affected rows: %s" % (rowsaffected))
    cursor.close()
    db.close()
    assert rowsaffected == 1

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
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1

    #sql = "update opensource set name = \"Lee3\" where uid = 1"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1

    sql = "update opensource set name = `Lee` where uid = 1"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 0

    #sql = "update opensource set name = \"Lee4\" where uid = '1'"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1    

    #sql = "update opensource set name = \"Lee\" where uid = \"1\""
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1        

    #sql = "update opensource set name = 'Lee2' where `uid` = 1"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1

    #sql = "update opensource set name = \"Lee\" where `uid` = 1"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1    

    #sql = "update opensource set `name` = \"Lee2\" where `uid` = 1"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1        

    #sql = "update opensource set `name` = \"Lee\" where `uid` = gh"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 0

    #sql = "update opensource set `name` = Lee2 where `uid` = 1"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 0

    db.commit()
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

def test_insert_remove_where_cluster():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(1, 'hello')"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    print("affected rows: %s" % (rowsaffected))
    cursor.close()
    db.close()
    assert rowsaffected == 1

'''
def test_insert_remove_where_cluster_without_specify_key():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource values(1, 'Jack', 'Shanghai', 1, 18)"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    print("affected rows: %s" % (rowsaffected))    
    cursor.close()
    db.close()
    assert rowsaffected == 1
'''

def test_select_limit():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()

    sql = "insert into opensource(uid,name,city,sex,age) values(1, 'Jack', 'Shanghai', 1, 18)"
    cursor.execute(sql)
    db.commit()

    sql = "select uid, name from opensource where uid = 1 limit 2"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 2
   
    cursor = db.cursor()
    sql = "select uid, name from opensource where uid = 1 limit 1"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 1

    cursor = db.cursor()
    sql = "insert into opensource(uid,name,city,sex,age) values(1, 'Jack', 'Shanghai', 1, 19)"
    cursor.execute(sql)
    db.commit()

    cursor = db.cursor()
    sql = "select uid, name,city,sex,age from opensource where uid = 1 limit 1,3"
    cursor.execute(sql)
    results = cursor.fetchall()
    assert len(results) == 2
    assert results[0][4] == 18
    assert results[1][4] == 19

    db.close()

def test_insert_automated_conversion():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()

    #name num to string
    sql = "insert into opensource(uid,name,city,sex,age) values(1, 123, 'Shanghai', 1, 18)"
    insert = cursor.execute(sql)
    assert insert == 1

    #name num to string
    sql = "insert into opensource(uid,name,city,sex,age) values(1, 123a, 'Shanghai', 1, 18)"
    insert = cursor.execute(sql)
    assert insert == 0    

    #name num(float) to string
    sql = "insert into opensource(uid,name,city,sex,age) values(1, 123.3, 'Shanghai', 1, 18)"
    insert = cursor.execute(sql)
    assert insert == 1

    #name string to num
    sql = "insert into opensource(uid,name,city,sex,age) values(1, 'jack', 'Shanghai', 1, '18')"
    insert = cursor.execute(sql)
    assert insert == 1

    #name string to num, error
    sql = "insert into opensource(uid,name,city,sex,age) values(1, 'jack', 'Shanghai', 1, '18a')"
    insert = cursor.execute(sql)
    assert insert == 0

    db.commit()

    db.close()

def test_insert_with_double_quotes():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(33, \"hello\")"
    cursor.execute(sql)
    db.commit()
    rowsaffected = cursor.rowcount
    cursor.close()
    db.close()    
    assert rowsaffected == 1

def test_insert_with_double_quotes():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    sql = "insert into opensource(uid, name) values(33, \"hello\")"
    rowsaffected = cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()    
    assert rowsaffected == 1

def test_insert_with_grave():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()

    sql = "insert into `opensource`(uid, name) values(33, 'hello')"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1

    #sql = "insert into opensource(`uid`, name) values(33, 'hello')"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1

    #sql = "insert into opensource(`uid`, `name`) values(33, 'hello')"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1

    #sql = "insert into `opensource`(`uid`, `name`) values(33, 'hello')"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1    

    sql = "insert into opensource(uid, name) values(33, `hello`)"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 0

    sql = "insert into opensource(uid, name) values(33, `123`)"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 0    

    #sql = "insert into \"opensource\"(uid, name) values(33, 'hello')"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1

    #sql = "insert into 'opensource'(uid, name) values(33, 'hello')"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1    

    db.commit()
    cursor.close()
    db.close()    

def test_insert_with_set_keyword():
    db = pymysql.connect(host='127.0.0.1', port=20015, user='test', password='test', database='test')
    cursor = db.cursor()
    
    sql = "insert into opensource set uid = 33, name = 'hello'"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1   

    #sql = "insert into opensource set `uid` = 33, `name` = 'hello'"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1   

    #sql = "insert into opensource set `uid` = 33, `name` = '12312'"
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1       

    #sql = "insert into opensource set `uid` = 33, `name` = \"12312\""
    #rowsaffected = cursor.execute(sql)
    #assert rowsaffected == 1           

    sql = "insert into opensource set uid = 33, name = 'hello'"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1   

    sql = "insert into opensource set uid = 33, name = '12312'"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1       

    sql = "insert into opensource set uid = 33, name = \"12312waefoioiwa\""
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1       

    sql = "insert into opensource set uid = 33, name = \"waefoioiwawaefwa\""
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1       

    sql = "insert into opensource set uid = 33, name = 123waefoioiwawaefwa"
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 0

    sql = "insert into opensource set uid = 33, name = \"12312\""
    rowsaffected = cursor.execute(sql)
    assert rowsaffected == 1           

    db.commit()
    cursor.close()
    db.close()    
 