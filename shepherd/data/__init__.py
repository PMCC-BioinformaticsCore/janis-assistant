import os
#
# from shepherd.management import get_default_export_dir
#
#
# def get_task_path(tid):
#     if not isinstance(tid, str): raise Exception(f"Expected taskid '{tid}' to be a a string")
#
#     path = get_default_export_dir() + "/" + tid +"/"
#     if not os.path.exists(path):
#         os.makedirs(path)
#     return path
#
#
# def get_sql_path(tid):
#     return get_task_path(tid) + "janis.db"
#
#
# def db_connection(tid):
#     import sqlite3
#     path = get_sql_path(tid)
#     return sqlite3.connect(path)
#
# if __name__ == "__main__":
#     con = db_connection("test-1")
#     c = con.cursor()
#     # c.execute('''CREATE TABLE stocks
#     #          (date text, trans text, symbol text, qty real, price real)''')
#     c.execute(''' INSERT INTO stocks VALUES ('2019', 'BUY', 'product', 1, 9.99)''')
#     c.execute('SELECT * FROM stocks')
#     print(c.fetchall())
#     con.commit()