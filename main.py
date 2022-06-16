import mysql.connector
from decorators import once

 
@once
def connect_to_db(conn=None) -> mysql.connector.connection_cext.CMySQLConnection:
    """Функция подключения к БД"""
    print('Подключение к БД')
    try:
        conn = mysql.connector.connect(user='root',
                                       password='*',
                                       host='127.0.0.1',
                                       database='department_of_information_technology_and_learning')
    except mysql.connector.DatabaseError:
        print(f'Не удалось подключиться к БД')
    return conn
 
db_handler = connect_to_db()


def query_sql(conn: mysql.connector.connection_cext.CMySQLConnection, statement: str, query: list):

    if statement == "SELECT":
        for q in query:
            if len(q) == 2:
                table = q[0]
                columns = q[1]
                select_sql(db_handler, table, columns)
            elif len(q) == 1:
                table = q[0]
                select_sql(db_handler, table)

    elif statement == "INSERT":
        for q in query:
            table = q[0]
            values = q[1]
            insert_sql(db_handler, table, values)

    elif statement == "UPDATE":
        for q in query:
            if len(q) == 3:
                table = q[0]
                values = q[1]
                condition = q[2]
                update_sql(db_handler, table, values, condition)
            elif len(q) == 2:
                table = q[0]
                values = q[1]
                update_sql(db_handler, table, values)
              
    elif statement == "DELETE":
        for q in query:
            if len(q) == 2:
                table = q[0]
                condition = q[1]
                delete_sql(db_handler, table, condition)
            elif len(q) == 1:
                table = q[0]
                delete_sql(db_handler, table)


def select_sql(conn: mysql.connector.connection_cext.CMySQLConnection, table, columns="*"):
    cursor = conn.cursor()
    select_query = f"SELECT {columns} FROM {table}"
    print("\n" + select_query)
    select_res = cursor.execute(select_query)
    for i in cursor:
        print(i)


def insert_sql(conn: mysql.connector.connection_cext.CMySQLConnection, table: str, *values):
    cursor = conn.cursor()
    insert_query = f"INSERT INTO {table} VALUES ("
    for i in range(len(values[0])):
        insert_query += '%s,'
    insert_query = insert_query[:-1] + ')'
    print("\n" + insert_query)
    cursor.executemany(insert_query, values)


def delete_sql(conn: mysql.connector.connection_cext.CMySQLConnection, table: str, condition: str = None):
    cursor = conn.cursor()
    if condition:
        delete_query = f"DELETE FROM {table} WHERE {condition}"
        print("\n" + delete_query)
        cursor.execute(delete_query)
    else:
        delete_query = f"DELETE FROM {table}"
        print("\n" + delete_query)
        cursor.execute(f"DELETE FROM {table}")


def update_sql(conn: mysql.connector.connection_cext.CMySQLConnection,
               table: str,
               value,
               condition: str = None):
    cursor = conn.cursor()
    if condition:
        update_query = f"UPDATE {table} SET {value} WHERE {condition}"
        print("\n" + update_query)
        cursor.execute(update_query)
    else:
        update_query = f"UPDATE {table} SET {value}"
        print("\n" + update_query)
        cursor.execute(update_query)


query_sql(db_handler, "SELECT", [("students", ), ("students", "id_student")])
query_sql(db_handler, "INSERT",
          [("students", ('4', 'Просвирнина Жанна Владимировна', 1, 1, 89167564433, 'janjac@mail.ru')),
           ("students", ('5', 'Тверской Дмитрий Романович', 2, 2, 89175349022, 'TDR@yandex.ru'))])
query_sql(db_handler, "SELECT", [("students", )])
query_sql(db_handler, "DELETE", [("students", "id_student = 4")])
query_sql(db_handler, "SELECT", [("students", )])
query_sql(db_handler, "UPDATE", [("students","id_student = 4", "id_student = 5")])
query_sql(db_handler, "SELECT", [("students", )])

query_sql(db_handler, "SELECT", [("student_groups", ), ("student_groups", "id_group")])
query_sql(db_handler, "INSERT",
          [("student_groups", ('4', 1, 1, 1, "ИСиТ", 2021, 2025)),
           ("student_groups", ('5', 2, 0, 1, "ИСиТ", 2021, 2025))])
query_sql(db_handler, "SELECT", [("student_groups", )])
query_sql(db_handler, "DELETE", [("student_groups", "id_group = 4")])
query_sql(db_handler, "SELECT", [("student_groups", )])
query_sql(db_handler, "UPDATE", [("student_groups","id_group = 4", "id_group = 5")])
query_sql(db_handler, "SELECT", [("student_groups", )])
