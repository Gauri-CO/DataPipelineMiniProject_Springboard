import mysql.connector
from mysql.connector import errorcode
import csv


def get_db_connection():
    cnx = None
    try:
        cnx = mysql.connector.connect(user='root', password='Eshaan@2021', host='127.0.0.1', database='dev_schema')

    except mysql.conector.Error as err:
        print(err)

    return cnx


def load_third_party(connection):
    cursor = connection.cursor()
    csv_data = csv.reader(
        open(filepath))
    next(csv_data)

    for row in csv_data:
        cursor.execute(
            'insert into ticket_sales(ticket_id,trans_date,event_id,event_name,event_date,event_type,event_city,customer_id,price,num_tickets) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            row)

    connection.commit()
    cursor.close()
    print("Completed")


def query_popular_tickets(connection):
    sql_stmt = "select distinct event_name from ticket_sales order by num_tickets,event_date"
    cursor = connection.cursor()
    cursor.execute(sql_stmt)
    records = cursor.fetchall()
    cursor.close()
    return records


if __name__ == '__main__':
    filepath = 'C:/Users/gauri/PycharmProjects/pythonProject/PipelineMiniProject/third_party_sales_1.csv'
    connection = get_db_connection()
    # load_third_party(connection,filepath)
    tup=query_popular_tickets(connection)
    for x in tup:
        name=str(*x)
        print(" - "+name)
