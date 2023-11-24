import psycopg2 
import csv
import threading
import time
 

def timer(f):
    '''
    Декоратор для измерения времени работы функции.
    Ответ возвращается в секундах.
    '''
    def wrapper(*args, **kwargs):
        t = time.time()
        res = f(*args, **kwargs)
        print("Время выполнения функции: %f" % (time.time()-t))
        return res
    return wrapper

@timer
def load_to_database(filename, table_name):
    '''
    Функция, на вход которой передаётся имя csv-файла и имя таблицы.
    Переносит все данные из csv в таблицы, минуя header строку.
    '''
    with open(f'../test_data/{filename}', 'r') as f:
        cur.copy_expert(f'COPY {table_name} FROM stdin WITH CSV HEADER', f)
    print(f'table {table_name} created')


# Для удобства указываю все данные для подключения к БД в словаре.

db_params = {'database' : 'postgres',
             'user' : 'postgres',
             'password' : 'admin',
             'host' : '127.0.0.1',
             'port' : '5432'}

# Выполняю подключение к базе данных и создаю курсор.

conn = psycopg2.connect(database=db_params['database'], 
						user=db_params['user'],
                        password=db_params['password'], 
						host=db_params['host'],
                        port=db_params['port']
) 
cur = conn.cursor() 


cur.execute('''CREATE TABLE cities(
            city_id INT NOT NULL,\
            city_key VARCHAR(100),\
            city_name VARCHAR(30),\
            PRIMARY KEY (city_key));''')

cur.execute('''CREATE TABLE branches(
            branch_id INT NOT NULL,\
            branch_key VARCHAR(100),\
            branch_name VARCHAR(40),\
            city_key VARCHAR(100) REFERENCES cities (city_key),\
            branch_short_name VARCHAR(100),\
            branch_region VARCHAR(40),\
            PRIMARY KEY (branch_key));''')

cur.execute('''CREATE TABLE products(
            product_id INT NOT NULL,\
            product_key VARCHAR(100),\
            product_name VARCHAR(200),\
            PRIMARY KEY (product_key));''')

cur.execute('''CREATE TABLE sales(
            sale_id INT NOT NULL,\
            sale_period TIMESTAMP,\
            branch_key VARCHAR(100) REFERENCES branches (branch_key),\
            product_key VARCHAR(40) REFERENCES products (product_key),\
            sale_quantity FLOAT,\
            sale_price FLOAT);''')

# th1 = threading.Thread(target=load_to_database, args=('t_cities.csv', 'cities',))
# th2 = threading.Thread(target=load_to_database, args=('t_branches.csv', 'branches',))
# th3 = threading.Thread(target=load_to_database, args=('t_products.csv', 'products',))
# th4 = threading.Thread(target=load_to_database, args=('t_sales.csv', 'sales',))

load_to_database('t_cities.csv', 'cities')
load_to_database('t_branches.csv', 'branches')
load_to_database('t_products.csv', 'products')
load_to_database('t_sales.csv', 'sales')

conn.commit() 
cur.close()
conn.close() 
