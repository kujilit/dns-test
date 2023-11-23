import psycopg2 
import csv
import threading
import time


def timer(f):
    def wrapper(*args, **kwargs):
        t = time.time()
        res = f(*args, **kwargs)
        print("Время выполнения функции: %f" % (time.time()-t))
        return res
    return wrapper

@timer
def load_to_database(filename, table_name):
    with open(f'../test_data/{filename}', 'r') as f:
        cur.copy_expert(f'COPY {table_name} FROM stdin WITH CSV HEADER', f)
    print(f'table {table_name} created')


db_params = {'database' : 'postgres',
             'user' : 'postgres',
             'password' : 'admin',
             'host' : '127.0.0.1',
             'port' : '5432'}

conn = psycopg2.connect(database=db_params['database'], 
						user=db_params['user'],
                        password=db_params['password'], 
						host=db_params['host'],
                        port=db_params['port']
) 

cur = conn.cursor() 


cur.execute('''CREATE TABLE cities(
            city_id int NOT NULL,\
            city_key char(100),\
            city_name char(30),\
            PRIMARY KEY (city_key));''')

print('created')

cur.execute('''CREATE TABLE branches(
            branch_id int NOT NULL,\
            branch_key char(100),\
            branch_name char(40),\
            city_key varchar(100) REFERENCES cities (city_key),\
            branch_short_name char(100),\
            branch_region char(40),\
            PRIMARY KEY (branch_key));''')

print('created')

cur.execute('''CREATE TABLE products(
            product_id int NOT NULL,\
            product_key char(100),\
            product_name char(200),\
            PRIMARY KEY (product_key));''')

print('created')

cur.execute('''CREATE TABLE sales(
            sale_id int NOT NULL,\
            sale_period char(40),\
            branch_key char(100) REFERENCES branches (branch_key),\
            product_key char(40) REFERENCES products (product_key),\
            sale_quantity float,\
            sale_price float);''')

print('created')

# th1 = threading.Thread(target=load_to_database, args=('t_cities.csv', 'cities',))
# th2 = threading.Thread(target=load_to_database, args=('t_branches.csv', 'branches',))
# th3 = threading.Thread(target=load_to_database, args=('t_products.csv', 'products',))
# th4 = threading.Thread(target=load_to_database, args=('t_sales.csv', 'sales',))

load_to_database('t_cities.csv', 'cities')
load_to_database('t_branches.csv', 'branches')
load_to_database('t_products.csv', 'products')
load_to_database('t_sales.csv', 'sales')

# th1.start()
# th2.start()
# th3.start()
# th4.start()

# th1.join()
# th2.join()
# th3.join()
# th4.join()

conn.commit() 
conn.close() 
