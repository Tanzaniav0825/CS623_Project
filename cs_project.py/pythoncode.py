import psycopg2
from tabulate import tabulate 

print('Beginning')

con = psycopg2.connect(
    host='localhost',
    database='cs623',
    user= 'lisawilson',
    password= 'Tanzania3@'

)

print(con)

con.set_isolation_level(3)

cur = con.cursor()

#Creating table Product
create_table_product = '''
CREATE TABLE Product(
Prodid VARCHAR(2) PRIMARY KEY, 
Pname VARCHAR(15),
Price INT
);
'''

#Creating table Depot
create_table_depot = '''
CREATE TABLE Depot(
Depid VARCHAR(2) PRIMARY KEY, 
Addr VARCHAR(20),
Volume INT
);
'''

#Creating table Stock
create_table_stock = '''
CREATE TABLE Stock(
Prodid VARCHAR(2),
Depid VARCHAR(2),
Quantity INT,
PRIMARY KEY (Prodid, Depid),
FOREIGN KEY (Prodid) REFERENCES Product (Prodid),
FOREIGN KEY (Depid) REFERENCES Depot (Depid)
);
'''

#executing the creation of the tables 
cur.execute(create_table_product)
cur.execute(create_table_depot)
cur.execute(create_table_stock)

#Inserting data into tables 

insert_info_product = '''
INSERT INTO Product(Prodid, Pname, Price) VALUES 
('p1', 'tape', 2.5),
('p2', 'tv', 250),
('p3', 'vcr', 80);
'''

insert_info_depot = '''
INSERT INTO Depot(Depid, Addr, Volume) VALUES 
('d1', 'New York', 9000),
('d2', 'Syracuse', 6000),
('d4', 'New York', 2000);
'''

insert_info_stock = '''
INSERT INTO Stock(Prodid, Depid, Quantity) VALUES 
('p1', 'd1', 1000),
('p1', 'd2', -100),
('p1', 'd4', 1200),
('p3', 'd1', 3000),
('p3', 'd4', 2000),
('p2', 'd4', 1500),
('p2', 'd1', -400),
('p2', 'd2', 2000);
'''

#Executing the insert into tables

cur.execute(insert_info_product)
cur.execute(insert_info_depot)
cur.execute(insert_info_stock)

try:
    con.autocommit = False

    delete_stock_d1 ='''
    DELETE FROM Stock
    WHERE Depid ='d1';
    '''
    cur.execute(delete_stock_d1)

    delete_depot_d1 = '''
    DELETE FROM Depot
    WHERE Depid = 'd1';
    '''
    cur.execute(delete_depot_d1)

    con.commit()
    print("Transaction commited successfully.")
except Exception as err:
    con.rollback()
    print("Transaction rolled back:",err)
finally: 
    con.autocommit = True

cur.close()
con.close()


