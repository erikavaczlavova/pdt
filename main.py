import psycopg2
import gzip
import json
import time
import csv

#conection to database
conn = psycopg2.connect(
   database="postgres", user='postgres', password='postgres', host='localhost', port='5432'
)
cursor = conn.cursor()
cursor.execute("select version()")
data = cursor.fetchone()
print("Connection established to: ",data)

#postgres_insert_query = """ INSERT INTO authors (name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
#record_to_insert = ('cica', 'cicamica', 'cute cat hehe', 12, 324, 950, 123)
#cursor.execute(postgres_insert_query, record_to_insert)
#conn.commit()
#count = cursor.rowcount
#print(data)


#postgres_insert_query = """ INSERT INTO authors (name, username, description, followers_count, following_count, tweet_count, listed_count) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
#count = 0


#creating temporary file
file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.csv","w",newline='',encoding='utf-8')
writer = csv.writer(file1, delimiter=";")

#opennig tge Gzip data file line by line
start_time = time.time()
with gzip.open("E:\School\ING\/1.Semeter\pdt database\/authors.jsonl.gz", "r") as f:
    counter = 0
    for line in f:
        data_line = json.loads(line)
        input = []
        input = [data_line['id'],
                data_line['name'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''),
                data_line['username'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''),
                data_line['description'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''),
                data_line['public_metrics']['followers_count'],data_line['public_metrics']['following_count'],
                data_line['public_metrics']['tweet_count'],data_line['public_metrics']['listed_count']]
        counter += 1
        writer.writerow(input)
        if counter == 100000:
            file1.close()
            file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.csv", "r", newline='',encoding='utf-8')
            cursor.copy_from(file1, 'authors', sep=';')
            print(time.time() - start_time)
            counter = 0
            file1.close()
            file1 = open('E:\School\ING\/1.Semeter\pdt database\/authors.csv', 'w', newline='',encoding='utf-8')
            writer = csv.writer(file1, delimiter=";")

    file1.close()
    file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.csv", "r", newline='', encoding='utf-8')
    cursor.copy_from(file1, 'authors', sep=';')
    conn.commit()
    print(time.time() - start_time)

deduplicate = """DELETE from authors a USING 
                    (SELECT MIN(ctid) as ctid, id
                    FROM authors
                    GROUP BY id HAVING COUNT(*)>1)
                    b WHERE a.id = b.id
                    AND a.ctid <> b.ctid"""

cursor.execute(deduplicate)
conn.commit()
print(time.time() - start_time)

keys = """ALTER TABLE authors
    ADD PRIMARY KEY (id),
    ALTER COLUMN name SET NOT NULL,
    ALTER COLUMN username SET NOT NULL,
    ALTER COLUMN description SET NOT NULL,
    ALTER COLUMN following_count SET NOT NULL,
    ALTER COLUMN followers_count SET NOT NULL,
    ALTER COLUMN tweet_count SET NOT NULL,
    ALTER COLUMN listed_count SET NOT NULL;"""

cursor.execute(keys)
conn.commit()
print(time.time() - start_time)


