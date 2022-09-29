import psycopg2
import gzip
import json
import datetime
import time
import csv
import math

#conection to database
conn = psycopg2.connect(
   database="postgres", user='postgres', password='postgres', host='localhost', port='5432'
)
cursor = conn.cursor()
cursor.execute("select version()")
data = cursor.fetchone()
print("Connection established to: ",data)

def timer(start,block_s,block_e):
    whole = block_e-start
    minutes, seconds = divmod(whole, 60)
    whole = str(int(minutes))+ ":"+ str(int(seconds))
    block_time = block_e-block_s
    minutes, seconds = divmod(block_time, 60)
    block_time = str(int(minutes))+ ":"+ str(math.ceil(int(seconds)))
    ret = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")+ "; "+whole+"; "+block_time

    return ret


def authors():
    start_time = time.time()
    # creating temporary file
    file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.csv", "w", newline='', encoding='utf-8')
    writer = csv.writer(file1, delimiter=";")
    cursor.execute("""create table authors (
    id int8,
    name VARCHAR(255),
    username VARCHAR(255),
    description TEXT,
    followers_count int4,
    following_count int4,
    tweet_count int4,
    listed_count int4)""")
    create_end = time.time()
    print("Creating table: "+ timer(start_time,start_time,create_end))
    print("Inserting 100 000 sized blocks")
    #opennig tge Gzip data file line by line
    with gzip.open("E:\School\ING\/1.Semeter\pdt database\/authors.jsonl.gz", "r") as f:
        counter = 0
        block_s = time.time()
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
            #write into temporary file
            writer.writerow(input)
            if counter == 100000:

                #cleaning file
                file1.close()
                file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.csv", "r", newline='',encoding='utf-8')
                cursor.copy_from(file1, 'authors', sep=';')
                block_e = time.time()
                print(timer(start_time,block_s,block_e))
                counter = 0
                file1.close()
                file1 = open('E:\School\ING\/1.Semeter\pdt database\/authors.csv', 'w', newline='',encoding='utf-8')
                writer = csv.writer(file1, delimiter=";")
                block_s = time.time()

        file1.close()
        file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.csv", "r", newline='', encoding='utf-8')
        cursor.copy_from(file1, 'authors', sep=';')
        conn.commit()
        block_e = time.time()
        print("Inserting last block and commit: " + timer(start_time,block_s,block_e))

    block_s = block_e
    #delete duplicates in database
    deduplicate = """DELETE from authors a USING 
                        (SELECT MIN(ctid) as ctid, id
                        FROM authors
                        GROUP BY id HAVING COUNT(*)>1)
                        b WHERE a.id = b.id
                        AND a.ctid <> b.ctid"""

    cursor.execute(deduplicate)
    conn.commit()
    block_e = time.time()
    print("Deduplicate and commit: " + timer(start_time, block_s, block_e))
    block_s = block_e
    #create constraints
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
    block_e = time.time()
    print("Create constrains: " + timer(start_time, block_s, block_e))


def conversations():
    start_time = time.time()
    with gzip.open("E:\School\ING\/1.Semeter\pdt database\/authors.jsonl.gz", "r") as f:
        counter = 0


authors()