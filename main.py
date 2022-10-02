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
    #opennig the Gzip data file line by line
    with gzip.open("E:\School\ING\/1.Semeter\pdt database\/authors.jsonl.gz", "r") as f:
        counter = 0
        block_s = time.time()
        for line in f:
            data_line = json.loads(line)
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
    with gzip.open("E:\School\ING\/1.Semeter\pdt database\/conversations.jsonl.gz", "r") as f:
        counter = 0
    start_time = time.time()

    # creating temporary files
    file1 = open("E:\School\ING\/1.Semeter\pdt database\/conversations.csv", "w", newline='', encoding='utf-8')
    writer = csv.writer(file1, delimiter=";")
    file2 = open("E:\School\ING\/1.Semeter\pdt database\/conversations_references.csv", "w", newline='', encoding='utf-8')
    writer2 = csv.writer(file2, delimiter=";")
    file3 = open("E:\School\ING\/1.Semeter\pdt database\/annotations.csv", "w", newline='',encoding='utf-8')
    writer3 = csv.writer(file3, delimiter=";")
    file4 = open("E:\School\ING\/1.Semeter\pdt database\/links.csv", "w", newline='', encoding='utf-8')
    writer4 = csv.writer(file4, delimiter=";")

    cursor.execute("""create table conversations (
    id int8,
    author_id int8,
    content TEXT,
    possibly_sensitive bool,
    language varchar(3),
    source text,
    retweet_count int4,
    reply_count int4,
    like_count int4,
    quote_count int4,
    created_at TIMESTAMP);
    CREATE table conversation_references (
    id bigserial primary key,
    conversation_id int8,
    parent_id int8,
    type varchar(20));
    CREATE table annotations (
    id bigserial primary key,
    conversation_id int8,
    value text,
    type text,
    probability NUMERIC(4,3));
    CREATE table links (
    id bigserial primary key,
    conversation_id int8,
    url varchar(2048),
    title text,
    description text)""")

    create_end = time.time()
    #conn.commit()
    print("Creating tables (4) + commit : " + timer(start_time, start_time, create_end))

    with gzip.open("E:\School\ING\/1.Semeter\pdt database\/conversations.jsonl.gz", "r") as f:
        counter = 0
        block_s = time.time()
        counter = 0
        for line in f:
            data_line = json.loads(line)
            counter += 1
            input = [data_line['id'],data_line['author_id'],
                    data_line['text'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''),
                    data_line['possibly_sensitive'],
                    data_line['lang'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''),
                    data_line['source'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''),
                    data_line['public_metrics']['retweet_count'],data_line['public_metrics']['reply_count'],
                    data_line['public_metrics']['like_count'],data_line['public_metrics']['quote_count'],
                    data_line['created_at']]
            writer.writerow(input)

            if data_line.get('referenced_tweets'):
                for ref in data_line['referenced_tweets']:
                    input_ref_c = [data_line['id'],ref['id'],ref['type']]
                    writer2.writerow(input_ref_c)

            if data_line.get('entities'):
                if data_line['entities'].get('annotations'):
                    for annot in data_line['entities']['annotations']:
                        input_annot = [data_line['id'],annot['normalized_text'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''),annot['type'],annot['probability']]
                        writer3.writerow(input_annot)

                if data_line['entities'].get('urls'):
                    for url in data_line['entities']['urls']:
                        input_link = [data_line['id'],url['expanded_url']]
                        if url.get('title'):
                            input_link.append(url['title'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''))
                        else:
                            input_link.append('')

                        if url.get('description'):
                            input_link.append(url['description'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''))
                        else:
                            input_link.append('')

                        writer4.writerow(input_link)

            if counter == 100000:
                # cleaning file
                file1.close()
                file2.close()
                file3.close()
                file4.close()

                file1 = open("E:\School\ING\/1.Semeter\pdt database\/conversations.csv", "r", newline='', encoding='utf-8')
                cursor.copy_from(file1, 'conversations', sep=';')

                file2 = open("E:\School\ING\/1.Semeter\pdt database\/conversations_references.csv", "r", newline='',encoding='utf-8')
                cursor.copy_from(file2,'conversation_references', sep=';', columns=('conversation_id','parent_id','type'))

                file3 = open("E:\School\ING\/1.Semeter\pdt database\/annotations.csv", "r", newline='',encoding='utf-8')
                cursor.copy_from(file3, 'annotations', sep=';',columns=('conversation_id','value', 'type', 'probability'))

                file4 = open("E:\School\ING\/1.Semeter\pdt database\/links.csv", "r", newline='',encoding='utf-8')
                cursor.copy_from(file4, 'links', sep=';',columns=('conversation_id', 'url', 'title', 'description'))

                block_e = time.time()
                print(timer(start_time, block_s, block_e))
                counter = 0
                file1.close()
                file1 = open('E:\School\ING\/1.Semeter\pdt database\/conversations.csv', 'w', newline='', encoding='utf-8')
                file2.close()
                file2 = open('E:\School\ING\/1.Semeter\pdt database\/conversations_references.csv', 'w', newline='', encoding='utf-8')
                file3.close()
                file3 = open('E:\School\ING\/1.Semeter\pdt database\/annotations.csv', 'w', newline='',encoding='utf-8')
                file4.close()
                file4 = open('E:\School\ING\/1.Semeter\pdt database\/links.csv', 'w', newline='',encoding='utf-8')
                writer = csv.writer(file1, delimiter=";")
                writer2 = csv.writer(file2, delimiter=";")
                writer3 = csv.writer(file3, delimiter=";")
                writer4 = csv.writer(file4, delimiter=";")
                conn.commit()
                block_s = time.time()
                break


conversations()