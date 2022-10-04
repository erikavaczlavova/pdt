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
            ADD PRIMARY KEY (id)"""
    cursor.execute(keys)
    conn.commit()
    block_e = time.time()
    print("Create constrains: " + timer(start_time, block_s, block_e))
def open_files(method):
    file1 = open("E:\School\ING\/1.Semeter\pdt database\/conversations.csv", method, newline='', encoding='utf-8')
    file2 = open("E:\School\ING\/1.Semeter\pdt database\/conversations_references.csv", method, newline='',encoding='utf-8')
    file3 = open("E:\School\ING\/1.Semeter\pdt database\/annotations.csv", method, newline='', encoding='utf-8')
    file4 = open("E:\School\ING\/1.Semeter\pdt database\/links.csv", method, newline='', encoding='utf-8')
    file5 = open("E:\School\ING\/1.Semeter\pdt database\/hashtags.csv", method, newline='', encoding='utf-8')
    file6 = open("E:\School\ING\/1.Semeter\pdt database\/context_domains.csv", method, newline='', encoding='utf-8')
    file7 = open("E:\School\ING\/1.Semeter\pdt database\/context_entities.csv", method, newline='', encoding='utf-8')
    file8 = open("E:\School\ING\/1.Semeter\pdt database\/context_annotations.csv", method, newline='', encoding='utf-8')
    return file1,file2,file3,file4,file5,file6,file7,file8
def copy(file1, file2, file3, file4, file5, file6, file7, file8):
    cursor.copy_from(file1, 'conversations', sep=';')
    cursor.copy_from(file2, 'conversation_references', sep=';', columns=('conversation_id', 'parent_id', 'type'))
    cursor.copy_from(file3, 'annotations', sep=';', columns=('conversation_id', 'value', 'type', 'probability'))
    cursor.copy_from(file4, 'links', sep=';', columns=('conversation_id', 'url', 'title', 'description'))
    cursor.copy_from(file5, 'hashtags', sep=';', columns=(['tag']))
    cursor.copy_from(file6, 'context_domains', sep=';', columns=('id', 'name', 'description'))
    cursor.copy_from(file7, 'context_entities', sep=';', columns=('id', 'name', 'description'))
    cursor.copy_from(file8, 'context_annotations', sep=';', columns=('conversation_id', 'context_domain_id', 'context_entity_id'))
def create_writers(file1, file2, file3, file4, file5, file6, file7, file8):
    writer = csv.writer(file1, delimiter=";")
    writer2 = csv.writer(file2, delimiter=";")
    writer3 = csv.writer(file3, delimiter=";")
    writer4 = csv.writer(file4, delimiter=";")
    writer5 = csv.writer(file5, delimiter=";")
    writer6 = csv.writer(file6, delimiter=";")
    writer7 = csv.writer(file7, delimiter=";")
    writer8 = csv.writer(file8, delimiter=";")
    return writer,writer2,writer3,writer4,writer5,writer6,writer7,writer8
def deduplicate_con():
    cursor.execute(
        """DELETE from conversations a USING 
           (SELECT MIN(ctid) as ctid, id
           FROM conversations
           GROUP BY id HAVING COUNT(*)>1)
           b WHERE a.id = b.id
           AND a.ctid <> b.ctid;
           DELETE from context_domains a USING 
           (SELECT MIN(ctid) as ctid, id
           FROM context_domains
           GROUP BY id HAVING COUNT(*)>1)
           b WHERE a.id = b.id
           AND a.ctid <> b.ctid;
           DELETE from context_entities a USING 
           (SELECT MIN(ctid) as ctid, id
           FROM context_entities
           GROUP BY id HAVING COUNT(*)>1)
           b WHERE a.id = b.id
           AND a.ctid <> b.ctid;
           DELETE from hashtags a USING 
           (SELECT MIN(ctid) as ctid, tag
           FROM hashtags
           GROUP BY id HAVING COUNT(*)>1)
           b WHERE a.tag = b.tag
           AND a.ctid <> b.ctid    
        """
    )

def altertables():
    cursor.execute("""
    ALTER table conversations
    ADD PRIMARY KEY (id),
    ALTER column author_id SET NOT NULL,
    ALTER column content SET NOT NULL,
    ALTER column possibly_sensitive SET NOT NULL,
    ALTER column language SET NOT NULL,
    ALTER column source SET NOT NULL,
    ALTER column created_at SET NOT NULL;    
    
    ALTER table contex_domains
    ALTER column name SET NOT NULL;
    
    ALTER table context_entities
    ALTER column name SET NOT NULL;
    
    ALTER table annotations
    ALTER column value SET NOT NULL,
    ALTER column type SET NOT NULL,
    ALTER column probability SET NOT NULL;
    
    ALTER table links
    ALTER column url SET NOT NULL;
    
    ALTER table conversation_references
    ALTER column type SET NOT NULL;
    
    ALTER table hashtags
    ADD UNIQUE (tag)
    """
    )
def conversations():
    start_time = time.time()
    with gzip.open("E:\School\ING\/1.Semeter\pdt database\/conversations.jsonl.gz", "r") as f:
        counter = 0
    start_time = time.time()
    # creating temporary files
    file1, file2, file3, file4, file5, file6, file7, file8 = open_files('w')
    writer, writer2, writer3, writer4, writer5, writer6, writer7, writer8 = create_writers(file1, file2, file3, file4, file5, file6, file7, file8)

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
    description text);
    CREATE table hashtags (
    id bigserial primary key,
    tag text);
    CREATE table context_domains (
    id int8,
    name VARCHAR(255),
    description text);
    CREATE table context_entities (
    id int8,
    name VARCHAR(255),
    description text);
    CREATE table context_annotations (
    id bigserial primary key,
    conversation_id int8,
    context_domain_id int8,
    context_entity_id int8)""")

    create_end = time.time()
    #conn.commit()
    print("Creating tables  + commit : " + timer(start_time, start_time, create_end))

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
                        if len(url['expanded_url']) > 2048:
                            input_link = [data_line['id'],url['expanded_url'].replace('\x00', '').replace('\\', '').replace(';',',').replace('\n', '').replace('\r', '')]
                        else:
                            input_link = [data_line['id'],'']
                        if url.get('title'):
                            input_link.append(url['title'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''))
                        else:
                            input_link.append('')

                        if url.get('description'):
                            input_link.append(url['description'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''))
                        else:
                            input_link.append('')

                        writer4.writerow(input_link)

                if data_line['entities'].get('hashtags'):
                    for has in data_line['entities'].get('hashtags'):
                        input_has = [has['tag'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', '')]
                        writer5.writerow(input_has)

            if data_line.get('context_annotations'):
                for x in data_line.get('context_annotations'):
                    input_c_ano = [data_line['conversation_id'],x['domain']['id'],x['entity']['id']]
                    writer8.writerow(input_c_ano)

                    input_c_dom = [x['domain']['id'],x['domain']['name'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', '')]
                    if x['domain'].get('description'):
                        input_c_dom.append(x['domain']['description'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''))
                    else:
                        input_c_dom.append('')
                    writer6.writerow(input_c_dom)

                    input_c_ent = [x['entity']['id'],x['entity']['name'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', '')]
                    if x['entity'].get('description'):
                        input_c_ent.append(x['entity']['description'].replace('\x00','').replace('\\','').replace(';',',').replace('\n','').replace('\r', ''))
                    else:
                        input_c_ent.append('')
                    writer7.writerow(input_c_ent)


            if counter == 100000:
                # cleaning file
                file1.close(),file2.close(),file3.close(),file4.close(),file5.close(),file6.close(),file7.close(),file8.close()
                file1, file2, file3, file4, file5, file6, file7, file8 = open_files('r')
                copy(file1, file2, file3, file4, file5, file6, file7, file8)

                block_e = time.time()
                print(timer(start_time, block_s, block_e))
                counter = 0
                file1.close(), file2.close(), file3.close(), file4.close(), file5.close(), file6.close(), file7.close(), file8.close()
                file1, file2, file3, file4, file5, file6, file7, file8 = open_files('w')
                conn.commit()
                block_s = time.time()
                break


        file1.close(), file2.close(), file3.close(), file4.close(), file5.close(), file6.close(), file7.close(), file8.close()
        file1, file2, file3, file4, file5, file6, file7, file8 = open_files('r')
        copy(file1, file2, file3, file4, file5, file6, file7, file8)
        block_e = time.time()
        print("Inserting last block of data and commit: " + timer(start_time, block_s, block_e))

    block_s = block_e
    altertables()
    block_e = time.time()
    print("Altertables: " + timer(start_time, block_s, block_e))
    block_s = block_e
    deduplicate_con()
    block_e = time.time()
    print("Deduplicate: (+commit)" + timer(start_time, block_s, block_e))

    block_s = block_e
    #connection conversations and conversation references + constrains
    cursor.execute("""DELETE from conversation_references cref where not exists (
                    SELECT id from conversations conv where cref.parent_id = conv.id);
                    ALTER TABLE conversation_references
                    add foreign key (parent_id) REFERENCES conversations (id)""")

    block_e = time.time()
    print("Conection and clean conversations : (+commit)" + timer(start_time, block_s, block_e))

    block_s = block_e
    cursor.execute("""insert into authors (id)
    select distinct author_id from conversations conv where conv.author_id not in (
    select id from authors auth where auth.id = conv.author_id )
    alter table conversations
    add foreign key (author_id) REFERENCES authors (id)""")
    block_e = time.time()
    conn.commit()
    print("Authors clean : (+commit)" + timer(start_time, block_s, block_e))


#authors()
print("conver\n")
conversations()