import psycopg2
import gzip
import json

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
file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.txt","w",encoding='utf-8')
#opennig tge Gzip data file line by line
with gzip.open("E:\School\ING\/1.Semeter\pdt database\/authors.jsonl.gz", "r") as f:
    counter = 0
    for line in f:
        #list_of_data.append(json.loads(line.decode()))
        data_line = json.loads(line)
        input = data_line['id'] + ';' + data_line['name'] + ';' + data_line['username'] + ';' +json.dumps(data_line['description']).replace('"','') +\
                ';' +str(data_line['public_metrics']['followers_count']) + ';' +str(data_line['public_metrics']['following_count']) +\
                ';' +str(data_line['public_metrics']['tweet_count']) + ';' + str(data_line['public_metrics']['listed_count']) +'\n'
        counter +=1
        file1.write(input)
        if counter > 10:
            file1.close()
            break



    file1 = open("E:\School\ING\/1.Semeter\pdt database\/authors.txt", "r",encoding='utf-8' )
    cursor.copy_from(file1, 'authors', sep=';')
    conn.commit()



