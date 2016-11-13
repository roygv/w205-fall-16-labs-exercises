import psycopg2
import sys

# Connect to postgres
conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()

# Run query
if len(sys.argv) == 1:
    cur.execute("SELECT word, count FROM tweetwordcount ORDER BY word ASC")
    records = cur.fetchall()
    for rec in records:
        print rec[0]+':', rec[1] 
else:
    cur.execute("SELECT word, count FROM tweetwordcount WHERE word = %s ORDER BY word ASC",(sys.argv[1],))
    records = cur.fetchall()
    for rec in records:
        print rec[0]+':', rec[1] 
conn.commit()
conn.close()
