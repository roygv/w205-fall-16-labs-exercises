import psycopg2
import sys

# Connect to postgres
conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()

# Run query
if (len(sys.argv) != 3 or sys.argv[1].isdigit() == False or sys.argv[2].isdigit() == False):
    print 'Usage: python hystogram.py num1 num2'
    print '\t The program returns a list of words that appeared in tweets between num1 and num2 times.'
    print '\t num1 and num2 must be integers and num1 must be smaller or equal to num2'
else:
    cur.execute("SELECT word, count FROM tweetwordcount WHERE count between %s and %s ORDER BY count DESC",(sys.argv[1], sys.argv[2],))
    records = cur.fetchall()
    for rec in records:
        print rec[0]+':', rec[1] 
conn.commit()
conn.close()
