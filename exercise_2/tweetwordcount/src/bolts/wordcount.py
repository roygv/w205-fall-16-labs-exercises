from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import psycopg2


class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
	self.conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")
        self.cur = self.conn.cursor()
        self.cur.execute("select count(*) from pg_tables where tablename='tweetwordcount'")
        records = self.cur.fetchall()
        for rec in records:
            cnt=int(rec[0])
            self.conn.commit()
        if cnt == 0:
            self.cur.execute("""CREATE TABLE Tweetwordcount
                                (word TEXT PRIMARY KEY     NOT NULL,
                                 count INT     NOT NULL);""")
            self.cur.execute("""
                                CREATE INDEX tweetwordcount_count 
                                ON tweetwordcount USING btree (count);""")
            self.conn.commit()
        else:
            self.cur.execute("SELECT word, count from tweetwordcount")
            records = self.cur.fetchall()
            for rec in records:
                self.counts[rec[0]] = rec[1]
                self.conn.commit()

    def process(self, tup):
        word = tup.values[0]
        # Increment the local count
        self.counts[word] += 1
        if self.counts[word] == 1:
            self.cur.execute("INSERT INTO tweetwordcount (word,count) VALUES (%s, %s)", (word,1))
        else:
            self.cur.execute("UPDATE tweetwordcount SET count=count+1 WHERE word=%s", (word,))
        self.conn.commit()

	if self.counts[word] > 0: 
            self.emit([word, self.counts[word]])

        # Log the count - just to see the topology running
        # self.log('%s: %d' % (word, self.counts[word]))
