import MySQLdb as mdb
import sys

con = None


con = mdb.connect('localhost', 'root', 'root', 'pubmed_bow');

cur = con.cursor()
cur.execute("SELECT VERSION()")

data = cur.fetchone()
    
print "Database version : %s " % data
con.close()