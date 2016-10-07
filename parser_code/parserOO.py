import csv
import os
import psycopg2

conn = psycopg2.connect(database="nhird_new", user="postgres", password="lckung413", host="127.0.0.1", port="5432")
print("Opened database successfully")
cur = conn.cursor()

path = []

for root, dirs, files in os.walk("./"):
    for f in files:
    	path.append(os.path.join(root, f)) 
row_num = 0
for t in path:
	if "oo" in t:
		if ".csv" in t:
			count = 0
			print(t)
			f = open(t, 'r',encoding='utf-8',newline='')
			for row in csv.DictReader(f):
				if(row['drug_no']=='B016526248' or row['drug_no']=='K000743248' or row['drug_no']=='K000744238' or row['drug_no']=='KC00743248'):
					cur.execute('''INSERT into oo (fee_ym, hosp_id, seq_no,drug_no) values (%s ,%s, %s, %s)''',(row['fee_ym'],row['hosp_id'], row['seq_no'],row['drug_no']))
					conn.commit()
					print("insert successfully")
					count+=1
					row_num += 1
			print(count)
			f.close()
print(row_num)	
conn.close()
