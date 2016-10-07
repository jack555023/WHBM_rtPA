import csv
import os
import psycopg2

conn = psycopg2.connect(database="nhird_new", user="postgres", password="lckung413", host="127.0.0.1", port="5432")
print("Opened database successfully")
cur = conn.cursor()
path=[]

for root, dirs, files in os.walk("./"):
    for f in files:
    	path.append(os.path.join(root, f)) 
for t in path:
		
	if "hosb" in t:
		if ".csv" in t:
			count = 0
			f = open(t, 'r',encoding='utf-8' ,newline='')
			print(t)
			# title=['hosp_id','hosp_cont_type','hosp_educ_mark','area_no_h']
			# returnList.append(title)
			for row in csv.DictReader(f):
				if(row['hosp_cont_type']=='1' or row['hosp_cont_type']=='2' or row['hosp_cont_type']=='3'):
					cur.execute('''INSERT into an_hosb (hosp_id, hosp_cont_type,hosp_educ_mark,area_no_h)values (%s, %s, %s, %s)''',(row['hosp_id'], row['hosp_cont_type'],row['hosp_educ_mark'],row['area_no_h']))
					conn.commit()
					count += 1
					# print ("Table inserted successfully")
			f.close()
	print(count)
conn.close()	

