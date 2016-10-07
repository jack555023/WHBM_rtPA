import csv
import os
import psycopg2

path = []

for root, dirs, files in os.walk("./"):
    for f in files:
    	path.append(os.path.join(root, f)) 
conn = psycopg2.connect(database="nhird_new", user="postgres", password="lckung413", host="127.0.0.1", port="5432")
print("Opened database successfully")
cur = conn.cursor()

row_num = 0
for t in path:
	if "do2" in t:
		if ".csv" in t:
			count = 0
			print(t)
			f = open(t, 'r',encoding='utf-8' ,newline='')
			# returnList=[]
			# title=['fee_ym','hosp_id','seq_no','order_amt']
			# returnList.append(title)
			for row in csv.DictReader(f):
				# tempList=[]
				# tempList.append(row['fee_ym'])
				# tempList.append(row['hosp_id'])
				# tempList.append(row['seq_no'])
				# tempList.append(row['order_amt'])  
				# returnList.append(tempList)
				cur.execute('''INSERT into "do" (fee_ym, hosp_id, seq_no, order_amt, order_type, order_code ) values (%s, %s, %s, %s, %s, %s)''',(row['fee_ym'],row['hosp_id'],row['seq_no'],row['order_amt'],row['order_type'],row['order_code']))
				conn.commit()
				count += 1
				row_num += 1
				# print ("Table inserted successfully")
			print(count)
			f.close()	

print(row_num)
conn.close	
# f = open('./do_new.csv''],row['"w" ,newline='')
# w = csv.writer(f)
# w.writerows(returnList)
# f.close()
