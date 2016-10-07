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
	if ("doc2012.csv" in t ):
		count = 0
		print(t)
		# if "doc" in t:
		# 	if ".csv" in t:
		f = open(t, 'r',encoding='utf-8' ,newline='')
		# returnList=[]
		# title=['prsn_id','lwrd_id','lwrd_renew_date','valid_e_date','work_rlace']
		# returnList.append(title)
		for row in csv.DictReader(f):
			if(row['lwrd_id']=='A0700' or row['lwrd_id']=='A2200'):
			# 	tempList=[]
			# 	tempList.append(row['prsn_id'])
			# 	tempList.append(row['lwrd_id'])
			# 	tempList.append(row['lwrd_renew_date'])
			# 	tempList.append(row['valid_e_date'])
			# 	tempList.append(row['work_rlace'])  
			# 	returnList.append(tempList)
				if row['lwrd_renew_date']=='':
					row['lwrd_renew_date']= None
				if row['valid_e_date']=='':
					row['valid_e_date']= None

				cur.execute('''INSERT into "an_doc" (prsn_id,lwrd_id,lwrd_renew_date,valid_e_date,work_rlace) values (%s, %s, %s, %s, %s)''',(row['prsn_id'],row['lwrd_id'],row['lwrd_renew_date'],row['valid_e_date'],row['work_rlace']))
				conn.commit()
				count += 1
				row_num += 1
			# print ("Table inserted successfully")
		print(count)
		f.close()
print(row_num)
conn.close			
# f = open('./doc2012_new.csv',"w" ,newline='')
# w = csv.writer(f)
# w.writerows(returnList)
# f.close()



  