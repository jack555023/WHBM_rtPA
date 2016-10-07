import csv
import os
import psycopg2


path=[]

for root, dirs, files in os.walk("./"):
    for f in files:
    	path.append(os.path.join(root, f)) 
conn = psycopg2.connect(database="nhird_new", user="postgres", password="lckung413", host="127.0.0.1", port="5432")
print("Opened database successfully")
cur = conn.cursor()


row_num = 0
for t in path:
	if "per" in t:
		if ".csv" in t:
			count = 0
			print(t)
			f = open(t, 'r',encoding='utf-8',newline='')
			# returnList=[]
			# title=['birthday','prsn_sex','work_status','valid_s_date','valid_e_date']
			# returnList.append(title)
			for row in csv.DictReader(f):
				if(row['work_status']=='2'):
					# tempList=[]
					# tempList.append(row['birthday'])
					# tempList.append(row['prsn_sex'])
					# tempList.append(row['work_status'])
					# tempList.append(row['valid_s_date']) 
					# tempList.append(row['valid_e_date'])  
					# returnList.append(tempList)
					if row['valid_s_date']=='':
						row['valid_s_date']= None
					if row['valid_e_date']=='':
						row['valid_e_date']= None

					cur.execute('''INSERT into "an_per" (prsn_id,birthday,prsn_sex,work_status,valid_s_date,valid_e_date,work_place	) values (%s, %s, %s, %s, %s, %s, %s)''',(row['prsn_id'],row['birthday'],row['prsn_sex'],row['work_status'],row['valid_s_date'],row['valid_e_date'],row['work_place']))
					conn.commit()
					count += 1
					row_num += 1
					# print ("Table inserted successfully")
			print(count)
			f.close()
print(row_num)
conn.close		
# f = open('./per_new.csv',"w" ,newline='')
# w = csv.writer(f)
# w.writerows(returnList)
# f.close()



  