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
	if "cd" in t:
		if ".csv" in t:
			count = 0
			print(t)

			f = open(t, 'r',encoding='utf-8' ,newline='')
			# returnList=[]
			# title=['hosp_id','seq_no','func_date','func_type','id_birthday','acode_icd9_1','acode_icd9_2','acode_icd9_3','id_sex','id']
			# returnList.append(title)
			for row in csv.DictReader(f):
				# tempList=[]
				# tempList.append(row['hosp_id'])
				# tempList.append(row['seq_no'])
				# tempList.append(row['func_date'])
				# tempList.append(row['func_type'])
				# tempList.append(row['id_birthday'])
				# tempList.append(row['acode_icd9_1'])
				# tempList.append(row['acode_icd9_2'])
				# tempList.append(row['acode_icd9_3'])
				# tempList.append(row['id_sex'])
				# tempList.append(row['id'])    
				# returnList.append(tempList)
				if row['func_date']=='':
					row['func_date']= None

				cur.execute('''INSERT into cd (hosp_id,seq_no,func_date,func_type,id_birthday,acode_icd9_1,acode_icd9_2,acode_icd9_3,id_sex,id) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',(row['hosp_id'],row['seq_no'],row['func_date'],row['func_type'],row['id_birthday'],row['acode_icd9_1'],row['acode_icd9_2'],row['acode_icd9_3'],row['id_sex'],row['id']))
				conn.commit()	
				# print("insert successfully")

				count += 1
				row_num += 1

			print(count)
			f.close()	
# f = open('./cd_new.csv',"w" ,newline='')
# w = csv.writer(f)
# w.writerows(returnList)
# f.close()
print(row_num)
conn.close()



  