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
	if "dd" in t:
		if ".csv" in t:
			count = 0
			print(t)
			f = open(t, 'r',encoding='utf-8' ,newline='')
			for row in csv.DictReader(f):
				if(row['part_mark']=='001' or row['part_mark']=='002' or row['part_mark']=='003' or row['part_mark']=='004' or row['part_mark']=='005' or row['part_mark']=='006'):
					if row['in_date']=='':
						row['in_date']= None
					if row['out_date']=='':
						row['out_date']= None
					cur.execute('''INSERT into dd (fee_ym,hosp_id,func_type,seq_no,id,id_birthday,in_date,out_date,prsn_id,icd9cm_code,icd9cm_code_1,icd9cm_code_2,icd9cm_code_3, icd9cm_code_4,id_sex,med_amt,part_mark) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',(row['fee_ym'],row['hosp_id'],row['func_type'],row['seq_no'],row['id'],row['id_birthday'],row['in_date'],row['out_date'],row['prsn_id'],row['icd9cm_code'],row['icd9cm_code_1'],row['icd9cm_code_2'],row['icd9cm_code_3'],row['icd9cm_code_4'],row['id_sex'],row['med_amt'],row['part_mark']))
					conn.commit()
					count += 1
					row_num += 1
					# print ("Table inserted successfully")
			print(count)
			f.close()
			
print(row_num)
conn.close	
# f = open('./dd_new.csv',"w" ,newline='')
# w = csv.writer(f)
# w.writerow(returnList)
# f.close()


# returnList=['']
			# title=[]
			# title.append('fee_ym')
			# title.append('hosp_id')
			# title.append('func_type')
			# title.append('seq_no')
			# title.append('id')
			# title.append('id_birthday')
			# title.append('in_date')
			# title.append('out_date')
			# title.append('prsn_id')
			# title.append('icd9cm_code')
			# title.append('icd9cm_code_1')
			# title.append('icd9cm_code_2')
			# title.append('icd9cm_code_3')
			# title.append('icd9cm_code_4')
			# title.append('id_sex')
			# title.append('med_amt')
			# title.append('part_mark')  
			# returnList.append(title)
 
# tempList=[]
					# tempList.append(row['fee_ym'])
					# tempList.append(row['hosp_id'])
					# tempList.append(row['func_type'])
					# tempList.append(row['seq_no'])
					# tempList.append(row['id'])
					# tempList.append(row['id_birthday'])
					# tempList.append(row['in_date'])
					# tempList.append(row['out_date'])
					# tempList.append(row['prsn_id'])
					# tempList.append(row['icd9cm_code'])
					# tempList.append(row['icd9cm_code_1'])
					# tempList.append(row['icd9cm_code_2'])
					# tempList.append(row['icd9cm_code_3'])
					# tempList.append(row['icd9cm_code_4'])
					# tempList.append(row['id_sex'])
					# tempList.append(row['med_amt'])
					# tempList.append(row['part_mark'])    
					# returnList.append(tempList)

#cur.execute('''INSERT into dd (fee_ym,hosp_id,func_type,seq_no,id,id_birthday,in_date,out_date,prsn_id,icd9cm_code,icd9cm_code_1,icd9cm_code_2,icd9cm_code_3, icd9cm_code_4,id_sex,med_amt,part_mark) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',(row['fee_ym'],row['hosp_id'],row['func_type'],row['seq_no'],row['id'],row['id_birthday'],row['in_date'],row['out_date'],row['prsn_id'],row['icd9cm_code'],row['icd9cm_code_1'],row['icd9cm_code_2'],row['icd9cm_code_3'],row['icd9cm_code_4'],row['id_sex'],row['med_amt'],row['part_mark']))