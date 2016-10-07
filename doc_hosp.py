import psycopg2
conn = psycopg2.connect(database="nhird_new", user="postgres", password="lckung413", host="127.0.0.1", port="5432")
print("Opened database successfully")
cur = conn.cursor()
def imports(period):
	sql_string1 = " select years,months,age_dec,lwrd_id,number,hosp_id,hosp_cont_type,hosp_educ_mark,area_no_h  from "  
	sql_string2 = " (select extract(year from date\'"+ period +"-01\') AS years, extract(month from date\'"+ period +"-01\') as months"
	sql_string3 = " ,10*extract(year from age(an_per.birthday,\'"+period+"-01\')/10) AS age_dec  ,count(distinct(an_per.prsn_id)) as number , "
	sql_string4 = " an_doc.lwrd_id,an_per.work_place"
	sql_string5 = " from an_per join an_doc "
	sql_string6 = " on an_per.prsn_id = an_doc.prsn_id"
	sql_string7 = " WHERE lwrd_renew_date<=\'"+ period +"-01\' AND an_doc.valid_e_date >=\'"+ period +"-01\'"
	sql_string8 = " group by an_per.work_place,age_dec,lwrd_id"
	sql_string9 = " )as a"
	sql_string10 =" join an_hosb on a.work_place = an_hosb.hosp_id"
	sql_string = sql_string1+sql_string2+sql_string3+sql_string4+sql_string5+sql_string6+sql_string7+sql_string8+sql_string9+sql_string10
	# print(sql_string)
	cur.execute(sql_string)
	data = cur.fetchall()
	for a in data:
		insert_string = "INSERT into doc_hosp (years,months,age,lwrd_id,doctor_num,hosp_id,hosp_cont_type,hosp_educ_mark,area_no_h) values (%s, %s, %s, %s,%s, %s, %s, %s,%s) ;"
		cur.execute(insert_string,a)
		conn.commit()
	print("successfully insert")
	print(period)
year = 2002
for y in range(11):
	for m in range(12):
		if m<9:
			period_date = str(year+y)+"-0"+str(m+1)
		else:
			period_date = str(year+y)+"-"+str(m+1)
		imports(period_date)
