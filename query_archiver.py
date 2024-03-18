import pandas as pd
import numpy as np
from pathlib import Path
import re as re
import pyodbc

pd.set_option('display.max_columns', 100)
#============================Ambil query===================================
adf = """
select
a.CFBRN CIFNO, a.CFNAME NAMA_NASABAH, a.BRANCH, c.BRDESC, D.NAMA NAMA_PBO, d.SLP_ID, e.NAMA_SLP
from [dbo].[CUSPRI] a with(nolock)
inner join T_PBO_TO_CUSPRI_RELATION b with(nolock)
on a.CRDNBR = b.NO_KARTU_PRIORITAS
inner join DWH_BRANCH c with(nolock)
on a.BRANCH = c.BRANCH
inner join MST_PBO d with(nolock)
on b.PBO_ID = d.PBO
inner join T_SLP e with(nolock)
on d.SLP_ID = e.ID
"""

#query = adf.replace("`",'')
adf = re.sub('--[^\n]+\n', '', adf)
query2 = adf.replace("\n",'  ')
query2 = query2.replace("  ","  ")
#query3 = query2.replace("] = ",']| = |')

########################Membersihkan Query###########################################
query4 = query2.replace("SELECT","select")
query4 = query4.replace("Select","select")
query4 = query4.replace("FROM","from")
query4 = query4.replace("from "," from ")
query4 = query4.replace("="," = ")
query4 = query4.replace("\'as","\' as")
query4 = query4.replace(")as",") as")

query4 = query4.replace("distinct","")
query4 = query4.replace("DISTINCT","")
query4 = query4.replace(")",") ")
query4 = query4.replace("--,",",--")
query4 = query4.replace('"','\'')
query4 = query4.replace('\t',' ')
query4 = query4.replace("  ",' ')
################################################################################

###############################MEMECAH FROM################################################
print(query4)
list = query4.split(' from ')
#re.split(" on ", temp, flags=re.IGNORECASE)[0]
#print(list)
###############################################################################################

#===============================MEMECAH KOMA======================================#
selectstring = list[0].replace("select",'')
selectitems = selectstring.split(',')
#############################################################################

#==============UNTUK MENGGABUNGKAN KOMA DALAM PROSES2 KURUNG=================================#
countcomma = selectstring.count(',')
countkurungbuka = np.char.count(selectitems, '(')
countkurungtutup = np.char.count(selectitems, ')')
flagkurung = 0
flagkurung2 = []
flagselect = []

while flagkurung <= countcomma:
    kurungbukacountainer = 0
    if countkurungbuka[flagkurung] - countkurungtutup[flagkurung] > 0:
        flagkurungdinamis = flagkurung

        flag = flagkurung
        flagselect.append(flag)
        flagkurung2.append("1")
        while kurungbukacountainer + countkurungbuka[flagkurungdinamis] - countkurungtutup[flagkurungdinamis] != 0:
            kurungbukacountainer = kurungbukacountainer + countkurungbuka[flagkurungdinamis] - countkurungtutup[flagkurungdinamis]
            flagselect.append(flag)
            flagkurung2.append("1")
            flagkurungdinamis = flagkurungdinamis + 1
        flagkurung = flagkurungdinamis + 1
    else:
        flagkurung = flagkurung + 1
        flagselect.append('x')
        flagkurung2.append("0")

selectlist = pd.DataFrame(selectitems, columns=['kolom'])
selectflag = pd.DataFrame(flagselect, columns=['flag'])
flagkurunglist = pd.DataFrame(flagkurung2, columns=['flag_kurung'])

selectlist.insert(loc=0, column="index_kol", value=selectlist.reset_index().index)
selectflag.insert(loc=0, column="index_flag", value=selectlist.reset_index().index)
flagkurunglist.insert(loc=0, column="index_kurung", value=selectlist.reset_index().index)

#selectflag.loc[selectflag['flag'] == "x", 'flag'] = selectflag['index_flag']

def indexflag(selectflag):
    if selectflag['flag'] == "x":
        return selectflag['index_flag']
    else:
        return selectflag['flag']

selectflag['flag'] = selectflag.apply(indexflag, axis = 1)

selectjoin = pd.DataFrame()
selectjoin = selectlist.join(selectflag, lsuffix='index_kol', rsuffix='index_flag')
selectjoin = selectjoin.join(flagkurunglist, lsuffix = 'index_kol', rsuffix='index_kurung')
selectjoin = selectjoin.loc[:, ('kolom', 'flag', 'flag_kurung')]
selectjoin['kolom2'] = selectjoin.loc[:, ('kolom', 'flag')].groupby(['flag'])['kolom'].transform(lambda x: ','.join(x))
selectjoin['kolom2'] = selectjoin.loc[:, ('kolom2')].str.rstrip()
selectjoin['kolom2'] = selectjoin.loc[:, ('kolom2')].str.lstrip()

selectjoin = selectjoin.loc[:, ('kolom2','flag', 'flag_kurung')].drop_duplicates()
#=====================================================================================================


def kolom_edit(selectjoin):
    if "\'" in selectjoin['kolom2']:
        temp = str(selectjoin['kolom2'])
        temp2 = temp.replace("\'",'~_e')
        return temp2
    else:
        return selectjoin['kolom2']
selectjoin['kolom2'] = selectjoin.apply(kolom_edit, axis = 1)

def kolom2(selectjoin):
    if "`" in selectjoin['kolom2'] and "[" in selectjoin['kolom2']:
        return re.sub(r'\`(.*?)\`', lambda x: '[' + x.group(1).replace("`", "") + ']', selectjoin['kolom2'])
    else:
        return selectjoin['kolom2']

selectjoin['kolom2'] = selectjoin.apply(kolom2, axis = 1)



def kolom3(selectjoin):
    if "`" in selectjoin['kolom2'] :
        return re.sub(r'\`(.*?)\`', lambda x: '`' + x.group(1).replace(" ", "ψ") + '`', selectjoin['kolom2'])
    elif "[" in selectjoin['kolom2']:
        return re.sub('\[(.*?)\]', lambda x: '[' + x.group(1).replace(" ", "ψ") + ']', selectjoin['kolom2'])
    elif "*" in selectjoin['kolom2']:
        return selectjoin['kolom2'].replace(" * ", "ψ*ψ")
    elif "\'" in selectjoin['kolom2']:
        return re.sub(r'\'(.*?)\'', lambda x: '\'' + x.group(1).replace(" ", "ψ") + '\'', selectjoin['kolom2'])
    else:
        return selectjoin['kolom2']

kolom = pd.DataFrame()
selectjoin['kolom3'] = selectjoin.apply(kolom3, axis = 1)
#selectjoin.to_csv(filepath)
#print(selectjoin)
def flag_drop(selectjoin):
    if "--" in selectjoin['kolom3']:
        return "x"
    else:
        return ""
selectjoin['flag_drop'] = selectjoin.apply(flag_drop, axis = 1)
selectjoin = selectjoin.drop(selectjoin[selectjoin['flag_drop']=="x"].index)


selectjoin = selectjoin.astype({'kolom2':'string'})
selectjoin = selectjoin.astype({'kolom3':'string'})
selectjoin['kolomreverse_temp'] = selectjoin.loc[:, ('kolom3')].apply(lambda x: x[::-1])

selectjoin['kolomreverse'] = selectjoin['kolomreverse_temp'].str.replace("\)",")^^>>",regex = True)

selectjoin[')_id_text'] = selectjoin['kolomreverse'].str.split('^^>>').str[0].apply(lambda x: x[::-1])
selectstatus = pd.DataFrame()
#print(selectjoin)

selectstatus['totalwords'] = selectjoin.loc[:, ('kolom3')].str.split().str.len()
selectstatus['cast'] = selectjoin.loc[:, ('kolom2')].str.lower().str.contains("cast", regex = False)
selectstatus['sama_dengan'] = selectjoin.loc[:, ('kolom2')].str.lower().str.contains("=", regex = False)
selectstatus['case_when'] = selectjoin.loc[:, ('kolom2')].str.lower().str.contains("case when", regex = False)
selectstatus['as'] = selectjoin.loc[:, ('kolom2')].str.lower().str.contains(" as ", regex = False)
selectstatus['flag_kurung'] = selectjoin['flag_kurung']
#========================butuh pengembanggan berikutnya======================================================

selectstatus['as_type'] = selectjoin.loc[:, ('kolom2')].str.lower().str.contains('as decimal|as int|as date|as string| as time|as datetime|as char|as varchar|as numeric|as binary|as float|as timestamp', regex = True)

#============================================================================================================
selectstatus['temp2'] = selectjoin.loc[:, ('kolom2')].str.lower()
selectstatus['temp2'] = selectstatus['temp2'].str.replace('as decimal|as int|as date|as string| as time|as datetime|as char|as varchar|as binary|as float|as timestamp|as numeric', "", regex = True)
selectstatus['as_id'] = selectstatus.loc[:, ('temp2')].str.lower().apply(lambda x: " as " in x and "as decimal" not in x and "as int" not in x and "as date" not in x and "as string" not in x and "as time" not in x and "as datetime" not in x and "as char" not in x and "as varchar" not in x and "as binary" not in x and "as float" not in x and "as timestamp" not in x)

selectstatus['Index_samadengan'] = selectjoin['kolom2'].str.find(' = ')
temp = selectjoin['kolom2'].str.lower()
selectstatus['Index_case_when'] = temp.str.find('case when')

def flag_equals_case(selectstatus):
    if(selectstatus['Index_samadengan'] == -1):
        return 0
    else:
        if(selectstatus['Index_samadengan'] < selectstatus['Index_case_when']) == True:
            return 1
        else:
            return 0
selectstatus['= case when'] = selectstatus.apply(flag_equals_case, axis = 1)
#============================================================================================================

selectstatus['end_id'] = selectjoin.loc[:, ('kolom2')].str.lower().apply(lambda x: "end " in x or "end)" in x and "end as" not in x)
selectstatus['end_as'] = selectjoin.loc[:, ('kolom2')].str.lower().apply(lambda x: "end as" in x)
selectstatus[')_id'] = selectjoin.loc[:, (')_id_text')].str.lower().apply(lambda x:") " in x and ") as" not in x)
selectstatus[')_as'] = selectjoin.loc[:, (')_id_text')].str.lower().apply(lambda x:")  as" in x or ") as" in x)
#print(selectstatus)
#print(selectjoin)
#print(selectstatus)
def flag(selectstatus):
    if(selectstatus['totalwords'] == 2):
        return "1/A"
    else:
        if(selectstatus['totalwords'] == 1):
            return "0"
        else:
            if(selectstatus['sama_dengan']==True):
                if(selectstatus['case_when']==True):
                    if(selectstatus['= case when']==1):
                        return "1/B"
                    else:
                        if(selectstatus['end_as'] == True):
                            return "0"
                        else:
                            if(selectstatus['end_id'] == True):
                                return"1/A"
                            else:
                                return "0"
                else:
                    return "1/B"
            else:
                if(selectstatus['flag_kurung']=="0"):
                    if(selectstatus['as'] == True):
                        if(selectstatus['as_type'] == True):
                            if(selectstatus['as_id'] == True):
                                return "0"
                            else:
                                return "1/A"
                        else:
                            return "0"
                    else:
                        return "1/A"
                else:
                    if(selectstatus['end_as'] == True or selectstatus[')_as'] == True):
                        return "0"
                    else:
                        if(selectstatus[')_id'] == True):
                            return "1/A"
                        else:
                            return "0"


selectstatus['Flag_samadengan'] = selectstatus.apply(flag, axis = 1)

selectstatus = selectstatus.loc[:, ('Flag_samadengan','totalwords')]
# print('select status')
# print(selectstatus)
# print('---------------------------')
selectjoin = selectjoin.join(selectstatus, lsuffix='Flag_samadengan', rsuffix='Flag')

selectjoin = selectjoin.loc[:, ('flag','kolom2', 'kolom3', 'totalwords','Flag_samadengan', 'kolomreverse')]

#print(selectjoin)
selectjoin = selectjoin.astype({'kolomreverse':'string'})

def kolom_final(selectjoin):
    if (selectjoin['Flag_samadengan'] == "1/B"):
        temp = str(selectjoin['kolom2'])
        temp = temp.replace("~_e", "\'")
        temp = temp.replace("[_e", "\'")
        temp = temp.replace("]_e", "\'")
        temp2 = temp.split(" = ", 1)
        temp3 = temp2[1] + ' as ' + temp2[0]

        return temp3.replace("ψ",' ')
    elif(selectjoin['Flag_samadengan']=="1/A"):
        if(selectjoin['totalwords']==2):
            temp = selectjoin['kolom3'].replace("  ", " ")
            temp = temp.replace(" ", " as ")
            temp = temp.replace("~_e", "\'")
            temp = temp.replace("[_e", "\'")
            temp = temp.replace("]_e", "\'")
            return temp.replace("ψ",' ')
        else:
            temp = selectjoin['kolomreverse'].replace("  "," ")
            temp = temp.replace("e_~", "\'")
            temp = temp.replace("e_[", "\'")
            temp = temp.replace("e_]", "\'")
            temp2 = temp.split(" ",1)
            temp3 = temp2[0] + ' sa ' + temp2[1]
            temp4 = temp3[::-1]
            temp5 = temp4.replace(">>^^",'')
            temp6 = temp5.replace(") )", "))")
            return temp6.replace("ψ",' ')
    else:
        temp = selectjoin['kolom3'].replace("ψ",' ')
        temp = temp.replace("~_e", "\'")
        temp = temp.replace("[_e", "\'")
        temp = temp.replace("]_e", "\'")
        return temp
selectjoin['Kolom_Final'] = selectjoin.apply(kolom_final, axis=1)


selectjoin = selectjoin.loc[:, ('flag','Kolom_Final')]
selectjoin['Kolom_Final'] = selectjoin.loc[:,('Kolom_Final')].str.strip()
#print('------------------')
print(selectjoin)
#LAPTOP-2FVS6AFV
connStr = ('DRIVER={SQL Server Native Client 11.0};SERVER=EDM-SV-ANDREP;DATABASE=SourceDataAdhoc;UID=AndrePra;PWD=rhianramos')
conn = pyodbc.connect(connStr)
cursor = conn.cursor()

sqlquery = """if Exists (select * from information_schema.tables where table_name like 'Adhoc_Query_Select_Python_raw')
drop table SourceDataAdhoc.dbo.Adhoc_Query_Select_Python_raw

create table SourceDataAdhoc.dbo.Adhoc_Query_Select_Python_raw(
[Nomor] varchar(10) null,
[Kolom] varchar(max) null,
)
on  [primary]
"""

cursor.execute(sqlquery)

sqlquery2 = """if Exists (select * from information_schema.tables where table_name like 'Adhoc_Query_From_Python_raw')
drop table SourceDataAdhoc.dbo.Adhoc_Query_From_Python_raw

create table SourceDataAdhoc.dbo.Adhoc_Query_From_Python_raw(
[tables] varchar(max) null,
)
on  [primary]
"""

cursor.execute(sqlquery2)

for index, row in selectjoin.iterrows():
    cursor.execute("INSERT INTO SourceDataAdhoc.dbo.Adhoc_Query_Select_Python_raw (Nomor,Kolom) values(?,?)", row.flag, row.Kolom_Final)


selectfrom = list[1]
selectfrom = selectfrom.replace(' ON ',' on ')
selectfrom = selectfrom.replace(' on ',' on ')
selectfrom = selectfrom.replace('WITH','with')
selectfrom = selectfrom.replace('(NOLOCK)','(nolock)')
selectfrom = selectfrom.replace('LEFT','left')
selectfrom = selectfrom.replace('RIGHT','right')
selectfrom = selectfrom.replace('INNER','inner')
selectfrom = selectfrom.replace('OUTER','outer')

selectfrom = selectfrom.replace('left','')
selectfrom = selectfrom.replace(' as ',' ')
selectfrom = selectfrom.replace(' AS ',' ')
selectfrom = selectfrom.replace('right','')
selectfrom = selectfrom.replace('inner','')
selectfrom = selectfrom.replace('outer','')
selectfrom = selectfrom.replace('with','')
selectfrom = selectfrom.replace('JOIN ','join ')
selectfrom = selectfrom.replace('(nolock)','')
#print(selectfrom)
fromitems = selectfrom.split(' join ')
from_df = pd.DataFrame(fromitems, columns=['tables_from'])

def table_from(from_df):
    temp = str(from_df['tables_from'])
    temp = re.split(" on ", temp, flags=re.IGNORECASE)[0]
    temp = re.split(" where ", temp, flags=re.IGNORECASE)[0]
    return temp.strip().replace(" ","] ")
from_df['tables_temp'] = from_df.apply(table_from, axis=1)

def table_temp2(from_df):
    if "." in from_df['tables_temp']:
        return from_df['tables_temp']
    else:
        return 'asd.'+from_df['tables_temp']

from_df['tables_temp'] = from_df.apply(table_temp2, axis=1)

def table_final(from_df):
    if "] " in from_df['tables_temp']:
        temp = str(from_df['tables_temp'])
        temp = temp.replace("] "," ")
        return temp
    else:
        return from_df['tables_temp'] + ' asd'

from_df['tables_from'] = from_df.apply(table_final, axis=1)

for index, row in from_df.iterrows():
    cursor.execute("INSERT INTO SourceDataAdhoc.dbo.Adhoc_Query_From_Python_raw (tables) values(?)", row.tables_from)

cursor.close()
conn.commit()
conn.close()

print(from_df)

