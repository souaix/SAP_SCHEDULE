#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import pyodbc

# connector
import connect.connect as cc
engine_mes, con_mes = cc.connect('MES', 'MES_Test')
# engine_cim, con_cim = cc.connect('CIM', 'SAP_WKTIME')
# engine_log, con_log = cc.connect('CIM', 'log')
engine_sap, con_sap = cc.connect('SAP', 'log')
cur=con_sap.cursor()


# In[13]:


# #呼叫其他程式 *要放cim2才可使用
# #我還沒試過
# import win32api
# win32api.shellexecute(0, 'open', 'C:\ProgramData\Anaconda3\python.exe', 'C:\xampp\htdocs\projects\SAP\scheduling\SAP_28.py','',1)


# In[2]:


BEGIN = '2023-08-01'
END = '2023-08-31'
MANDT = '240'


# In[3]:



def SAP_COWIPSTATE(BEGIN,END):
    #撈取指定MO過帳紀錄
    sql ="EXECUTE SAP_COWIPSTATE @BEGIN='"+BEGIN+"',@END='"+END+"'"
    df_shot = pd.read_sql(sql,engine_mes)

    return df_shot


# In[4]:


#取盤點現況
df_shot = SAP_COWIPSTATE(BEGIN,END)
#把在途工單分出
df_pack = df_shot[df_shot["STATUS"]==-1]
boxno = list(set(df_pack["BOXNO"].tolist()))
#組成字串
boxno_str=''
for i in boxno:
    boxno_str = boxno_str+"'"+i+"',"
boxno_str = boxno_str[:-1]
if boxno_str=="":
    boxno_str="''"


# In[5]:


#已拋但尚未入庫
sql ='''
	SELECT ZZCASSET_IPL,sum(BDMNG) AS BDMNG, '未入' AS MARK FROM 
	(SELECT IDBSNO,WERKS ,AUFNR ,GRSNO,BDMNG,ZZCASSET_IPL  FROM THSAP.ZPPT0026A WHERE WERKS='1031' AND MANDT=\''''+MANDT+'''\' AND ZZCASSET_IPL IN ('''+boxno_str+''')) A
	LEFT JOIN 
	(SELECT IDBSNO,WERKS ,AUFNR ,GRSNO,PDADAT,PDASTSTYP  FROM SAPS4.ZPPT0026B2 WHERE IDBSNO IN (SELECT MAX(IDBSNO) FROM SAPS4.ZPPT0026B2 GROUP BY AUFNR,GRSNO)) B2
	ON　A.IDBSNO=B2.IDBSNO AND A.WERKS=B2.WERKS AND A.AUFNR=B2.AUFNR AND A.GRSNO=B2.GRSNO 
	INNER JOIN
	(SELECT AUFNR,DISPO FROM SAPS4.ZPPT0024A WHERE DISPO ='3AA' AND WERKS='1031') A24
	ON A.AUFNR=A24.AUFNR
	INNER JOIN 
	(SELECT AUFNR,STSTYP FROM SAPS4.ZPPT0024D1 WHERE WERKS='1031' AND (STSTYP <> 'D' OR STSTYP IS NULL) )D124
	ON A.AUFNR=D124.AUFNR	
	WHERE PDADAT IS NULL GROUP BY ZZCASSET_IPL
'''


df_26_yn = pd.read_sql(sql,con_sap)
#已拋且已入庫(MES待入庫) *並且沒有退庫

sql ='''
	SELECT ZZCASSET_IPL,sum(BDMNG) AS BDMNG, '未入' AS MARK FROM 
	(SELECT IDBSNO,WERKS ,AUFNR ,GRSNO,BDMNG,ZZCASSET_IPL  FROM THSAP.ZPPT0026A WHERE WERKS='1031' AND MANDT=\''''+MANDT+'''\' AND ZZCASSET_IPL IN ('''+boxno_str+''')) A
	LEFT JOIN 
	(SELECT IDBSNO,WERKS ,AUFNR ,GRSNO,PDADAT,PDASTSTYP  FROM SAPS4.ZPPT0026B2 WHERE IDBSNO IN (SELECT MAX(IDBSNO) FROM SAPS4.ZPPT0026B2 GROUP BY AUFNR,GRSNO)) B2
	ON　A.IDBSNO=B2.IDBSNO AND A.WERKS=B2.WERKS AND A.AUFNR=B2.AUFNR AND A.GRSNO=B2.GRSNO 
	INNER JOIN
	(SELECT AUFNR,DISPO FROM SAPS4.ZPPT0024A WHERE DISPO ='3AA' AND WERKS='1031') A24
	ON A.AUFNR=A24.AUFNR
	INNER JOIN 
	(SELECT AUFNR,STSTYP FROM SAPS4.ZPPT0024D1 WHERE WERKS='1031' AND (STSTYP <> 'D' OR STSTYP IS NULL) )D124
	ON A.AUFNR=D124.AUFNR	
	WHERE PDADAT IS NOT NULL AND PDASTSTYP ='F' GROUP BY ZZCASSET_IPL
'''
df_26_yy = pd.read_sql(sql,con_sap)

df_26 = pd.concat([df_26_yn,df_26_yy])

#將已拋未入、已拋已入，合併主表再拆分出來
df = df_shot.merge(df_26,left_on=['BOXNO'],right_on=['ZZCASSET_IPL'],how='left')

df_未入 = df[df["MARK"]=="未入"]
df_已入 = df[df["MARK"]=="已入"]

df = df[df["MARK"] != "未入"]
df = df[df["MARK"] != "已入"]

#未拋入庫通知
df_未拋 = df[df["BOXNO"].notna()]
df_未拋 = df_未拋[df_未拋["ZZCASSET_IPL"].isna()]
df_未拋["MARK"] = "未拋"

#主表不留在途
df = df[df["BOXNO"].isna()]

#未入的數量換置"入庫單未結數量"
df_未入 = df_未入.copy()
df_未入["入庫單未結數量"] = df_未入["可入庫數量"]
df_未入["可入庫數量"] = 0

#已入的數量換置"入庫數量" (MES未入但實際SAP已入)
df_已入 = df_已入.copy()
df_已入["入庫數量"] = df_已入["可入庫數量"]
df_已入["可入庫數量"] = 0
df_已入["STATUS"]=-2
df_已入["OPNO"]='入庫'

#因單批會多箱號，故移除重複箱號但不同狀態的資料
df_未入.drop_duplicates(subset =['工單狀態','工單','料號','產品線','CLASS CODE','ERP工單狀態','下線數量','入庫數量','報廢數量','未完工數量','可入庫數量','入庫單未結數量','BD FLAG','投料過站','WIP產生時間','OPNO','MARK'] ,inplace = True)
df_已入.drop_duplicates(subset =['工單狀態','工單','料號','產品線','CLASS CODE','ERP工單狀態','下線數量','入庫數量','報廢數量','未完工數量','可入庫數量','入庫單未結數量','BD FLAG','投料過站','WIP產生時間','OPNO','MARK'] ,inplace = True)

#各狀態by工單相加數量，再合併成主表
df = df.groupby(by=['工單狀態','工單','料號','產品線','CLASS CODE','ERP工單狀態','下線數量','BD FLAG','投料過站','WIP產生時間'], as_index=False).sum()
df_未入 = df_未入.groupby(by=['工單狀態','工單','料號','產品線','CLASS CODE','ERP工單狀態','下線數量','BD FLAG','投料過站','WIP產生時間'], as_index=False).sum()
df_已入 = df_已入.groupby(by=['工單狀態','工單','料號','產品線','CLASS CODE','ERP工單狀態','下線數量','BD FLAG','投料過站','WIP產生時間'], as_index=False).sum()
df_未拋 = df_未拋.groupby(by=['工單狀態','工單','料號','產品線','CLASS CODE','ERP工單狀態','下線數量','BD FLAG','投料過站','WIP產生時間'], as_index=False).sum()
df = pd.concat([df,df_未入,df_已入,df_未拋])
#留下需要欄位
df = df[['工單狀態','工單','料號','產品線','CLASS CODE','ERP工單狀態','下線數量','入庫數量','報廢數量','未完工數量','可入庫數量','入庫單未結數量','BD FLAG','投料過站','WIP產生時間']]
df_工單現況表 = df.copy()


# In[6]:


df_完工清單 = df[df["未完工數量"]>0]


# In[7]:


#報工失敗清單
sql='''
SELECT MAIN.WERKS AS 廠別, MAIN.AUFNR AS 工單,MESSAGE AS 錯誤說明,MAIN.VORNR AS 錯誤站別  FROM
(SELECT * FROM SAPS4.ZPPT0025C2 WHERE WERKS ='1031' AND (GETDAT >= TO_DATE(\''''+BEGIN+'''\','YYYY-MM-DD') AND GETDAT <= TO_DATE(\''''+END+'''\','YYYY-MM-DD') ) AND MANDT=\''''+MANDT+'''\' AND IDBSNO IN (SELECT MAX(IDBSNO) FROM SAPS4.ZPPT0025C2 GROUP BY AUFNR)) MAIN
LEFT JOIN
(SELECT AUFNR,DISPO FROM SAPS4.ZPPT0024A WHERE DISPO ='3AA' AND WERKS='1031') A24
ON MAIN.AUFNR=A24.AUFNR
INNER JOIN 
(SELECT AUFNR,STSTYP FROM SAPS4.ZPPT0024D1 WHERE WERKS='1031' AND (STSTYP <> 'D' OR STSTYP IS NULL) )D124
ON MAIN.AUFNR=D124.AUFNR	
LEFT JOIN
(SELECT IDBSNO,AUFNR,VORNR,MESSAGE FROM SAPS4.ZPPT0025D WHERE WERKS='1031') D25
ON MAIN.IDBSNO=D25.IDBSNO AND MAIN.AUFNR=D25.AUFNR AND MAIN.VORNR=D25.VORNR
WHERE DISPO IS NOT NULL AND MAIN.STSTYP='E'
'''
df_fail = pd.read_sql(sql,con_sap)


# In[8]:


aufnr_list = list(set(df_fail["工單"].tolist()))
#組成字串
aufnr_str=''
for i in aufnr_list:
    aufnr_str = aufnr_str+"'"+i+"',"
aufnr_str = aufnr_str[:-1]
if aufnr_str=="":
    aufnr_str="''"


# In[9]:


#MES資訊
sql ='''
SELECT AUFNR,PRD.PRODUCTNO AS 料號,PRD.DESCRIPTION as 料號說明,PSNO AS 'CLASS CODE',
(CASE WHEN UNITNO = 'EAC' THEN DICEQTY ELSE MOQTY END) AS '下線數量'
FROM
(SELECT AUFNR,PRODUCTNO,PSNO,DICEQTY,MOQTY FROM TBLOEMOBASIS WHERE AUFNR IN ('''+aufnr_str+''')) AS OEMO
LEFT JOIN
(SELECT PRODUCTNO,DESCRIPTION,UNITNO from TBLPRDPRODUCTBASIS) AS PRD
on OEMO.PRODUCTNO=PRD.PRODUCTNO
'''

df_mo = pd.read_sql(sql,engine_mes)


# In[10]:


df_fail = df_fail.merge(df_mo,left_on=['工單'],right_on=['AUFNR'],how='left')
df_fail["錯誤訊息"]=""
df_fail["ERP工單狀態"]=""
df_報工失敗清單 = df_fail[['廠別', '工單', '錯誤訊息', '錯誤說明', '錯誤站別','CLASS CODE','ERP工單狀態','下線數量']]


# In[14]:


writer = pd.ExcelWriter("CO_工單現況表.xlsx")
df_工單現況表.to_excel(writer, sheet_name="MES工單現況表",index=False)
df_完工清單.to_excel(writer, sheet_name="MES完工清單",index=False)
df_報工失敗清單.to_excel(writer, sheet_name="MES報工失敗清單",index=False)
writer.save()
writer.close()


# In[ ]:





# In[ ]:




