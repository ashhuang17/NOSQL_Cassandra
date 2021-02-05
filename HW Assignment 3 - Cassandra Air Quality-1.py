#!/usr/bin/env python
# coding: utf-8

# In[3]:


from cassandra.cluster import Cluster
import json


# In[4]:


host = 'ec2-54-67-89-95.us-west-1.compute.amazonaws.com'
port = 9220

cluster = Cluster([host], port = port)
session = cluster.connect()
session.set_keyspace('nosql')


# In[5]:


#Q1

print('Q1 - How many total records are there in this air_quality table ?')
a1 = session.execute('select count(*) from air_quality;')
for row in a1:
    print('Answer - The total records in the table are', row[0])


# In[6]:


#Q2

print('Q2 - What was the air quality metric value for MeasureId 83 in the Sandoval county in the state of New Mexico in 2003 ?')

q2 = session.prepare('select value from air_quality where measureid =? and countyname =? and statename=? and reportyear =?')
a2 = session.execute(q2, [83,'Sandoval','New Mexico',2003])

for row in a2:
    print('Answer - The air quality value:', row[0])


# In[7]:


#Q3

print('Q3 - Provide the name of the state, county, year, and metric value of the highest recorded air quality metric value of MeasureId 87 ?')


q3 = session.prepare('select statename, countyname, reportyear, max(value) from air_quality where measureid =?')
a3 = session.execute(q3, [87])

for row in a3:
    print('Answer - The State name:', row[0])
    print('         The County name:', row[1])
    print('         The report year:', row[2])
    print('         The maxinum value:', row[3])
    


# In[8]:


#Q4

print('Q4 - What were the average of air quality value and the unit for MeasureId 84, State - Massachusetts, CountyName - Barnstable, for years between 2002 - 2008 ?')

q4 = session.prepare('select avg(value), unit from air_quality where measureid =? and statename=? and countyname =? and reportyear in (2002,2003,2004,2005,2006,2007,2008)')
a4 = session.execute(q4, [84,'Massachusetts','Barnstable'])

for row in a4:
    print('Answer - The average value:', row[0])
    print('         The unit:', row[1])


# In[9]:


#Q5

print('Q5 - What was the average of air quality value for MeasureId - 84, State - Massachusetts, for years between 2002 - 2008 ?')

q5 = session.prepare('select avg(value) from air_quality where measureid =? and statename =? and reportyear >= 2002 and reportyear <= 2008')
a5 = session.execute(q5, [84, 'Massachusetts'])

for row in a5:
    print('Answer -  The average value:', row[0])


# In[10]:


#Q6

print('Q6 - What was the average of air quality value for MeasureId - 84, State - Massachusetts ?')

q6 = session.prepare('select avg(value) from air_quality where measureid =? and statename=?')
a6 = session.execute(q6, [84, 'Massachusetts'])

for row in a6:
    print('Answer - The average value:', row[0])


# In[17]:


#Q7

print('Q7 - What kind of data query limitations does this table structure impose in analyzing the air quality data ?')

a7 = session.execute('select * from air_quality_3PK_1CK limit 1')

for row in a7:
    print(row)


# In[24]:


print('Answer: For the table air_quality_3pk_1ck, we can see that the structure is set to PRIMARY KEY ((measureid, statename, reportyear), countyname).')
print('Which means that this table is structured with multi-column partition key.') 
print('We have 3 columns - measureid, statename and reportyear grouped together as 3 partition keys that followed by countyname as the clustering column where it is singled out from the 3.')
print('This indicates that the 3 partition keys we have in the table air_quality_3pk_1ck must be contained in all queries of data in this table for analysis, which is the limitation in the query rule')
print('Unlike the previous table air_quality, we have (PRIMARY KEY ((measureid, statename, reportyear, countyname)) where everything is in one paretheses.')
print('This means that this table is structured with partition key with clustering columns. In this case, the measureid is the only partition key here, and statename,reportyear, countyname are clustering columns.')
print('Therefore, it is only required to use measureid in all the queries.')


# In[ ]:




