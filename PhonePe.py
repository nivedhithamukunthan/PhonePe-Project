import streamlit as st
import plotly.express as px
import os
import pandas as pd
import json
import mysql.connector
import requests
import numpy as np
            

#Sql Connection
connection=mysql.connector.connect(host="localhost",user="root",password="12345",db="phonepe_project")
mycursor=connection.cursor()


#Extraction of Data and converting to DF
#Aggregated transaction

path_1="C:/Users/mukun/Desktop/Anu's Data Science/VS/PhonePe/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path_1)


Agg_trans_cols={'State':[], 'Year':[],'Quater':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in Agg_state_list:
    p_i=path_1+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              Agg_trans_cols['Transaction_type'].append(Name)
              Agg_trans_cols['Transaction_count'].append(count)
              Agg_trans_cols['Transaction_amount'].append(amount)
              Agg_trans_cols['State'].append(i)
              Agg_trans_cols['Year'].append(j)
              Agg_trans_cols['Quater'].append(int(k.strip('.json')))
              

df_aggregated_transaction=pd.DataFrame(Agg_trans_cols)

df_aggregated_transaction["State"]=df_aggregated_transaction['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_aggregated_transaction["State"]=df_aggregated_transaction['State'].str.replace("-"," ")
df_aggregated_transaction['State']=df_aggregated_transaction['State'].str.title()
df_aggregated_transaction['State']=df_aggregated_transaction['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli and Daman and Diu')


#***************************************Aggregated User*****************

#Aggregated User
path_2 = "C:/Users/mukun/Desktop/Anu's Data Science/VS/PhonePe/pulse/data/aggregated/user/country/india/state/"
Agg_user_state_list = os.listdir(path_2)

Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'User_Count': [], 'User_Percentage': []}

for i in Agg_user_state_list:
    p_i = path_2 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            B = json.load(Data)
            
            try:
                for l in B["data"]["usersByDevice"]:
                    brand_name = l["brand"]
                    count_ = l["count"]
                    ALL_percentage = l["percentage"]
                    Agg_user["State"].append(i)
                    Agg_user["Year"].append(j)
                    Agg_user["Quarter"].append(int(k.strip('.json')))
                    Agg_user["Brands"].append(brand_name)
                    Agg_user["User_Count"].append(count_)
                    Agg_user["User_Percentage"].append(ALL_percentage*100)
            except:
                pass

df_aggregated_user = pd.DataFrame(Agg_user)
df_aggregated_user["State"]=df_aggregated_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_aggregated_user["State"]=df_aggregated_user['State'].str.replace("-"," ")
df_aggregated_user['State']=df_aggregated_user['State'].str.title()
df_aggregated_user['State']=df_aggregated_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli and Daman and Diu')

#**********************************#Map Transaction****************************
#Map Transaction
path_3="C:/Users/mukun/Desktop/Anu's Data Science/VS/PhonePe/pulse/data/map/transaction/hover/country/india/state/"
map_state_trans=os.listdir(path_3)

map_trans_col={'State':[],'Year':[],'Quarter':[],'District_Name':[],'Count':[],'Amount':[]}

for state in map_state_trans:
    map_state_path=path_3+state+"/"
    map_year_trans=os.listdir(map_state_path)

    for year in map_year_trans:
        map_path=map_state_path+year+"/"
        map_json_trans=os.listdir(map_path)

        for json_datas in map_json_trans:
            json_path=map_path+json_datas
            Data=open(json_path,'r')
            J=json.load(Data)

            try:
                for m in J['data']['hoverDataList']:
                    dname=m['name']
                    dcount=m['metric'][0]['count']
                    damount=m['metric'][0]['amount']
                    map_trans_col["State"].append(state)
                    map_trans_col["Year"].append(year)
                    map_trans_col["Quarter"].append(json_datas.strip(".json"))
                    map_trans_col['District_Name'].append(dname)
                    map_trans_col['Count'].append(dcount)
                    map_trans_col['Amount'].append(damount)
            except:
                pass
        

df_map_transaction=pd.DataFrame(map_trans_col)

df_map_transaction["State"]=df_map_transaction['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_map_transaction["State"]=df_map_transaction['State'].str.replace("-"," ")
df_map_transaction['State']=df_map_transaction['State'].str.title()
df_map_transaction['State']=df_map_transaction['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli and Daman and Diu')


#**************************************Map user*************************************

#Map user
path_4="C:/Users/mukun/Desktop/Anu's Data Science/VS/PhonePe/pulse/data/map/user/hover/country/india/state/"
map_user_state=os.listdir(path_4)

map_user_col = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_User": []}

for state in map_user_state:
    map_state=path_4+state+"/"
    map_user_year=os.listdir(map_state)

    for year in map_user_year:
        map_year_path=map_state+year+"/"
        map_user_json=os.listdir(map_year_path)

        for jsonvalues in map_user_json:
            json_path=map_year_path+jsonvalues
            Data=open(json_path,'r')
            D=json.load(Data)
            try:
                for k in D['data']['hoverData'].items():
                    district=k[0]
                    reg_users=k[1]['registeredUsers']
                    map_user_col["State"].append(state)
                    map_user_col["Year"].append(year)
                    map_user_col["Quarter"].append(jsonvalues.strip(".json"))
                    map_user_col["District"].append(district)
                    map_user_col["Registered_User"].append(reg_users)
                    
            except:
                pass
df_map_user=pd.DataFrame(map_user_col)
df_map_user["State"]=df_map_user['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_map_user["State"]=df_map_user['State'].str.replace("-"," ")
df_map_user['State']=df_map_user['State'].str.title()
df_map_user['State']=df_map_user['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli and Daman and Diu')

#***************************************#Top Transaction*******************************************
#Top Transaction
path_5="C:/Users/mukun/Desktop/Anu's Data Science/VS/PhonePe/pulse/data/top/transaction/country/india/state/"
top_trans_states=os.listdir(path_5)

top_trans_cols={'State':[],'Year':[],'Quarter':[],'District_Name':[],'Count':[],'Amount':[]}

for states in top_trans_states:
    states_path=path_5+states+"/"
    top_trans_years=os.listdir(states_path)

    for years in top_trans_years:
        years_path=states_path+years+"/"
        top_json_values=os.listdir(years_path)

        for json_values in top_json_values:
            top_json_path=years_path+json_values
            Data=open(top_json_path,'r')
            Top_d=json.load(Data)

            try:
                for T in Top_d['data']['districts']:
                    district_name=T['entityName']
                    top_count=T['metric']['count']
                    top_amount=T['metric']['amount']
                    top_trans_cols['District_Name'].append(district_name)
                    top_trans_cols['Count'].append(top_count)
                    top_trans_cols['Amount'].append(top_amount)
                    top_trans_cols['State'].append(states)
                    top_trans_cols['Year'].append(years)
                    top_trans_cols['Quarter'].append(json_values.strip(".json"))


            except:
                pass

df_top_trans=pd.DataFrame(top_trans_cols)
df_top_trans["State"]=df_top_trans['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_top_trans["State"]=df_top_trans['State'].str.replace("-"," ")
df_top_trans['State']=df_top_trans['State'].str.title()
df_top_trans['State']=df_top_trans['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli and Daman and Diu')

#**********************************************#Top User*****************************************************

#Top User
path_6="C:/Users/mukun/Desktop/Anu's Data Science/VS/PhonePe/pulse/data/top/user/country/india/state/"
top_users_states=os.listdir(path_6)

top_users_cols={'State':[],'Year':[],'Quarter':[],'District_Name':[],'Registered_Users':[]}

for states in top_users_states:
    states_path=path_6+states+"/"
    top_users_years=os.listdir(states_path)

    for years in top_users_years:
        years_path=states_path+years+"/"
        top_json_values=os.listdir(years_path)

        for json_values in top_json_values:
            top_json_path=years_path+json_values
            Data=open(top_json_path,'r')
            Top_u=json.load(Data)

            try:
                for T in Top_u['data']['districts']:
                    district_name=T['name']
                    top_reg_users=T['registeredUsers']

                    top_users_cols['District_Name'].append(district_name)
                    top_users_cols['Registered_Users'].append(top_reg_users)
                    top_users_cols['State'].append(states)
                    top_users_cols['Year'].append(years)
                    top_users_cols['Quarter'].append(json_values.strip(".json"))


            except:
                pass

df_top_users=pd.DataFrame(top_users_cols)
df_top_users["State"]=df_top_users['State'].str.replace('andaman-&-nicobar-islands','Andaman & Nicobar')
df_top_users["State"]=df_top_users['State'].str.replace("-"," ")
df_top_users['State']=df_top_users['State'].str.title()
df_top_users['State']=df_top_users['State'].str.replace('dadra-&-nagar-haveli-&-daman-&-diu','Dadra and Nagar Haveli and Daman and Diu')

#********************************************************************************************************
#Aggregate Transaction
#Inserting from df to SQL


import mysql.connector
connection=mysql.connector.connect(host="localhost",user="root",password="12345",db="phonepe_project")
mycursor=connection.cursor()

#Creating a table
#Agg_trans_cols={'State':[], 'Year':[],'Quater':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}
try:
    query_create="create table  AggTrans2(State varchar(300),Year int,Quater int,Transaction_type varchar(300),Transaction_count bigint,Transaction_amount bigint)"
    mycursor.execute(query_create)
    connection.commit()
    print("Table inserted successfully")



    query_1='''insert into AggTrans2(State,Year,Quater,Transaction_type,Transaction_count,Transaction_amount)
                        values(%s,%s,%s,%s,%s,%s)'''
    data1=df_aggregated_transaction.values.tolist()
    mycursor.executemany(query_1,data1)
    connection.commit()

except Exception as es:
    print(es)

#****************************************************************************************************
    
#Aggregate User:Creation of table into Sql

try:
    query_create="create table AggUser(State varchar(300),Year int,Quater int,Brands varchar(300),User_Count bigint,User_Percentage bigint)"
    mycursor.execute(query_create)
    connection.commit()
    print("Table created")

    #Insertion of datas
    query_2='''insert into AggUser(State,Year,Quater,Brands,User_Count,User_Percentage)
            values(%s,%s,%s,%s,%s,%s)'''

    data2=df_aggregated_user.values.tolist()
    mycursor.executemany(query_2,data2)
    connection.commit()
    print("Datas inserted successfully")
except Exception as es:
    print(es)


#***************************************************************************************************

#Map transaction
try:
    query_create="create table MapTrans(State varchar(300),Year int,Quater int,District_Name varchar(300),Count bigint,Amount bigint)"
    mycursor.execute(query_create)
    connection.commit()
    print("Table created")

    #Insertion of Datas
    query_3='''insert into MapTrans(State,Year,Quater,District_Name,Count,Amount)
            values(%s,%s,%s,%s,%s,%s)'''

    data3=df_map_transaction.values.tolist()
    mycursor.executemany(query_3,data3)
    connection.commit()
    print("Datas inserted successfully")

except Exception as es:
    print(es)

#*************************************************************************************************

#Table for MapUser

try:
    query_create="create table MapUser(State varchar(300),Year int,Quater int,District varchar(300),Registered_User bigint)"
    mycursor.execute(query_create)
    connection.commit()
    print("Table created")


    query_4='''insert into MapUser(State,Year,Quater,District,Registered_User)
            values(%s,%s,%s,%s,%s)'''
    data4=df_map_user.values.tolist()
    mycursor.executemany(query_4,data4)
    connection.commit()
    print("Datas inserted successfully")

except Exception as es:
    print(es)



#***********************************************************************************************
    
#Table for top transaction
try:
    query_create="create table TopTrans(State varchar(300),Year int,Quater int,District_Name varchar(300),Count bigint,Amount bigint)"
    mycursor.execute(query_create)
    connection.commit()
    print("Table created")

    query_5='''insert into TopTrans(State,Year,Quater,District_Name,Count,Amount)
            values(%s,%s,%s,%s,%s,%s)'''
    data5=df_top_trans.values.tolist()
    mycursor.executemany(query_5,data5)
    connection.commit()
    print("Datas inserted successfully")
except Exception as es:
    print(es) 

#***********************************************************************************************
    
#Top User

try:
    query_create="create table TopUser(State varchar(300),Year int,Quater int,District_Name varchar(300),Registered_Users bigint)"
    mycursor.execute(query_create)
    connection.commit()
    print("Table created")

    query_6='''insert into TopUser(State,Year,Quater,District_Name,Registered_Users)
            values(%s,%s,%s,%s,%s)'''
    data6=df_top_users.values.tolist()
    mycursor.executemany(query_6,data6)
    connection.commit()
    print("Datas inserted successfully")

except Exception as es:
    print(es)


#**********************************************************************************************

#Streamlit Application
with st.sidebar:
    st.title(":violet[Phonepe Pulse Data Visualization and Exploration:A User-Friendly Tool Using Streamlit and Plotly]")
    
tab_title=[":violet[Aggregated]",
        ":violet[Map]",
        ":violet[Top]"]
tabs=st.tabs(tab_title)

with tabs[0]:
    #**************Aggregated trans/user********************
    #**************Aggregated Transaction********************
    
    dropdown_select=["Transaction","User"]
    
    select_agg=st.selectbox("Select to display...",dropdown_select)
    if select_agg=="Transaction":
        dropdown_state=['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chandigarh',
                        'Chhattisgarh','Dadra & Nagar Haveli & Daman & Diu','Delhi','Goa','Gujarat','Haryana',
                        'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Ladakh','Lakshadweep',
                        'Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry',
                        'Punjab','Rajasthan','Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal']
        state_agg=st.selectbox("State",dropdown_state)
        dropdown_year=['2018','2019','2020','2021','2022','2023']
        year_agg=st.selectbox("Year",dropdown_year)
        dropdown_quarter=['1','2','3','4']
        quarter_agg=st.selectbox("Quarter",dropdown_quarter)
        #col1,col2=st.columns([0.2,0.2])
        
        if st.button("Display Datas"):
                query1=f"select * from phonepe_project.aggtrans2 where Year='{year_agg}' and State='{state_agg}' and Quater='{quarter_agg}'"
                mycursor.execute(query1)
                res1=mycursor.fetchall()
                df1=pd.DataFrame(res1,columns=["State","Year","Quarter","Transaction Type","Transaction Count","Transaction Amount"])
                st.write(df1)
        #Geo Visualisation
        #Conversion to DF
        dropdown_categories=['Recharge & bill payments','Peer-to-peer payments','Merchant payments','Financial Services','Others']
        categories=st.selectbox("Select categories",dropdown_categories)
        #col1,col2=st.columns([0.2,0.2])
        #with col1:
        if st.button("Geo-Visualization"):
            mycursor.execute(f"select State,Transaction_amount from phonepe_project.aggtrans2 WHERE Year = '{year_agg}' AND Quater = '{quarter_agg}' AND Transaction_type = '{categories}';")
            agg_trans = mycursor.fetchall()
            df_agg_trans = pd.DataFrame(np.array(agg_trans), columns=['State', 'Transaction_amount'])
            df_agg_trans1 = df_agg_trans.set_index(pd.Index(range(1, len(df_agg_trans)+1)))
        
        #India map
        
            df_agg_trans.drop(columns=['State'], inplace=True)
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data1 = json.loads(response.content)
            state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
            state_names_tra.sort()
            df_state_names_tra = pd.DataFrame({'State': state_names_tra})
            df_state_names_tra['Transaction_amount']=df_agg_trans
            df_state_names_tra.to_csv('State_trans.csv', index=False)
            df_tra = pd.read_csv('State_trans.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Transaction_amount',color_continuous_scale='thermal',title = 'Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_tra,use_container_width=True)
            #fig_tra.show()
        #with col2:
        if st.button("Explore Datas for Aggregated Transaction"):
            mycursor.execute(f"select * from phonepe_project.aggtrans2 WHERE Year = '{year_agg}' AND Quater = '{quarter_agg}' order by Transaction_amount desc limit 10;")
            trans_insight = mycursor.fetchall()
            df_trans_insight = pd.DataFrame(trans_insight, columns=['State','Year','Quarter','Transaction_type','Transaction_count','Transaction_amount'])
            st.text(f"Top 10 States with Maximum Transactions in Quarter {quarter_agg} and Year {year_agg} !!!!!!!")
            st.write(df_trans_insight)
    
    #************************Aggregated user***************************
    if select_agg=="User":
        dropdown_state=['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chandigarh',
                        'Chhattisgarh','Dadra & Nagar Haveli & Daman & Diu','Delhi','Goa','Gujarat','Haryana',
                        'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Ladakh','Lakshadweep',
                        'Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry',
                        'Punjab','Rajasthan','Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal']
        state_user=st.selectbox("State",dropdown_state)
        dropdown_year=['2018','2019','2020','2021','2022','2023']
        year_user=st.selectbox("Year",dropdown_year)
        dropdown_quarter=['1','2','3','4']
        quarter_user=st.selectbox("Quarter",dropdown_quarter)
        if st.button("Display Datas"):
                query2=f"SELECT * FROM phonepe_project.agguser where State='{state_user}' and quater='{quarter_user}' and Year='{year_user}';'"
                mycursor.execute(query2)
                res2=mycursor.fetchall()
                df2=pd.DataFrame(res2,columns=["State","Quarter","Year","Brands","User count","User percentage"])
                st.write(df2)
        #Geo Visualisation
        #Conversion to DF
        if st.button("Geo-Visualization"):
            mycursor.execute(f"SELECT State,SUM(User_Count) FROM phonepe_project.agguser WHERE Year = '{year_user}' AND Quater = '{quarter_user}' group by State;")
            in_us_co_qry_rslt = mycursor.fetchall()
            df_in_us_tab_qry_rslt = pd.DataFrame(np.array(in_us_co_qry_rslt), columns=['State','Sum'])
            df_in_us_co_qry_rslt1 = df_in_us_tab_qry_rslt.set_index(['State'])
            #India map
            
            df_in_us_tab_qry_rslt.drop(columns=['State'], inplace=True)
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data2 = json.loads(response.content)
            state_names_use = [feature['properties']['ST_NM'] for feature in data2['features']]
            state_names_use.sort()
            df_state_names_use = pd.DataFrame({'State': state_names_use})
            df_state_names_use['Sum']=df_in_us_tab_qry_rslt
            df_state_names_use.to_csv('State_user1.csv', index=False)
            df_use = pd.read_csv('State_user1.csv')
            # Geo plot
            fig_use = px.choropleth(
                df_use,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Sum',color_continuous_scale='thermal',title = 'User Analysis')
            fig_use.update_geos(fitbounds="locations", visible=False)
            fig_use.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_use,use_container_width=True)
            #fig_use.show()
        if st.button("Explore Datas for Aggregated User"):
            mycursor.execute(f"select * from phonepe_project.agguser WHERE Year = '{year_user}' AND Quater = '{quarter_user}' order by User_Count desc limit 10;")
            agg_user_insight = mycursor.fetchall()
            df_agg_user_insight = pd.DataFrame(agg_user_insight, columns=['State','Year','Quarter','Brand','User Count','User Percentage'])
            st.text(f"Top 10 States with Maximum number of Users in Quarter {quarter_user} and Year {year_user} !!!!!!!")
            st.write(df_agg_user_insight)
 #************************* MAP transaction********************************
with tabs[1]:
    
    map_dropdown_select=["Transaction","User"]
    select_map=st.selectbox("Select to display...",map_dropdown_select,key = "1")
    if select_map=="Transaction":
        map_dropdown_state=['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chandigarh',
                        'Chhattisgarh','Dadra & Nagar Haveli & Daman & Diu','Delhi','Goa','Gujarat','Haryana',
                        'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Ladakh','Lakshadweep',
                        'Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry',
                        'Punjab','Rajasthan','Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal']
        map_state=st.selectbox("State",map_dropdown_state,key = "<uniquevalueofsomesort>")
        map_dropdown_year=['2018','2019','2020','2021','2022','2023']
        map_year=st.selectbox("Year",map_dropdown_year,key="2")
        map_dropdown_quarter=['1','2','3','4']
        map_quarter=st.selectbox("Quarter",map_dropdown_quarter,key = "3")
        if st.button("Display Datas",key="1a"):
                query3=f"select * from phonepe_project.maptrans where Year='{map_year}' and State='{map_state}' and Quater='{map_quarter}'"
                mycursor.execute(query3)
                res3=mycursor.fetchall()
                df3=pd.DataFrame(res3,columns=["State","Year","Quarter","District Name","Transaction Count","Transaction Amount"])
                st.write(df3)
        #Geo Visualisation
        #Conversion to DF
       
        if st.button("Geo-Visualization",key="2a"):
            mycursor.execute(f"select State,SUM(Amount) from phonepe_project.maptrans WHERE Year = '{map_year}' AND Quater = '{map_quarter}' group by State;")
            map_trans = mycursor.fetchall()
            df_map_trans = pd.DataFrame(np.array(map_trans), columns=['State', 'Transaction Amount'])
            df_map_trans1 = df_map_trans.set_index(pd.Index(range(1, len(df_map_trans)+1)))
            
            #India map
            
            df_map_trans.drop(columns=['State'], inplace=True)
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data3 = json.loads(response.content)
            state_names_tra = [feature['properties']['ST_NM'] for feature in data3['features']]
            state_names_tra.sort()
            df_state_names_tra = pd.DataFrame({'State': state_names_tra})
            df_state_names_tra['Amount']=df_map_trans
            df_state_names_tra.to_csv('State_map.csv', index=False)
            df_tra = pd.read_csv('State_map.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Amount',color_continuous_scale='thermal',title = 'Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_tra,use_container_width=True)
            #fig_tra.show()

        if st.button("Explore Datas for Map Transaction"):
            mycursor.execute(f"select * from phonepe_project.maptrans WHERE Year = '{map_year}' AND Quater = '{map_quarter}' order by Amount desc limit 10;")
            map_trans_insight = mycursor.fetchall()
            df_map_trans_insight= pd.DataFrame(map_trans_insight, columns=['State','Year','Quarter','District Name','Transaction_count','Transaction_amount'])
            st.text(f"Top 10 States with Maximum Transactions in the Quarter {map_quarter} and Year {map_year} !!!!!!!")
            st.write(df_map_trans_insight)

            
            #*****************************Map User********************************************
    #map_dropdown_select=["Transaction","User"]
    #select_map=st.selectbox("Select to display...",map_dropdown_select,key = "1u")
    if select_map=="User":
        map_dropdown_state=['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chandigarh',
                        'Chhattisgarh','Dadra & Nagar Haveli & Daman & Diu','Delhi','Goa','Gujarat','Haryana',
                        'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Ladakh','Lakshadweep',
                        'Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry',
                        'Punjab','Rajasthan','Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal']
        map_state=st.selectbox("State",map_dropdown_state,key = "2u")
        map_dropdown_year=['2018','2019','2020','2021','2022','2023']
        map_year=st.selectbox("Year",map_dropdown_year,key="3u")
        map_dropdown_quarter=['1','2','3','4']
        map_quarter=st.selectbox("Quarter",map_dropdown_quarter,key = "4u")
        if st.button("Display Datas",key="5u"):
                query4=f"SELECT * FROM phonepe_project.mapuser where State='{map_state}'and Year='{map_year}' and Quater='{map_quarter}';"
                mycursor.execute(query4)
                res4=mycursor.fetchall()
                df4=pd.DataFrame(res4,columns=["State","Year","Quarter","District Name","Registered Users"])
                st.write(df4)
        if st.button("Geo-Visualization",key="6u"):
            mycursor.execute(f"select State,SUM(Registered_User) from phonepe_project.mapuser WHERE Year = '{map_year}' AND Quater = '{map_quarter}' group by State;")
            map_user = mycursor.fetchall()
            df_map_user = pd.DataFrame(np.array(map_user), columns=['State', 'Registered User'])
            df_map_user1 = df_map_user.set_index(pd.Index(range(1, len(df_map_user)+1)))

            #India Map
            df_map_user.drop(columns=['State'], inplace=True)
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data4 = json.loads(response.content)
            state_names_tra = [feature['properties']['ST_NM'] for feature in data4['features']]
            state_names_tra.sort()
            df_state_names_tra = pd.DataFrame({'State': state_names_tra})
            df_state_names_tra['Registered_User']=df_map_user
            df_state_names_tra.to_csv('State_map_user.csv', index=False)
            df_tra = pd.read_csv('State_map_user.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Registered_User',color_continuous_scale='thermal',title = 'User Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_tra,use_container_width=True)
            #fig_tra.show()
        if st.button("Explore Datas for Map User"):
            mycursor.execute(f"select * from phonepe_project.mapuser WHERE Year = '{map_year}' AND Quater = '{map_quarter}' order by Registered_User desc limit 10;")
            agg_user_insight = mycursor.fetchall()
            df_agg_user_insight = pd.DataFrame(agg_user_insight, columns=['State','Year','Quarter','User Count','Registered User'])
            st.text(f"Top 10 States with Maximum number of Users in Quarter {map_quarter} and Year {map_year} !!!!!!!")
            st.write(df_agg_user_insight)


    #****************************TOP TRANS*****************************************
with tabs[2]:
     top_dropdown_select=["Transaction","User"]
     select_top=st.selectbox("Select to display...",map_dropdown_select,key = "1t")

     if select_map=="Transaction":
        top_dropdown_state=['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chandigarh',
                        'Chhattisgarh','Dadra & Nagar Haveli & Daman & Diu','Delhi','Goa','Gujarat','Haryana',
                        'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Ladakh','Lakshadweep',
                        'Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry',
                        'Punjab','Rajasthan','Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal']
        top_state=st.selectbox("State",top_dropdown_state,key = "2t")
        top_dropdown_year=['2018','2019','2020','2021','2022','2023']
        top_year=st.selectbox("Year",top_dropdown_year,key="3t")
        top_dropdown_quarter=['1','2','3','4']
        top_quarter=st.selectbox("Quarter",top_dropdown_quarter,key = "4t")
        if st.button("Display Datas",key="5t"):
                query4=f"select * from phonepe_project.toptrans where Year='{top_year}' and State='{top_state}' and Quater='{top_quarter}'"
                mycursor.execute(query4)
                res4=mycursor.fetchall()
                df4=pd.DataFrame(res4,columns=["State","Year","Quarter","District Name","Transaction Count","Transaction Amount"])
                st.write(df4)
                
        #Geo Visualisation
        #Conversion to DF
       
        if st.button("Geo-Visualization",key="6t"):
            mycursor.execute(f"select State,SUM(Amount) from phonepe_project.toptrans WHERE Year = 2018 AND Quater = 1 group by State;")
            top_trans = mycursor.fetchall()
            df_top_trans = pd.DataFrame(np.array(top_trans), columns=['State', 'Transaction Amount'])
            
            #India map
            
            df_top_trans.drop(columns=['State'], inplace=True)
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data5 = json.loads(response.content)
            state_names_tra = [feature['properties']['ST_NM'] for feature in data5['features']]
            state_names_tra.sort()
            df_state_names_tra = pd.DataFrame({'State': state_names_tra})
            df_state_names_tra['Amount']=df_top_trans
            df_state_names_tra.to_csv('State_map.csv', index=False)
            df_tra = pd.read_csv('State_map.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Amount',color_continuous_scale='thermal',title = 'Transaction Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_tra,use_container_width=True)
            #fig_tra.show()

        if st.button("Explore Datas for Top Transaction"):
            mycursor.execute(f"select * from phonepe_project.toptrans WHERE Year = '{top_year}' AND Quater = '{top_quarter}' order by Amount desc limit 10;")
            top_trans_insight = mycursor.fetchall()
            df_top_trans_insight= pd.DataFrame(top_trans_insight, columns=['State','Year','Quarter','District Name','Transaction_count','Transaction_amount'])
            st.text(f"Top 10 States with Maximum Transactions in the Quarter {top_quarter} and Year {top_year} !!!!!!!")
            st.write(df_top_trans_insight)
    #*************************************TOP USER**********************************************
     if select_map=="User":
        topu_dropdown_state=['Andaman & Nicobar','Andhra Pradesh','Arunachal Pradesh','Assam','Bihar','Chandigarh',
                        'Chhattisgarh','Dadra & Nagar Haveli & Daman & Diu','Delhi','Goa','Gujarat','Haryana',
                        'Himachal Pradesh','Jammu & Kashmir','Jharkhand','Karnataka','Kerala','Ladakh','Lakshadweep',
                        'Madhya Pradesh','Maharashtra','Manipur','Meghalaya','Mizoram','Nagaland','Odisha','Puducherry',
                        'Punjab','Rajasthan','Sikkim','Tamil Nadu','Telangana','Tripura','Uttar Pradesh','Uttarakhand','West Bengal']
        topu_state=st.selectbox("State",topu_dropdown_state,key = "2tu")
        topu_dropdown_year=['2018','2019','2020','2021','2022','2023']
        topu_year=st.selectbox("Year",topu_dropdown_year,key="3tu")
        topu_dropdown_quarter=['1','2','3','4']
        topu_quarter=st.selectbox("Quarter",topu_dropdown_quarter,key = "4tu")
        if st.button("Display Datas",key="5tu"):
                query6=f"SELECT * FROM phonepe_project.mapuser where State='{map_state}'and Year='{map_year}' and Quater='{map_quarter}';"
                mycursor.execute(query6)
                res6=mycursor.fetchall()
                df6=pd.DataFrame(res6,columns=["State","Year","Quarter","District Name","Registered Users"])
                st.write(df6)
        #Geo Visualization
        if st.button("Geo-Visualization",key="6tu"):
            mycursor.execute(f"select State,SUM(Registered_Users) from phonepe_project.topuser WHERE Year = '{topu_year}' AND Quater = '{topu_quarter}' group by State;")
            top_user = mycursor.fetchall()
            df_top_user = pd.DataFrame(np.array(top_user), columns=['State', 'Registered User'])
            df_top_user1 = df_map_user.set_index(pd.Index(range(1, len(df_map_user)+1)))

            #India Map
            df_top_user.drop(columns=['State'], inplace=True)
            url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
            response = requests.get(url)
            data6 = json.loads(response.content)
            state_names_tra = [feature['properties']['ST_NM'] for feature in data6['features']]
            state_names_tra.sort()
            df_state_names_tra = pd.DataFrame({'State': state_names_tra})
            df_state_names_tra['Registered_User']=df_top_user
            df_state_names_tra.to_csv('State_map_user.csv', index=False)
            df_tra = pd.read_csv('State_map_user.csv')
            # Geo plot
            fig_tra = px.choropleth(
                df_tra,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',locations='State',color='Registered_User',color_continuous_scale='thermal',title = 'User Analysis')
            fig_tra.update_geos(fitbounds="locations", visible=False)
            fig_tra.update_layout(title_font=dict(size=33),title_font_color='#6739b7', height=800)
            st.plotly_chart(fig_tra,use_container_width=True)
            #fig_tra.show()
        if st.button("Explore Datas for Top User"):
            mycursor.execute(f"select * from phonepe_project.topuser WHERE Year = '{topu_year}' AND Quater = '{topu_quarter}' order by Registered_Users desc limit 10;")
            top_user_insight = mycursor.fetchall()
            df_top_user_insight = pd.DataFrame(top_user_insight, columns=['State','Year','Quarter','District Name','Registered User'])
            st.text(f"Top 10 States with Maximum number of Users in Quarter {topu_quarter} and Year {topu_year} !!!!!!!")
            st.write(df_top_user_insight)

     
     
    
                
        






     
     
          


                

      










