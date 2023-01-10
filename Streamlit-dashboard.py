import streamlit as st
import altair as alt
import snowflake.connector
import pandas as pd 

# Create input widgets in the sidebar
st.sidebar.markdown('## Snowflake Connection')
account_name = st.sidebar.text_input('Account name', value="account-name.ap-south-1.aws")
database = st.sidebar.text_input('Database', value="Sales")
username = st.sidebar.text_input('Username',value="username")
password = st.sidebar.text_input('Password' ,type="password", value="password")
role = st.sidebar.text_input('Role',"ACCOUNTADMIN")

st.session_state.setdefault('snow_conn', None)


def connect_to_snowflake(username, password, account_name, database, role):
    # Connect to Snowflake using the entered credentials
    conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account_name,
        database=database,
        role=role,
        warehouse = "COMPUTE_WH"
    )
    return conn

@st.cache(suppress_st_warning=True)
def getData():
    query = 'SELECT * FROM "Sales".PUBLIC.SALES ;'
    cursor = st.session_state.snow_conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    # Get the column names from the cursor object
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    return df

if st.sidebar.button('Connect to Snowflake'):
    # Connect to Snowflake using the entered credentials
    conn = connect_to_snowflake(username, password, account_name, database, role)
    st.session_state.snow_conn = conn
    

# If snowflake session is created then run this or else don't
if st.session_state.snow_conn :

    st.header("Connection Successful")
    df = getData()

    # Get Max and Minimum Values for range
    min_value = df['GLOBAL_SALES'].min()
    max_value = df['GLOBAL_SALES'].max()
    # Create the range filter
    min,max = st.slider('Global Sales Range', float(min_value), float(max_value), value=(float(min_value), float(max_value)))
    st.write('Number of Rows:' , len(df.loc[df['GLOBAL_SALES'].between(min,max)]))
    df.loc[df['GLOBAL_SALES'].between(min,max)]
    
    # Plotting 
    st.subheader("Genre x Global Sales")
    data_genre = df.groupby(by=['GENRE'])['GLOBAL_SALES'].sum()
    data_genre = data_genre.reset_index()
    data_genre = data_genre.sort_values(by=['GLOBAL_SALES'], ascending=False)
    c = alt.Chart(data_genre).mark_bar().encode(x='GENRE', y='GLOBAL_SALES' )
    st.altair_chart(c,use_container_width=True,)

    st.subheader("Game x Global Sales")
    top_game_sale = df.head(20)
    top_game_sale = top_game_sale[['NAME', 'YEAR', 'GENRE', 'GLOBAL_SALES']]
    top_game_sale = top_game_sale.sort_values(by=['GLOBAL_SALES'], ascending=False)
    c = alt.Chart(top_game_sale).mark_bar().encode(x='GLOBAL_SALES', y='NAME' )
    st.altair_chart(c,use_container_width=True,)

    st.subheader("Publisher x Region")
    sale_option = st.selectbox(
    'Whose Sale You want to see?',
    ('GLOBAL_SALES','JP_SALES','NA_SALES','EU_SALES'))
    
    data_platform = df.groupby(by=['PUBLISHER'])[sale_option].sum()
    data_platform = data_platform.reset_index()
    data_platform = data_platform.sort_values(by=[sale_option], ascending=False).head()

    # You can use altair as well which is an advanced plotting libarary 
    c = alt.Chart(data_platform).mark_bar().encode(x='PUBLISHER', y=sale_option )
    st.altair_chart(c,use_container_width=True,)

    st.header(" Line Plots Year x Region ")
    data_for_YR = df.groupby('YEAR').sum(numeric_only=True).sort_values('YEAR', ascending=False)
    new = data_for_YR.drop(['RANK'], axis=1)
    # Turning Year into str so there will be no commas 
    new.index = new.index.astype(str)
    st.line_chart(new)

else:
    st.header("Connect with snowflake to get Started")
    st.write('''This Streamlit app allows you to connect to a Snowflake /
     database and retrieve data from a table. The app has a sidebar with  /
     input widgets for entering the credentials for connecting to Snowflake /
     (account name, database, username, password, and role). When you click 
     the Connect to Snowflake button, the app connects to the Snowflake database 
     fetching your tables and visualizing data into beautiful plots.''')
