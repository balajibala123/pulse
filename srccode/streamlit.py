import streamlit as st
import geopandas as gpd
import plotly.express as px
from aggregated import aggDataTrans, aggDataUsers
from aggregated import aggDataTransCategory
from aggregated import StateTransactions
from aggregated import StateUsers
from aggregated import TopDistrictUsers
from aggregated import TopPincodeUsers
from aggregated import DistrictTransactions
from aggregated import PincodeTransactions
from aggregated import UsersCategory
import plotly.graph_objects as go


# Set the display mode to "wide"
st.set_page_config(layout="wide")

st.title("Streamlit App with Data Selection and Interactive Map")

data_type = st.sidebar.selectbox("Select Data Type:", ('Transcations', 'Users'))
# Year = st.sidebar.selectbox('Select Year:', list(range(2018, 2024)))
# Quarter = st.sidebar.selectbox('Select Quarter:', ["Q1","Q2","Q3","Q4"])

# Define the list of years and quarters
years = list(range(2018, 2024))
quarters = ["Q1", "Q2", "Q3", "Q4"]

# Get the selected year from the user
Year = st.sidebar.selectbox('Select Year:', years)

# If the selected year is 2023, remove "Q4" from the quarters list
if Year == 2023:
    available_quarters = quarters[:-1]  # Remove the last element ("Q4")
else:
    available_quarters = quarters

# Get the selected quarter from the user
Quarter = st.sidebar.selectbox('Select Quarter:', available_quarters)

data_options = st.sidebar.selectbox("Select Data Options:", ('States', 'Districts', 'Postal_Codes'))

if st.sidebar.button("Submit"):
    if data_type == "Transcations":
        #  --------------------------------------------------------------------------------------------------------------------------
        # Displaying category
        # Assuming you have already calculated result_Category
        result_Category = aggDataTransCategory(Quarter, Year)

        # finding unique value 
        Total_Transaction = result_Category['Total_Transactions'].unique()[0]
        Total_Payment_Value = result_Category['Total_Payment_Value'].unique()[0]
        Avg_Transaction_Value = result_Category['Avg_Transaction_Value'].unique()[0]
        
        # Display the result in a summary format
        st.sidebar.subheader("Transaction Category Summary")
        st.sidebar.write(f"Total Transactions: {Total_Transaction:,.0f}")
        st.sidebar.write(f"Total Payment Value: {Total_Payment_Value:,.0f} Cr")
        st.sidebar.write(f"Avg Transaction Value: {Avg_Transaction_Value:,.0f}")
        st.sidebar.markdown(result_Category[['Transactions_Name', 'Transcations']].to_markdown(index=False), unsafe_allow_html=True)
        
        if data_options == "States":
            # Top 10 Trans States 
            Top10TransactionsState = StateTransactions(Quarter, Year)
            st.sidebar.subheader("Top 10 States Transactions")
            st.sidebar.markdown(Top10TransactionsState[['state','Transacations_Count']].to_markdown(index=False), unsafe_allow_html=True)       
        elif data_options == 'Districts':
            Top10Districts = DistrictTransactions(Quarter, Year)
            st.sidebar.subheader("Top 10 Districts Transactions")
            st.sidebar.markdown(Top10Districts[['District_Name', 'Transactions_Count']].to_markdown(index=False), unsafe_allow_html=True)
        elif data_options == "Postal_Codes":
            Top10Pincodes = PincodeTransactions(Quarter, Year)
            st.sidebar.subheader("Top 10 Postal Codes")
            st.sidebar.markdown(Top10Pincodes[['Pincodes', 'Transactions_Count']].to_markdown(index=False), unsafe_allow_html=True)

        # ---------------------------------------------------------------------------------------------------------------------------
        # Displaying Maps
        result = aggDataTrans(Quarter, Year)
    

        # Create a custom hover template to display the desired columns
        hover_template = "<b>%{location}</b><br>" + \
                        "All Transactions: %{customdata[0]:.0f}<br>" + \
                        "Total Payment Value: %{customdata[1]:.0f} Cr<br>" + \
                        "Avg Transactions Value: %{customdata[2]:.0f}"

        fig = px.choropleth(
            result,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='state',
            color='All_Transactions',
            custom_data=['All_Transactions', 'Total_Payment_Value', 'Avg_Transactions_Value'],
            hover_name= result.index,
            color_continuous_scale='Reds',
        )

        # # Set the hover template
        fig.update_traces(
            hovertemplate=hover_template
        )

        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(geo=dict(
            center={'lon': 78.9629, 'lat': 20.5937},
            projection_scale=5))
        # # fig.show()

        st.plotly_chart(fig)

    elif data_type == 'Users':
        UsersRegistered = UsersCategory(Quarter,Year)
        st.sidebar.subheader("Registered PhonePe users")
        st.sidebar.markdown(UsersRegistered['RegisteredUsers'].to_markdown(index=False), unsafe_allow_html=True)
        st.sidebar.subheader("PhonePe app opens")
        st.sidebar.markdown(UsersRegistered['AppOpens'].to_markdown(index=False), unsafe_allow_html=True)

        # Top 10 Users State
        if data_options == 'States':
            Top10UsersState = StateUsers(Quarter, Year)
            st.sidebar.subheader("Top 10 State RegisteredUsers")
            st.sidebar.markdown(Top10UsersState[['state','registeredUsers']].to_markdown(index=False), unsafe_allow_html=True)
        
        elif data_options == 'Districts':
            Top10UDistrictUsers = TopDistrictUsers(Quarter, Year)
            st.sidebar.subheader("Top 10 Districts RegisteredUsers")
            st.sidebar.markdown(Top10UDistrictUsers[['District_Name', 'RegisteredUsers']].to_markdown(index=False), unsafe_allow_html=True)
        
        elif data_options == 'Postal_Codes':
            Top10PincodeUsers = TopPincodeUsers(Quarter, Year)
            st.sidebar.subheader("Top 10 Pincode RegistedUsers")
            st.sidebar.markdown(Top10PincodeUsers[['Pincodes', 'RegisteredUsers']].to_markdown(index=False), unsafe_allow_html=True)
        
        #  ------------------------------------------------------------------------------------------------------------------

        # AggDataUsers showing Map for registeredusers and app opens
        result = aggDataUsers(Quarter, Year)
        # Create a custom hover template to display the desired columns
        hover_template = "<b>%{location}</b><br>" + \
                        "registeredUsers: %{customdata[0]:.0f}<br>" + \
                        "appOpens: %{customdata[1]:.0f}"

        fig = px.choropleth(
            result,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='state',
            color='registeredUsers',
            custom_data=['registeredUsers','appOpens'],
            hover_name= result.index,
            color_continuous_scale='Viridis',

        )

        # Set the hover template
        fig.update_traces(
            hovertemplate=hover_template
        )

        fig.update_geos(fitbounds="locations", visible=False)

        fig.update_layout(geo=dict(
            center={'lon': 78.9629, 'lat': 20.5937},
            projection_scale= 5)
            # width=800,  # Set the width of the figure
            # height=600,  # Set the height of the figure
            # plot_bgcolor='lightblue'  # Set the background color
            
            )
        
        # fig.show()
        st.plotly_chart(fig)

