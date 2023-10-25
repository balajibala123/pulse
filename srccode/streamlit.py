import streamlit as st
import geopandas as gpd
import plotly.express as px
from aggregated import aggDataTrans, aggDataUsers
import plotly.graph_objects as go
# from aggregated import MyCursor



# Set the display mode to "wide"
st.set_page_config(layout="wide")


st.title("Streamlit App with Data Selection and Interactive Map")


# result = aggDataTrans()
data_type = st.sidebar.selectbox("Select Data Type:", ('Transcations', 'Users'))
Year = st.sidebar.selectbox('Select Year:', list(range(2018, 2024)))
Quarter = st.sidebar.selectbox('Select Quarter:', ["Q1","Q2","Q3","Q4"])


if st.sidebar.button("Submit"):
    if data_type == "Transcations":
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

        # Set the hover template
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

