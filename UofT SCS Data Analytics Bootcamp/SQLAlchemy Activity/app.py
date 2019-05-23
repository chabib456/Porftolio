
from statistics import mean
import pandas as pd
import datetime as dt
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func,inspect
from flask import Flask,jsonify
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# We can view all of the classes that automap found
Base.classes.keys()
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)
# Design a query to retrieve the last 12 months of precipitation data and plot the results
last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
# Calculate the date 1 year ago from the last data point in the database
stmt=session.query(Measurement.date,Measurement.prcp).filter(Measurement.date>=(dt.date(int(last_date[0].split('-')[0]),int(last_date[0].split('-')[1]),int(last_date[0].split('-')[2]))-dt.timedelta(days=365))).all()
# Perform a query to retrieve the data and precipitation scores
# Save the query results as a Pandas DataFrame and set the index to the date column
df=pd.DataFrame(stmt,columns=["Date","Prcp"])
df.set_index("Date",inplace=True)
prcp_dict = df["Prcp"].to_dict()

# Merge Tables ###
measurement = session.query(Measurement.id,Measurement.station,Measurement.date,Measurement.prcp,Measurement.tobs).all()
station = session.query(Station.id,Station.station,Station.name,Station.latitude,Station.longitude,Station.elevation).all()
measurement_df=pd.DataFrame(measurement)
station_df = pd.DataFrame(station)
complete_df = pd.merge(measurement_df,station_df,on="station")
# FInd prcp for last 12 months - App 1
last_date = complete_df["date"].max()
last_year = int(last_date[:4])
last_year = last_year - 1
last_year = str(last_year) + last_date[4:]
last_year_results = complete_df.loc[complete_df["date"]>=last_year,["date","prcp"]]
last_year_results.set_index("date",inplace=True)
#last_year_results.head()

# Find total number of stations 
total_stns = len(station_df["id"].unique())

#list of stations - App 2
stations = complete_df[["name","station"]]
stations = stations.drop_duplicates()
stations = stations.set_index("station")
stations_dict = stations.to_dict()
#Most active stations 
stns_grouped = complete_df.groupby("station")
active_stns = stns_grouped["station"].count()
active_stns = active_stns.sort_values(ascending=False)
active_stns = pd.DataFrame(active_stns)
#active_stns

#Most active stn average, max and min tobs
active_stn = active_stns.idxmax()
active_stn = active_stn[0]
active_stn_data = complete_df.loc[complete_df["station"]==active_stn,["tobs"]]
active_stn_data = [active_stn_data.min()[0],active_stn_data.mean()[0],active_stn_data.max()[0]]
#print(active_stn_data)

#tobs for last 12 months for active station
active_stn_df = complete_df.loc[complete_df["station"]==active_stn]
last_day_active_station = active_stn_df["date"].max()
last_year_active_stn = str(int(last_day_active_station[:4])-1)+last_day_active_station[4:]
last_year_active_data=active_stn_df.loc[active_stn_df["date"]>=last_year_active_stn,"tobs"]
last_year_active_data = pd.DataFrame(last_year_active_data)
active_stn_grouped = last_year_active_data.groupby("tobs")
tobs_counts = active_stn_grouped["tobs"].count()
#tobs_counts

#tobs for last 12 month - App 3
last_day_tobs = complete_df[["date","tobs"]]
last_day_tobs = last_day_tobs.dropna(how="any")
last_day = last_day_tobs["date"].max()
last_year = str(int(last_day[:4])-1)+last_day_active_station[4:]
last_year_tobs=last_day_tobs.loc[last_day_tobs["date"]>=last_year,["date","tobs"]]
last_year_tobs = pd.DataFrame(last_year_tobs)
last_year_tobs.set_index("date",inplace=True)
last_year_tobs_dict=last_year_tobs.to_dict()
#tobs for vaction - App 4
cont =1 
while cont == 1:
    start = input("Input vacation start date (YYY-MM-DD): ")
    dates = list(complete_df["date"])   
    if start in dates:
        cont=0
    else:
        print("Sorry, invalid date format. Please try again\n")

ending = input("Do you know your trip's end date? Type 'y' for YES ")
end_date = 0
if ending == 'y':
    cont = 1
    while cont == 1:
        end=input("Input vacation end date (YYY-MM-DD): ")
        
        if end in dates:
            cont=0
            end_date = 1
        else:
            print("Sorry, invalid date. Please try again\n")
        if end_date!=0:
            trip_data = complete_df.loc[(complete_df["date"]>=start)&(complete_df["date"]<=end),"tobs"]
            trip_data1 = {"Min":trip_data.min(),"Average":trip_data.mean(),"Max":trip_data.max()}
            
        else:
            trip_data = complete_df.loc[(complete_df["date"]>=start),"tobs"]
            trip_data2 = {"Min":trip_data.min(),"Average":trip_data.mean(),"Max":trip_data.max()}
            



app = Flask(__name__)
if end_date!=0:
    @app.route("/")
    def home():
        return(f"Welcome to the Climate App<br/>"
            f"Avalilable Routes<br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/start_date/end_date")

    @app.route("/api/v1.0/precipitation")
    def precipitation():
        return jsonify(prcp_dict)

    @app.route("/api/v1.0/tobs")
    def temp_obs():
   
        return jsonify(last_year_tobs_dict)

    @app.route("/api/v1.0/stations")
    def list_station():
    
        return jsonify(stations_dict)

    @app.route("/api/v1.0/<start_date>/<end_date>")
    def calc_temps1(start_date,end_date):
        start_date = 0
        end_date = 0
        return jsonify(trip_data1)


    if __name__ =="__main__":
        app.run(debug=True)
else:
    @app.route("/")
    def home():
        return(f"Welcome to the Climate App<br/>"
            f"Avalilable Routes<br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/start_date")

    @app.route("/api/v1.0/precipitation")
    def precipitation():
        return jsonify(prcp_dict)

    @app.route("/api/v1.0/tobs")
    def temp_obs():
   
        return jsonify(last_year_tobs_dict)

    @app.route("/api/v1.0/stations")
    def list_station():
    
        return jsonify(stations_dict)

    @app.route("/api/v1.0/<start_date>/")
    def calc_temps1(start_date):
        start_date = 0
        return jsonify(trip_data2)


    if __name__ =="__main__":
        app.run(debug=True)
    

