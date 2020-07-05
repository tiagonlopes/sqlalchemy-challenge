import numpy as np
import datetime as dt
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station
#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/placedate/<start_date><br/>"
        f"/api/v1.0/placedate/<start_date>/<end_date><br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return the Precipitation amount by Date"""
    # Query Precipitation from last year
    last_date = session.query(func.strftime(Measurement.date)).\
        order_by(Measurement.date.desc()).first()
    
    last_year = dt.datetime.strptime(last_date[0],"%Y-%m-%d")-dt.timedelta(days=365)


    last_year_prcp = session.query(func.strftime(Measurement.date),Measurement.prcp).\
        filter(Measurement.date>= last_year).\
        order_by(func.strftime(Measurement.date)).all()
    
    session.close()

    # Define Output
    data_prcp= []
    for date,prcp in last_year_prcp:
        data_dict={}
        data_dict[date]=prcp
        data_prcp.append(data_dict)
    
    return jsonify(data_prcp)

@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return all Active Stations of the dataset"""
    # Query for all Stations
    active_stations = session.query(Measurement.station,Station.name).\
        filter(Measurement.station == Station.station).\
        group_by(Measurement.station).all()
    
    session.close()

    # Define Output
    data_station= []
    for id,name in active_stations:
        data_dict={}
        data_dict["ID"]=id
        data_dict["Station Name"]=name
        data_station.append(data_dict)
    
    return jsonify(data_station)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return all Active Stations of the dataset"""
    # Query Temperature in Last Year based on the station more active
    last_date = session.query(func.strftime(Measurement.date)).\
        order_by(Measurement.date.desc()).first()
    
    last_year = dt.datetime.strptime(last_date[0],"%Y-%m-%d")-dt.timedelta(days=365)

    active_stations = session.query(Measurement.station,func.count(Measurement.tobs)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.tobs).desc()).all()
    
    most_active = active_stations[0][0]

    most_active_temp = session.query(Measurement.tobs).\
        filter(Measurement.date>= last_year).\
        filter(Measurement.station == most_active).all()

    session.close()

    # Define Output
    temp_mostActive = list(np.ravel(most_active_temp))
    
    
    return jsonify(temp_mostActive)


@app.route("/api/v1.0/placedate/<start_date>/<end_date>")
def hawai_date(start_date,end_date):
    """Fetch the date and calculate temperature, or a 404 if not."""
    # Validate Date Format
    try: 
        dt.datetime.strptime(start_date,"%Y-%m-%d")
    except:
        return jsonify({"error": f"Please place a start date on the following format: YYYY-MM-DD"}), 404
    
    try:
        dt.datetime.strptime(start_date,"%Y-%m-%d")
    except:
        return jsonify({"error": f"Please place a start date on the following format: YYYY-MM-DD"}), 404
    
    # Create our session (link) from Python to the DB
    session = Session(engine)
    
    #Query First and last date of the dataset

    last_date = session.query(func.strftime(Measurement.date)).\
        order_by(Measurement.date.desc()).first()
    
    first_date = session.query(func.strftime(Measurement.date)).\
        order_by(Measurement.date.asc()).first()
    
    #Validate date

    tdelta = dt.datetime.strptime(start_date,"%Y-%m-%d")-dt.datetime.strptime(end_date,"%Y-%m-%d")
    if tdelta.days >0:
        d1=start_date
        d2=end_date
        end_date = d1
        start_date = d2
    
    tdelta1 = dt.datetime.strptime(end_date,"%Y-%m-%d")-dt.datetime.strptime(last_date[0],"%Y-%m-%d")
    if tdelta1.days>0:
        session.close()
        return jsonify({"error": f"Your End Date is above the dataset limit. Last date: {last_date[0]}"}), 404

    
    tdelta2 = dt.datetime.strptime(start_date,"%Y-%m-%d")-dt.datetime.strptime(first_date[0],"%Y-%m-%d")
    if tdelta2.days<0:
        session.close()
        return jsonify({"error": f"Your Start Date is below the dataset limit. Initial date: {first_date[0]}"}), 404

    #Calculate Tmin, Tavg and Tmax

    [(tmin,tavg,tmax)]= session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    result = {"Temp Min":tmin,"Temp Avg":tavg,"Temp Max":tmax}

    session.close()
    
    return jsonify(result)

@app.route("/api/v1.0/placedate/<start_date>")
def hawai_date2(start_date):
    """Fetch the date and calculate temperature, or a 404 if not."""
    # Validate Date Format
    try: 
        dt.datetime.strptime(start_date,"%Y-%m-%d")
    except:
        return jsonify({"error": f"Please place a start date on the following format: YYYY-MM-DD"}), 404
    
    # Create our session (link) from Python to the DB
    
    session = Session(engine)
    
    #Query First and last date of the dataset
    
    last_date = session.query(func.strftime(Measurement.date)).\
        order_by(Measurement.date.desc()).first()
    
    first_date = session.query(func.strftime(Measurement.date)).\
        order_by(Measurement.date.asc()).first()
        
    end_date = last_date[0]

   #Validate date

    tdelta1 = dt.datetime.strptime(start_date,"%Y-%m-%d")-dt.datetime.strptime(first_date[0],"%Y-%m-%d")
    
    if tdelta1.days <0:
        session.close()
        return jsonify({"error": f"Your Start Date is below the dataset limit. Initial date: {first_date[0]}"}), 404
    
    tdelta2 = dt.datetime.strptime(start_date,"%Y-%m-%d")-dt.datetime.strptime(end_date,"%Y-%m-%d")
    
    if tdelta2.days>0:
        session.close()
        return jsonify({"error": f"Your Start date is above the Last date in dataset. Last date: {end_date}"}), 404
    

    #Calculate Tmin, Tavg and Tmax
    [(tmin,tavg,tmax)]= session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    result = {}
    result["Temp Min"]=tmin
    result["Temp Avg"]=tavg
    result["Temp Max"]=tmax

    session.close()

    return jsonify(result)

if __name__ == '__main__':
    app.run(debug=True)
