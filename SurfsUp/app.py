# Import the dependencies.
from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import MetaData

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
metadata = MetaData()
metadata.reflect(bind=engine)

# reflect the tables
base = automap_base(metadata=metadata)
base.prepare()

# Save references to each table
station = base.classes.station
measurement = base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    return(
        f"Welcome to the Hawaii Climate Analysis API! <br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/temp/start<br/>"
        f"/api/v1.0/temp/start/end<br/>"
        f"<p> 'start' and 'end' dates should be in MM-DD-YYYY format.</p>"
        
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Return precipitation data for the last year
    # Calculate the date one year ago from the last date in the database
    most_recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()[0]
    one_year_ago = dt.datetime.strptime(most_recent_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Query for the precipitation from the last year
    results = session.query(measurement.date, measurement.prcp)\
        .filter(measurement.date >= one_year_ago, measurement.date <= most_recent_date)\
        .all()

    session.close()

    precipitation = {date: prcp for date, prcp in results}
    return jsonify(precipitation)

@app.route("/api/v1.0/stations")
def stations():
    results = session.query(station.station).all()

    session.close()
    stations = list(np.ravel(results))
    return jsonify(stations=stations)

@app.route("/api/v1.0/tobs")
def temp():
    station_with_most_observations = session.query(measurement.station, func.count(measurement.station))\
    .group_by(measurement.station)\
    .order_by(func.count(measurement.station).desc())\
    .first()[0]
    most_recent_date = session.query(func.max(measurement.date)).scalar()
    one_year_ago = dt.datetime.strptime(most_recent_date, '%Y-%m-%d') - dt.timedelta(days=365)

    results = session.query(measurement.tobs)\
        .filter(measurement.station == station_with_most_observations, measurement.date >= one_year_ago, measurement.date <= most_recent_date)\
        .all()
    
    session.close()
    temps = list(np.ravel(results))
    return jsonify(temps=temps)

@app.route("/api/v1.0/temp/<start>")
@app.route("/api/v1.0/temp/<start>/<end>")
def stats(start=None, end=None):
    
    most_active_stations = session.query(station.station, func.count(measurement.station))\
    .filter(station.station == measurement.station)\
    .group_by(station.station)\
    .order_by(func.count(measurement.station).desc())\
    .all()
    most_active_station_id = most_active_stations[0][0] 

    temperature_stats = session.query(func.min(measurement.tobs), func.max(measurement.tobs), func.avg(measurement.tobs))\
    .filter(measurement.station == most_active_station_id)\
    .all()

    lowest_temp, highest_temp, avg_temp = temperature_stats[0]
    print(f"Most Active Station: {most_active_station_id}")
    print(f"Lowest Temperature: {lowest_temp} F")
    print(f"Highest Temperature: {highest_temp} F")
    print(f"Average Temperature: {avg_temp} F")

    most_recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()[0]
    one_year_ago = dt.datetime.strptime(most_recent_date, '%Y-%m-%d') - dt.timedelta(days=365)

    results = session.query(measurement.date, measurement.prcp)\
    .filter(measurement.date >= one_year_ago, measurement.date <= most_recent_date)\
    .all()

    df = pd.DataFrame(results, columns=['Date', 'Precipitation'])

    df['Date'] = pd.to_datetime(df['Date'])  
    df = df.sort_values('Date')



    session.close()
    temps = list(np.ravel(results))
    return jsonify(temps)

if __name__ == "__main__":
    app.run()