import numpy as np
import pandas as pd
import datetime as dt

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
# We can view all of the classes that automap found
Base.classes.keys()
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station
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
def home():
    return (f"Available Routes:<br/>"
            f"/api/v1.0/precipitation<br/>"
            f"/api/v1.0/stations<br/>"
            f"/api/v1.0/tobs<br/>"
            f"/api/v1.0/start_date<br/>"
            f"/api/v1.0/start_date/end_date<br/>")

# * Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
# * Return the JSON representation of your dictionary.
@app.route("/api/v1.0/precipitation")
def precipitation():

    query_p = session.query(Measurement.date,Measurement.station,Measurement.prcp).\
                filter(Measurement.date.between('2016-08-23','2017-08-23')).\
                order_by(Measurement.date).all()
    session.close()

    prcp_dict = {d:{s:p for d,s,p in query_p if d==d} for d,s,p in query_p}

    return jsonify(prcp_dict)


# * Return a JSON list of stations from the dataset.
@app.route("/api/v1.0/stations")
def stations():
    query_s = session.query(Station.station,Station.name).group_by(Station.station).all()
    session.close()

    station_list = [{i:j} for i,j in query_s]

    return jsonify(station_list)


# * query for the dates and temperature observations from a year from the last data point.
# * Return a JSON list of Temperature Observations (tobs) for the previous year.
@app.route("/api/v1.0/tobs")
def tobs():
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    last_year = dt.datetime.strptime(latest_date,"%Y-%m-%d") - dt.timedelta(days=365)
    last_year = dt.datetime.strftime(last_year,"%Y-%m-%d")

    query_t = session.query(Measurement.date, Measurement.station, Measurement.tobs).\
                filter(Measurement.date.between(last_year,latest_date)).\
                order_by(Measurement.date).all()
    session.close()

    tobs_dict = {d:{s:t for d,s,t in query_t if d==d} for d,s,t in query_t}

    return jsonify(tobs_dict)


# * Return a JSON list of the minimum temperature, the average temperature, and the 
# max temperature for a given start or start-end range.

# * When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates 
# greater than and equal to the start date.

# * When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` 
# for dates between the start and end date inclusive.

@app.route("/api/v1.0/<start_date>")
def start_date(start_date):
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    query_calc = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    calc_temps = session.query(*query_calc).filter(Measurement.date >= start_date).all()[0]
    session.close()

    d_range = f"{start_date} to {last_date}"
    calc_dict = {d_range:{'TMIN':calc_temps[0],'TAVG':calc_temps[1],'TMAX':calc_temps[2]}}

    return jsonify(calc_dict)


@app.route("/api/v1.0/<start_date>/<end_date>")
def startend_date(start_date, end_date):
    query_cal = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    calc_temp = session.query(*query_cal).filter(Measurement.date >= start_date).\
                    filter(Measurement.date <= end_date).all()[0]
    session.close()

    d_ranges = f"{start_date} to {end_date}"
    calc_dicts = {d_ranges:{'TMIN':calc_temp[0],'TAVG':calc_temp[1],'TMAX':calc_temp[2]}}

    return jsonify(calc_dicts)

if __name__ == '__main__':
    app.run(debug=True)
