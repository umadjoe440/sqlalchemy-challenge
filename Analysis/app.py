import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

import datetime as dt
from dateutil.relativedelta import relativedelta

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
# Save references to each table
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
        f"<h2>WELCOME TO THE HAWAII CLIMATE INFO API</h2>"
        f"<h3>Available Routes:</h3>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"<b>Temperature Averages from Start Date to end of Data Set:</b><br/>"
        f"/api/v1.0/YYYY-MM-DD<br/>"
        f"<b>Temperature Averages for Date Range:</b><br/>"
        f"/api/v1.0/YYYY-MM-DD/YYYY-MM-DD"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Run the Precipitation Query from the Jupyter Notebook
    max_date_array =engine.execute('SELECT max(date) FROM measurement')
    row = max_date_array.fetchone()
    max_date =row[0]
    # Calculate the date 1 year ago from the last data point in the database
    format_str = '%Y-%m-%d' # The format
    max_msr_date = dt.datetime.strptime(max_date, format_str)
    year_ago = max_msr_date - relativedelta(years=1)

    # Perform a query to retrieve the data and precipitation scores
    sel = [Measurement.date,Measurement.prcp]
    measure_12month = session.query(*sel).filter(Measurement.date >= year_ago.strftime(format_str)).order_by(Measurement.date).all()
    session.close()
    #return results in dictionary format
    precipitation_data = []
    for date, precipitation_value in measure_12month:
        precip_dict = {}
        precip_dict["date"] = date
        precip_dict["precipitation"] = precipitation_value
        precipitation_data.append(precip_dict)

    return jsonify(precipitation_data)


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    # Query all weather stations
    sel = [Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation ]
    station_info = session.query(*sel).all()
    session.close()
    return jsonify(station_info)

@app.route("/api/v1.0/tobs")
def tobs():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    #Get the most active station name
    station_info = engine.execute("SELECT station, count(*) FROM measurement GROUP BY station ORDER BY 2 DESC")
    busy_station = station_info.fetchone()
    most_active_station = busy_station[0]

    sel = [Measurement.station, func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)]

    most_active_station_info = session.query(*sel).filter(Measurement.station == most_active_station).group_by(Measurement.station).all()
    busy_station_name = most_active_station_info[0].station
    #Get the last 12 months of observed temperature from the busiest station
    # Get the max date for station 'USC00519281'
    busy_station_max_array =engine.execute('SELECT max(date) FROM measurement WHERE station == :bs', {'bs': busy_station_name})
    row = busy_station_max_array.fetchone()
    max_tobs_date =row[0]

    # Go 12 months back from max date for station 'USC00519281'
    format_str = '%Y-%m-%d' # The format
    max_busy_station_date = dt.datetime.strptime(max_tobs_date, format_str)
    busy_station_12_months_back = max_busy_station_date - relativedelta(years=1)

    # Perform a query to retrieve the tobs data
    sel = [Measurement.date,Measurement.tobs]
    busy_12month = session.query(*sel).filter(Measurement.station == busy_station_name).filter(Measurement.date >= busy_station_12_months_back.strftime(format_str)).order_by(Measurement.date).all()
    session.close()
    return jsonify(busy_12month)

@app.route("/api/v1.0/<start>")
def start_only(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # This function called `calc_temps1` will accept start date in the format '%Y-%m-%d' 
    # and return the minimum, average, and maximum temperatures from the start date
    # to the end of the data set
    def calc_temps1(start_date):
        """TMIN, TAVG, and TMAX for a list of dates.
    
        Args:
            start_date (string): A date string in the format %Y-%m-%d
            
        
        Returns:
            TMIN, TAVE, and TMAX
        """
    
        return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).all()
    
    start_result =calc_temps1(start)
    session.close()
    return jsonify(start_result)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # This function called `calc_temps2` will accept start date and end date in the format '%Y-%m-%d' 
    # and return the minimum, average, and maximum temperatures for that range of dates
    def calc_temps2(start_date, end_date):
        """TMIN, TAVG, and TMAX for a list of dates.
    
        Args:
            start_date (string): A date string in the format %Y-%m-%d
            end_date (string): A date string in the format %Y-%m-%d
        
        Returns:
            TMIN, TAVE, and TMAX
        """
    
        return session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()
    
    start_result =calc_temps2(start, end)
    session.close()
    return jsonify(start_result)


if __name__ == '__main__':
    app.run(debug=True)