#!/usr/bin/env python
# coding: utf-8

# In[7]:


import _duckdb
from datetime import datetime
import calendar


# In[1]:


def _check_input_source(osm_history_data):
    """
    Dynamically build SQL FROM clause based on the input type of the OSM history data parameter. 

    Parameters:
    - osm_history_data (str | DuckDB PyRelation): input source containing OSM history data

    Returns: 
    - osm_history_data (str): file path pointing to OSM history data
    - view_osm_history_data (str): name of view created based on OSM DuckDB PyRelation
    """
    if isinstance(osm_history_data, str):
        # Dynamically build SQL query FROM clause 
        return f"\n read_parquet('{osm_history_data}')"
    elif isinstance(osm_history_data, _duckdb.DuckDBPyRelation): 
        view_osm_history_data = "_temp_data_source"
        osm_history_data.create_view(view_osm_history_data, replace=True)
        return view_osm_history_data 
    else: 
        raise ValueError('The input data source is invalid') 


# In[2]:


# Check validity of time parameters
def _check_validity_input_time_parameters(date):
    """
    Read a date string against a list of accepted formats and returns the datetime object plus the matching format. 
    If none of the formats match, it raises an error. Later code will not run  

    Parameters:
    - date (str): accepted formats are ['YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD',  'YYYY-MM', 'YYYY']

    Returns: 
    - dt (str): date object
    - fmt (str): time granularity of date object
    """

    formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m', '%Y']

    dt = None 
    for fmt in formats:
        try: 
            dt = datetime.strptime(date, fmt)
            return dt, fmt
        except ValueError: 
            continue

    if dt == None:
        raise ValueError(f"Invalid date format: '{date}'. Expected formats: YYYY, YYYY-MM, YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")


# In[3]:


def _normalize_start(dt, fmt):
    """
    Infer user intent for start_of parameter based on granularity of date object. Arguments derived using _check_validity_input_time_parameters() helper function

    Parameters:
    - dt (str): date object
    - fmt (str): granularity of date object

    Returns: 
    - dt (str): Noramalized date object
    """
    # Assumes dt and fmt have already been validated by _check_validity_input_time_parameters
    if fmt == '%Y':
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0)
    elif fmt == '%Y-%m':
        return dt.replace(day=1, hour=0, minute=0, second=0)
    elif fmt == '%Y-%m-%d':
        return dt.replace(hour=0, minute=0, second=0)
    else:  # '%Y-%m-%d %H:%M:%S'
        return dt


# In[4]:


def _normalize_end(dt, fmt):
    """
    Infer user intent for end_of parameter based on granularity of date object. Arguments derived using _check_validity_input_time_parameters() helper function

    Parameters:
    - dt (str): date object
    - fmt (str): granularity of date object

    Returns: 
    - dt (str): Noramalized date object
    """
    # Assumes dt and fmt have already been validated by _check_validity_input_time_parameters
    if fmt == '%Y':
        return dt.replace(month=12, day=31, hour=23, minute=59, second=59)
    elif fmt == '%Y-%m':
        last_day = calendar.monthrange(dt.year, dt.month)[1]
        return dt.replace(day=last_day, hour=23, minute=59, second=59)
    elif fmt == '%Y-%m-%d':
        return dt.replace(hour=23, minute=59, second=59)
    else:  # '%Y-%m-%d %H:%M:%S'
        return dt

