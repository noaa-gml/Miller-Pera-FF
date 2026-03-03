#!/usr/bin/env python3
import xarray
import numpy as np
from datetime import datetime

full_data = xarray.open_dataset('from_ash/ash_ff_2024b.nc')



full_data = full_data.rename({
    'time': 'date',
    'fossil_imp_area':'fossil_imp',
    'time_bnds': 'date_bounds'
})

full_data = full_data.rename({ 'bnds':'bounds'})

full_data['fossil_imp'].attrs['long_name'] = full_data['fossil_imp'].attrs['long name']
del full_data['fossil_imp'].attrs['long name']
full_data['fossil_imp'] = full_data['fossil_imp'].transpose('date', 'lat', 'lon')
full_data = full_data.drop_vars(['fossil_imp_cell', 'earth_radius', 'month_lengths', 'year_lengths'])

full_data['date'] = (full_data['date_bounds'][:, 1] - full_data['date_bounds'][:, 0]) / 2 + full_data['date_bounds'][:, 0] # make exact middle of month
full_data['date'].attrs['bounds'] = "date_bounds"
full_data['decimal_date'] = (full_data['date'] - np.datetime64(30, 'Y')) / np.timedelta64(1, 'D') / 365 + 2000
full_data['decimal_date'].attrs['units'] = 'years'

full_data['calendar_components'] = [1, 2, 3, 4, 5, 6]
full_data['date_components'] = (('calendar_components', 'date'), np.stack([
    full_data['date'].dt.year.values,
    full_data['date'].dt.month.values,
    full_data['date'].dt.day.values,
    full_data['date'].dt.hour.values,
    full_data['date'].dt.minute.values,
    full_data['date'].dt.second.values
]))
full_data['date_components'].attrs['long_name'] = "integer components of UTC date"
full_data['date_components'].attrs['comment'] = "Calendar date components as integers.  Times and dates are UTC."
full_data['date_components'].attrs['order'] = "year, month, day, hour, minute, second"

full_data.attrs['Notes'] = "This file contains CarbonTracker surface CO2 fluxes averaged over each time interval.  The times on the date axis are the centers of each averaging period."
full_data.attrs['disclaimer'] = """CarbonTracker is an open product of the NOAA Earth System Research 
Laboratory using data from the Global Monitoring Division greenhouse 
gas observational network and collaborating institutions.  Model results 
including figures and tabular material found on the CarbonTracker 
website may be used for non-commercial purposes without restriction,
but we request that the following acknowledgement text be included 
in documents or publications made using CarbonTracker results: 

     CarbonTracker results provided by NOAA/ESRL,
     Boulder, Colorado, USA, http://carbontracker.noaa.gov

Since we expect to continuously update the CarbonTracker product, it
is important to identify which version you are using.  To provide
accurate citation, please include the version of the CarbonTracker
release in any use of these results.

The CarbonTracker team welcomes special requests for data products not
offered by default on this website, and encourages proposals for
collaborative activities.  Contact us at carbontracker.team@noaa.gov.
"""
full_data.attrs['email'] = "carbontracker.team@noaa.gov"
full_data.attrs['url'] = "http://carbontracker.noaa.gov"
full_data.attrs['institution']= "NOAA Earth System Research Laboratory"
full_data.attrs['conventions'] = "CF-1.9" ;
full_data.attrs['history'] = f"Created on {datetime.now()}\nby script {__file__}" 
full_data.attrs['Source'] = "John Miller (Ash) fossil fuel emissions estimate 2024b" 


for year, year_data in full_data.groupby('date.year'):
    print(year,':',end="")
    year_data.to_netcdf(f'flux1x1_ff.{year}.nc', encoding={
        'date': {'units':'days since 1900-01-01', 'dtype':'float32'},
        'calendar_components': {'dtype':'float32'},
        'date_components': {'dtype':'float32'}},
        unlimited_dims={'date'})

    for month, month_data in year_data.groupby('date.month'):
        print(month,"",end="",flush=True)
        month_data.to_netcdf(f'flux1x1_ff.{year}{month:02d}.nc', encoding={
        'date': {'units':'days since 1900-01-01', 'dtype':'float32'},
        'calendar_components': {'dtype':'float32'},
        'date_components': {'dtype':'float32'}},
        unlimited_dims={'date'})
    print()




print(year_data)
