; pro ff_country_2026, dev=dev, season=season

; intended to be an improvement on ff_hires, by using annual country data from
; cdiac (until 2004) and bp (2004-) instead of global edgar patterns

; 2009  is slightly modified to take account of the 2008- global recession
; by not assuming a linearly increasing emission after 2007, but keeping 2008 and 2009 at the same total as 2007

; 2009a does not assume flat emissions, but uses the BP numbers for 2008.  For 2009, in some cases, negative growth in FF will be extrapolated.
; Also:
; 1. instead of using the glb_fos_2004 file, the CDIAC global and country updates through 2006 are available and will be used
; the 2006 global numbers are significantly different (lower) than the 2004 numbers.  e.g., 1997-2004 appear to be ~0.3 PgC/yr less
; This is about 2.5Pg less emission integrated over that time.  This is primarilly due to adjustments in
; liquid fuel (petroleum) globally and specifically an increase in the amount of liquid fuel that is
; not oxidized quickly, but stays put, presumably as plastics or other long lasting HC forms.

; 2009b updates the edgar fractional emissions maps to version 4.  Edgar4 covers every year from through 2005.
; After that an extrapolation is used (see edgar_frac.pro)
; The spatial distribution in Edgar v4 doesn't appear to be population based, but actually derived from
; locations of different sectoral emissions.

; 2010a uses new CDIAC and BP totals

; 2011a is an extrapolation only

; 2011b now assigns weights to months based on actual days per month

; 2011c uses new BP and CDIAC inputs, through 2010 and 2008, respectively
; (and additionally these inputs are now processed in idl, not excel, via read_bp.pro and read_cdiac.pro)
; and... extrapolate cement.  Previous versions held both at 1.0 factors.  Flaring still doesn't have enough info

; 2012a uses new BP and CDIAC inputs, through 2011 and 2008, respectively
; also corrects bug that prevented emissions from being put over the ocean
; also corrects error where fractional cement/flaring increase was not updated in 2011 code.

; 2012b adds 3% to 2012 fluxes (all categories)

; 2013a uses BP 2013 World Review, but no CDIAC updates.  3% is added to 2012 for 2013 global fluxes

; 2014a uses BP 2014 World Review, and CDIAC updated through 2010 (countries and global total)

; 2015a uses BP 2015 World Review, and CDIAC updated through 2010 (countries and global total)

; 2016a uses BP 2015 World Review, and CDIAC updated through 2010 (countries and global total)
; this is fully backwards compatible (in inputs) with 2015a

; 2016b uses BP 2016 World Review, and CDIAC updated through 2013 (countries and global total)

; 2017a uses BP 2017 World Review, and CDIAC updated through 2013 (countries and global total)

; 2017b uses BP 2017 World Review, and CDIAC updated through 2014 (countries and global total)

; 2018a uses BP 2018 World Review, and CDIAC updated through 2014 (countries and global total)
; fix minor bug: extrapolated increase in flaring was treated identically to that of cement
; now flaring is extrapolated at same rate as (oil + gas)

; 2019a uses BP 2019 World Review, and CDIAC updated through 2014 (countries and global total)

; 2019b applies "bunker" emissions (the difference between global and sum of country totals) to grid cells with *any* ocean, not just 100% ocean

; 2026 uses EI 2025 World Energy Review, and CDIAC updated through 2021 (countries and global total)
; removed dead isotope code, unused accumulators (ncelltot, eetot), and commented-out file writes

; ALSO.... [not sure which year this is associated with!]
; after many years CDIAC made both country and global numbers gas,oil,coal
; previously, country totals had coal,oil,gas

compile_opt idl3

season = 'nam'
seas2 = 'euras'

; will still use edgar 1x1 patterns for within country patterning of country totals

; time ranges
yr_start = 1993
yr_cdiac = 2021 ; final year of cdiac data
yr_ei = 2024 ; final year of EI data
yr_final = 2025 ; final full year for extrapolated emissions
n_ei_yrs = yr_ei - yr_cdiac ; percent increases for 2022, 2023, 2024 ( 2025, no data available, so use guess of 1.00 (0% or flat), which has been roughly true globally for 2015 - 2017)

n_cdiac_yrs = yr_cdiac - yr_start + 1
n_extrap_yrs = yr_final - yr_cdiac
n_total_yrs = n_extrap_yrs + n_cdiac_yrs

; read in cdiac global totals
; This file is made by: 1) converting the CDIAC@appstate .xlsx file to .csv, 2) adding zeros to the per capita emission column as necessary
; 3)saving just the values (no headers) as an MS-DOS .txt file
; later, we will subtract from sum of country totals and this difference will be applied to the oceans
; NOTE!!! Order of columns has changed:  now Year, Total, Solid, Liquid, Gas, Cement, Flaring, Per Capita
; Was: Year, Total, Gas, Liquid, Solid, Cement, Flaring, Per Capita
;
; 2024b: Intermediate CSVs produced by injest.ipynb format both global and national as : Year, Total, Gas, Liquid, Solid, Flaring, Cement
; They are both also converted to gigagrams carbon. This removes some of the fiddly manipulation here.

ccg_fread, file = './processed_inputs/CDIAC_global_2020.csv', nc=7, skip=1, result
cdiacff = result[1:6, *]
cdiactime = result[0, *]

ranged_rows = where(cdiactime ge yr_start and cdiactime le yr_cdiac)
glob_cdiac = cdiacff[*, ranged_rows]

; 2024b: No need to switch, both files are in order Year, Total, Gas, Liquid, Solid, Flaring, Cement

; globaltot array has following order: tot,gas,liquid,solid,cement,flaring
; country_cdiac has this: tot,gas,liquid,solid,flaring,cement
glob_cdiac = transpose(glob_cdiac)

; read in cdiac country data
; 2024b: file from injest.ipynb

ccg_read, file='./processed_inputs/CDIAC_national_2020.csv', skip=1, delimiter=',', res
country = res.field1

aa = uniq(country)
ncountry1 = n_elements(aa)
aa = [-1, aa]

; to simplyify things to start with, use only data from 1992 on, where country splitting/combining
; (e.g. USSR, Czechoslovakia, Yugoslavia, Germany, Yemen) has mainly stopped (few exceptions below).
; note also that American Samoa, Antarctic Fisheries and Guam have data only until 2003 in the original
; data.  I have added 2004 years based on linear extrapolation in the file.  Hmmm... for some reason
; Guam and Am. Samoa are in the original data set on the website but not in the file;  they may be folded
; into the US total.)
; I have also combined:
; Eritrea+Ethiopia
; Israel+Palestinian Terr.
; East Timor+Indonesia
; and some others (see read_cdiac.pro)
; Somalia data ends in 1995, use 1995 value through 2004 (0.003 TgC/yr)
; -- in 2006 releases and later, Somalia data resumes in 2000, so for 1996-1999, interpolate between 1995 and 2000 values
; As of ?? full Somalia record available.
;
; in order to conform to GISS spatial country definitions, I have also made the following modifications:
; ST. PIERRE & MIQUELON + Canada
; Macau + China
; Gibraltar + Spain
; Aruba + Venezuela
; ;new in the 1751-2007 list:
; ANDRORRA + Spain


; The following fluxes totaling only ~200,000 tons of Carbon/yr have been eliminated (~25 ppm error)
; CAYMAN ISLANDS
; NIUE
; MONTSERRAT
; PALAU
; BRITISH VIRGIN ISLANDS
; ANTARCTIC FISHERIES
; SAINT HELENA
; NOTE:  HAVE BEEN ADDED TO THE COUNTRY LIST AS OF THE '2006' RELEASE, BUT I AM NOT INCLUDING THEM
; ANGUILLA (Anguilla could be added to St. Kitts & Nevis, but Anguilla starts only in 1997 and has very small emissions)
; WALLIS AND FUTUNA
; MARSHALL ISLANDS
; Federated states of Micronesia
; Turks and Caicos Islands
; NOTE: CURACAO and several others (see read_cdiac.pro) also added in 2013 release but only has data for 2012 and 2013, so not using

country_cdiac = dblarr(n_cdiac_yrs, ncountry1, 6)
countrystr = res[aa[1 : *]].field1
countrystr2 = countrystr

; fill up array of country ff data: total and 5 sectors:gas,liquid,solid,flaring,cement
for i=0,ncountry1-1 do begin
  temp = res[aa[i] + 1 : aa[i + 1]]
  ; print,countrystr[i],aa[i]+1,aa[i+1]
  bb = where(temp.field2 ge yr_start)
  if bb[0] ne -1 then begin
    ; if i eq 71 then stop
    temp = temp[bb]
    for j = 0, 5 do begin
      country_cdiac[*, i, j] = temp.(j + 3) ; total, gas, oil, coal, flaring, cement
    endfor
  endif else begin
    country_cdiac[*, i, *] = -999 ; for countries with no data yr_start or after
    countrystr2[i] = 'NODATA'
  endelse
endfor

; eliminate countries with NODATA
dd = where(countrystr2 ne 'NODATA', complement = cc, countdd)
country_cdiac = country_cdiac[*, dd, *]
countrystr2 = countrystr2[dd]
ncountry2 = countdd
ccg_fwrite, file = './outputs/CDIAC_countries_' + strtrim(yr_cdiac, 2) + '.txt', nc = 1, countrystr2

; extrapolate country data through 20xx using BP country-fueltype data and then trends
; note that BP country data does not include all countries, only major ones
; with residuals added into, e.g. 'S. America other' or 'Europe other'
; to deal with this fact, I have used the % increase for '[Continent] other'
; for all countries not listed in the BP data.

; read header (should be the same for all fuels)
file = './processed_inputs/EI_frac_changes_2020-2024_gas.csv'
ccg_sread, file = file, res
ccg_strtok, str = res[0], delimiter = ',', header

bb = where(fix(header) ge yr_cdiac + 1 and fix(header) le yr_final - 1, countbb)
if countbb ne n_ei_yrs then stop

fuel = ['gas', 'oil', 'coal']; the order of this array should align with the ordering of the categories in CDIAC national and global files
nfuel = fuel.length
ei_ratios = dblarr(n_ei_yrs, ncountry2, nfuel)

; ratios for coal, oil, and gas
for i = 0, nfuel - 1 do begin
  file = './processed_inputs/EI_frac_changes_2020-2024_'+fuel[i]+'.csv'
  ccg_read, file = file, data, delimiter = ',', skip = 1
  
  for j = 0, n_ei_yrs - 1 do begin
    ei_ratios[j, *, i] = data.(bb[j] + 1) ; e.g. 2009 increase over 2008, etc.    ;+1 is because field0 in ccg_read-produced structure is full string
  endfor
endfor

; 2. ratios for flaring and cement
;
; Use either:
; 1. GCP global numbers for ease/speed
; from https://data.icos-cp.eu/licence_accept?ids=%5B%226QlPjfn_7uuJtAeuGGFXuPwz%22%5D
; 2. direct source for cement is  http://minerals.usgs.gov/minerals/pubs/commodity/cement/
; From USGS PDFs.  In thousand metric tons (of cement, not CO2) of global production:

; need a "base" year.  e.g., even if CDIAC has 2020, you need 2020 to get the 2021/2020 fractional increase
; cement/flaring base years: 2021, 2022, 2023, 2024

; global portland cement production (1e3 tons) from USGS PDFs (2025 pdf has 2023 and 2024 global data, etc.)
; Always take revised/updated yr when possible (except for most recent year).
; this is the "World Total" under "Cement Production" table on page 2 that has historically just listed the last two years
cem = [4.4e6, 4.1e6, 4.1e6, 4.0e6] ; 4.2e6, 4.4e6, 4.1e6, 4.1e6, 4.0e6
ceminc = cem / shift(cem, 1)

; flaring in billions of cubic m from EI
flare = [152.7, 146.8, 157.1, 158.8] ; 148.8, 152.7, 146.8, 157.1, 158.8

flareinc = flare / shift(flare, 1)
frac_inc_c = ceminc[1 : *]
frac_inc_f = flareinc[1 : *]

extrap_ei = dblarr(n_ei_yrs + 1, ncountry2, 6)
extrap_ei[0, *, *] = country_cdiac[-1, *, *]

frac_arr = dblarr(n_ei_yrs, ncountry2, 5)
frac_arr[*, *, 0 : 2] = ei_ratios
frac_arr[*, *, 3] = rebin(frac_inc_f, n_ei_yrs, ncountry2)
frac_arr[*, *, 4] = rebin(frac_inc_c, n_ei_yrs, ncountry2)

; 1. extrapolate for coal, oil, and gas (indices 1:3)
for i = 0, n_ei_yrs - 1 do begin
  extrap_ei[i + 1, *, 1 : 5] = extrap_ei[i, *, 1 : 5] * frac_arr[i, *, *]
endfor

; now total is sum of all categories
extrap_ei[1 : -1, *, 0] = total(extrap_ei[1 : -1, *, 1 : 5], 3)

addarr = dblarr(n_extrap_yrs, ncountry2, 6)
addarr[0 : n_ei_yrs - 1, *, *] = extrap_ei[1 : n_ei_yrs, *, *]
; 2025 is identical to 2024
for i = n_ei_yrs, n_extrap_yrs - 1 do addarr[i, *, *] = addarr[n_ei_yrs - 1, *, *]

; concatenate
country_all = [country_cdiac, addarr] ; [n_total_yrs,ncountry2,6]

a = total(addarr[*, *, 0], 2)
print, 'addarr', a

;
; geographically assign countries to 1x1 grid based on NASA/GISS country grid
; https://data.giss.nasa.gov/landuse/country.html
; Note: This map is as of 1993, but doesn't provide country definitions for the countries that were Yugoslavia
;
; there was not a 1 to 1 match in the GISS country data set and the
; CDIAC country list, even after the modifications made above.
; To resolve this I did the following.
; I modified the GISS grid by:
; Combining the two Yemens into a single code (17950)
; Combining Anguilla and St. Kitts & Nevis (13750)
; Add San Marino to Italy
; Add Lesotho to S. Africa
; Add Liechtenstein to Germany
; Add all Yugoslav countries back to Yugoslavia
;
; And the following regions will not have fluxes mapped onto them:
; ANTARCTICA
; GUAM
; KERGUELEN
; OCEAN
; TUVALU
; TURKS&C.I
; Am samoa
;
; Dealing with sub-division codes.
;
; The GISS country map contains many subdivision codes for states and provinces of:
; US, Canada, Brazil, India, former USSR, Germany, Czechoslovakia, and Australia
; We need the codes for USSR and Czechoslovakia, because these are separate countries
; but don't want the codes for the other countries.  We have deleted the unwanted sub-division
; codes from COUNTRY1X1.CODE.mod.csv, but not from COUNTRY1X1.1993.mod.txt.  This means, in this code,
; that we have to match these countries based only on the first 3 digits.
; The primary codes for these countries are:
;
; Additionally, the former Yugoslav republic is now divided into numerous countries.
; To deal with this, I have combined all the Yugoslav countries as mentioned above
; (Macedonia, Yugoslavia, Croatia, Slovenia, and Bosnia)

; read in modified GISS map and country identifiers
; note that the identifiers have been mapped offline into the order
; of countries as defined by CDIAC
; note: mod2 refers to aggregation of all of former Yugoslavia

; This has Western Sahara and Netherland Antilles deleted
file1 = './inputs/COUNTRY1X1.CODE.mod2.2013.csv'
ccg_read, file = file1, delimiter = ',', codes

file2 = './inputs/COUNTRY1X1.1993.mod.txt'
gissmap = lonarr(360, 180)
temp = ''
openr, unit, file2, /get_lun
for i = 0, 2 do readf, unit, temp
readf, unit, gissmap
free_lun, unit

; country totals will be weighted by emissions patterns in the country,
; for which we'll use EDGAR patterns computed using edgar_fracv80.pro
restore, './processed_inputs/fracarr_2026.sav' ; global EDGAR pixel fractions (at 1x1) from yr_start through yr_final
flux_annual = dblarr(n_total_yrs, 360, 180, 6)
for ii = 0, n_total_yrs - 1 do begin
  tempfrac = reform(fracarr[ii, *, *])
  for i = 0, ncountry2 - 1 do begin
    ; for four cases below use subdivision codes, otherwise, not.
    if (codes[i].field1 / 100 eq 41 || $ ; Czech
      codes[i].field1 / 100 eq 172 || $ ; USSR
      codes[i].field1 / 100 eq 137 || $ ; St Kitts&Nevis
      codes[i].field1 / 100 eq 179) then $ ; Yemen
      temp = gissmap else temp = gissmap / 100 * 100
    ee = where(temp eq codes[i].field1, ncells)
    idx = array_indices(temp, ee)
    fractot = total(tempfrac[ee], /double)
    ; if ncells eq 1 then no geographic scaling required
    if ncells gt 1 && fractot gt 0 then begin
      for j = 0, ncells - 1 do begin
        flux_annual[ii, idx[0, j], idx[1, j], *] = country_all[ii, i, *] * tempfrac[idx[0, j], idx[1, j]] / fractot
      endfor
    endif else if ncells ge 1 && fractot eq 0 then begin
      ; EDGAR pattern is all zeros; distribute evenly across cells
      for j = 0, ncells - 1 do begin
        flux_annual[ii, idx[0, j], idx[1, j], *] = country_all[ii, i, *] / ncells
      endfor
    endif else if ncells eq 1 then begin
      flux_annual[ii, idx[0, 0], idx[1, 0], *] = country_all[ii, i, *]
    endif
  endfor
endfor

; now add bunker fuels
; these will be mapped onto oceanic areas according to EDGAR fracarr.
; ;1. read in CT basis region map to use as a land mask.
; ;this is a 720x360 array
; ;regions 0-11 are ice and land regions,else ocean
; file='/Users/john/idl/from_wouter/gmd_basemap.nc'
; a=nc2struct(file)
; ct_grid=a.region_index
; aa=where(ct_grid gt 11, complement=bb)
; ct_grid[aa]=1.      ;ocean regions
; ct_grid[bb]=0.
; ct_grid1x1=rebin(ct_grid,360,180)   ;this new 360 x 180 array will contain values of 0,0.25,0.5,0.75, and 1.0
; ;to define oceans cleanly we will only use values of 1.0 to represent ocean
; ocemask=fix(ct_grid1x1)

; NOTE: Using CT basis regions to define land/ocean was problematic because Black, Baltic and Mediterranean Seas were all included in land
; This meant that when 'convolving' ocemask calculated in this way with fracarr, no emissions were placed in these seas.
; Instead, we will use the ocean grid cells from the gissmap array (=0). This also has the advantage of ensuring consistency -- i.e. that
; there will not be any coastal gridcells neither defined as land nor ocean.

ocemask = gissmap
aa = where(gissmap eq 0, complement = bb)
ocemask[aa] = 1
ocemask[bb] = 0

ocearr = matrix_repeat(ocemask, n_total_yrs) * fracarr
ocetot = total(total(ocearr, 2), 2)

; attribute difference between global and country totals to 'bunker' fuels and assign to
; oceanic shipping routes
; before calculating the difference, we need to extrapolate glob_cdiac to yr_final
; we will do this by sector

ei_glob_inc = dblarr(n_ei_yrs, 3)

for i = 0, 2 do begin ; loop over fuel types
  ; read in global percentage increase files
  ccg_fread, file = './processed_inputs/EI_frac_changes_2020-2024_global_'+fuel[i]+'.csv', nc = 1, skip=1, globperc
  ei_glob_inc[*, i] = globperc[-(n_ei_yrs) : -1]
endfor

addtot = dblarr(n_extrap_yrs, 6)
; extrapolate coal, oil, gas using EI global ratios
for i = 0, n_ei_yrs - 1 do begin
  if i eq 0 then addtot[i, 1 : 3] = glob_cdiac[n_cdiac_yrs - 1, 1 : 3] * ei_glob_inc[i, *] $
  else addtot[i, 1 : 3] = addtot[i - 1, 1 : 3] * ei_glob_inc[i, *]
endfor
; flat extrapolation for years beyond EI coverage
for i = n_ei_yrs, n_extrap_yrs - 1 do addtot[i, 1 : 3] = addtot[n_ei_yrs - 1, 1 : 3] * 1.00

; now cement and flaring
; first cement
for i = 0, n_ei_yrs - 1 do begin
  if i eq 0 then addtot[i, 5] = glob_cdiac[n_cdiac_yrs - 1, 5] * frac_inc_c[i] $
  else addtot[i, 5] = addtot[i - 1, 5] * frac_inc_c[i]
endfor
for i = n_ei_yrs, n_extrap_yrs - 1 do addtot[i, 5] = addtot[n_ei_yrs - 1, 5] * 1.00

; then flaring
for i = 0, n_ei_yrs - 1 do begin
  if i eq 0 then addtot[i, 4] = glob_cdiac[n_cdiac_yrs - 1, 4] * frac_inc_f[i] $
  else addtot[i, 4] = addtot[i - 1, 4] * frac_inc_f[i]
endfor
for i = n_ei_yrs, n_extrap_yrs - 1 do addtot[i, 4] = addtot[n_ei_yrs - 1, 4] * 1.00

; now total
addtot[*, 0] = total(addtot, 2)

print, addtot[*, 0]

; concatenate with glob_cdiac
glob_all = [glob_cdiac, addtot]

glob_country_sum = total(country_all, 2)
bunker = glob_all - glob_country_sum
bunkarr = dblarr(n_total_yrs, 360, 180, 6)
for i = 0, n_total_yrs - 1 do begin
  if ocetot[i] gt 0 then begin
    for j = 0, 5 do bunkarr[i, *, *, j] = bunker[i, j] * ocearr[i, *, *] / ocetot[i]
  endif
endfor

flux_with_bunker = flux_annual + bunkarr ; this ensures that 'bunker' fuels do not replace other fuels but are added.
; otherwise, there could be potential problems for coastal areas.

flux_with_bunker = reform(flux_with_bunker[*, *, *, 0]) ; redefine just as total flux (no need now for component sectors)

; --at this point we have x years (1992-y) of patterned 1x1 fluxes that total to cdiac totals
; we still need to:
;
; 2. interpolate to create monthly arrays
; 3. add seasonality using Blasing et al harmonics
; other possibile adjustments:
; 4. for seasonality
; a. remove 2nd harmonic for fluxes outside of US
; b. adjust amplitude of 1st harmonic according to latitude
; or seasonal temperatures
; 5. level of emissions
; a. break out airplane emissions into separate category
; and place at another level.

; 2. create monthly arrays
; interpolate to create monthly values
; time span of interpolation is from 1992.0 to yr_final, inclusive

; ;Original formulation to divide into months
; ;   To apply different lengths per month, we now need to interpolate daily and then recombine

; calculate ndays over n_total_yrs, including leap years
leapdays = 0
for i = yr_start, yr_final do leapdays = leapdays + ccg_leapyear(i)
ndays = n_total_yrs * 365. + leapdays

; initial jan1 index value
jan1 = 0

flux_daily = dblarr(ndays, 360, 180)
time15 = dblarr(ndays)
for i = 0, n_total_yrs - 1 do begin
  daysinyear = 365. + ccg_leapyear(yr_start + i)
  dec31 = jan1 + daysinyear - 1
  time15[jan1 : dec31] = findgen(daysinyear) / daysinyear + yr_start + i + 1 / (daysinyear * 2)
  ; increment day indices
  jan1 = dec31 + 1
endfor

x = indgen(n_total_yrs + 1) + yr_start

for i = 0, 359 do begin
  for j = 0, 179 do begin
    fit = piqs(x, flux_with_bunker[*, i, j])
    ; initialize jan1 index
    jan1 = 0
    for k = 0, n_total_yrs - 1 do begin
      ; define time indices
      daysinyear = 365. + ccg_leapyear(yr_start + k)
      dec31 = jan1 + daysinyear - 1
      year1 = x[k]
      year2 = x[k + 1]
      aa = where(time15 ge year1 and time15 le year2)
      flux_daily[jan1 : dec31, i, j] = (time15[aa] - x[k]) ^ 2 * fit[0, k] + (time15[aa] - x[k]) * fit[1, k] + fit[2, k]
      ; overwrite negative values caused by piqs with constant annual values -- i.e. all months the same
      ; piqs evaluations f(t_d) represent annual rates (Gg C/yr), NOT daily fractions.
      ; The monthly binning (total/ndays) averages these, so each daily slot should hold
      ; the annual value, not annual/daysinyear.
      temp = flux_daily[jan1 : dec31, i, j]
      zz = where(temp lt 0, complement = yy, countzz)
      if zz[0] ne -1 then flux_daily[jan1 : dec31, i, j] = flux_with_bunker[k, i, j]

      ; increment day indices
      jan1 = dec31 + 1
    endfor
  endfor
endfor

; now place daily values in monthly bins
fluxarr2 = dblarr((n_total_yrs * 12), 360, 180)
time2 = findgen(n_total_yrs * 12) / 12. + yr_start + 1 / (12. * 2.)
x = indgen(n_total_yrs + 1) + yr_start

idx0 = 0
for i = 0, n_total_yrs * 12 - 1 do begin
  res = month2sec(leapyr = ccg_leapyear(yr_start + i / 12), daysinmonth = daysinmonth)
  ndays = daysinmonth[i mod 12]
  idx1 = idx0 + ndays - 1
  fluxarr2[i, *, *] = total(flux_daily[idx0 : idx1, *, *], 1, /double) / ndays
  ; increment idx0 for next loop iteration
  idx0 = idx1 + 1
endfor

; destroy flux_daily
flux_daily = 0
; fluxarr15isof = 0

; ;c no interpolation:  i.e. flat emissions for each month in a year, but with conservation of annual total and no negative values.
; time2=findgen(n_total_yrs*12)/12.+yr_start+1/24
; fluxarr2=fltarr((n_total_yrs*12),360,180)
; fluxarr2nb=fltarr((n_total_yrs*12),360,180)
; for k=0,n_total_yrs*12-1 do begin
; fluxarr2[k,*,*]=flux_with_bunker[k/12,*,*]
; fluxarr2nb[k,*,*]=fluxarr1nb[k/12,*,*]
; endfor

; ;identify negative values created by piqs algorithm (which is a spline that preserves the integral flux for each year
; ;and at the same time prevents jumps in flux at the annual borders.)
; idxarr=[0,0]
; for i=0,n_total_yrs*12-1 do begin
; aa=where(fluxarr2[i,*,*] lt 0)
; bb=where(fluxarr2[i,*,*] eq 0)
; if aa[0] ne -1 then begin
; idx=array_indices([360,180],aa,/dimensions)
; idxarr=[[idxarr],[idx]]
; endif
; endfor
; idxarr=idxarr[*,1:*]
; a=size(idxarr)
; npts=a[2]


ff_monthly = temporary(fluxarr2)
ff_time = time2

; 3 add seasonality
; put in seasonality for USA based on average seasonality of emissions
; from Blasing et al 2004 CDIAC
if season ne '' then begin
  ; read in blasing data
  ccg_fread, file = './inputs/emis_mon_usatotal_2col.txt', nc = 2, monthff
  ; fit smooth curve to data and extract the average seasonal cycle (centered on zero)
  ccg_ccgvu, x = monthff[0, *], y = monthff[1, *], fsc = fsc, sc = sc, coef = coef, ftn = ftn
  ; normalize fsc as percentage of total
  seasff = fsc[1, 0 : 11] / mean(monthff[1, 12 : 23])
  ; apply seasonal cycle to all months in N. Am. [30 - 60N; 60-140W]
  seasff2 = matrix_repeat(seasff, n_total_yrs, /vector, /swap) ; repeats average sc over n_total_yrs years

  if season eq 'nam' then begin
    lonidxmin = 40
    lonidxmax = 120
    latidxmin = 120
    latidxmax = 150
  endif else if season eq 'glb' then begin
    lonidxmin = 0
    lonidxmax = 359
    latidxmin = 0
    latidxmax = 179
  endif ; else if season eq 'glb2' then begin

  nx = lonidxmax - lonidxmin + 1
  ny = latidxmax - latidxmin + 1
  seasff3 = fltarr((n_total_yrs) * 12, nx, ny)
  for i = 0, nx - 1 do for j = 0, ny - 1 do seasff3[*, i, j] = seasff2
  region = ff_monthly[*, lonidxmin : lonidxmax, latidxmin : latidxmax]
  region_seas = temporary(region) * (seasff3 + 1)
  ff_monthly[*, lonidxmin : lonidxmax, latidxmin : latidxmax] = temporary(region_seas)

  if seas2 eq 'euras' && season ne 'glb' then begin
    ; read in seasonal adjustment factors -- provisional from EDGAR -- based on W. Europe
    ccg_fread, file = './inputs/eurasian_seasff.txt', skip = 3, nc = 1, seasffa
    seasff2a = matrix_repeat(seasffa, n_total_yrs, /vector, /swap) ; repeats average sc over n_total_yrs years
    lonidxmin = 160
    lonidxmax = 350
    latidxmin = 120
    latidxmax = 150
    nx = lonidxmax - lonidxmin + 1
    ny = latidxmax - latidxmin + 1
    seasff3a = fltarr((n_total_yrs) * 12, nx, ny)
    for i = 0, nx - 1 do for j = 0, ny - 1 do seasff3a[*, i, j] = seasff2a
    region = ff_monthly[*, lonidxmin : lonidxmax, latidxmin : latidxmax]
    region_seas = temporary(region) * (seasff3a)
    ff_monthly[*, lonidxmin : lonidxmax, latidxmin : latidxmax] = temporary(region_seas)
  endif
endif

sav_file = './outputs/ff_monthly_2026.sav'
save, ff_monthly, ff_time, filename = sav_file
print, 'saved ', sav_file

end
