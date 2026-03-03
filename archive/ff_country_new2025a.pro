; pro ff_country_new2025a, dev=dev, season=season

; intended to be an improvement on ff_hires, by using annual country data from
; cdiac (until 2004) and bp (2004-) instead of global edgar patterns

; ;;this version *2009*  is slightly modified to take account of the 2008- global recession
; ;;by not assuming a linearly increasing emission after 2007, but keeping 2008 and 2009 at the same total as 2007

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

; ALSO.... [not sure which year this is associated with!]
; after many years CDIAC made both country and global numbers gas,oil,coal
; previously, country totals had coal,oil,gas

compile_opt idl3

; "iso" version with  isotopic ratios by fossil type (no space or time variation yet)
dgas = -40.d
dcoal = -26.d
doil = -28.d
dflare = dgas
dcem = 0.d

season = 'nam'
; season=''
; season='glb'
; seas2=''
seas2 = 'euras'

; will still use edgar 1x1 patterns for within country patterning of country totals

; time ranges
yr1 = 1993
yr2 = 2021 ; final year of cdiac data
yrbp = 2023 ; final year of BP data
yr3 = 2025 ; final full year for extrapolated emissions
bpyears = yrbp - yr2 ; percent increases for 2022, 2023 ( 2024, 2025, no data available, so use guess of 1.00 (0% or flat), which has been roughly true globally for 2015 - 2017)

nyears1 = yr2 - yr1 + 1
nyears2 = yr3 - yr2
nyears3 = nyears2 + nyears1
;time = findgen(nyears3) + 0.5 + yr1

; read in cdiac global totals
; This file is made by: 1) converting the CDIAC@appstate .xlsx file to .csv, 2) adding zeros to the per capita emission column as necessary
; 3)saving just the values (no headers) as an MS-DOS .txt file
; later, we will subtract from sum of country totals and this difference will be applied to the oceans
; NOTE!!! Order of columns has changed:  now Year, Total, Solid, Liquid, Gas, Cement, Flaring, Per Capita
; Was: Year, Total, Gas, Liquid, Solid, Cement, Flaring, Per Capita
;
; 2024b: Intermediate CSVs produced by injest.ipynb format both global and national as : Year, Total, Gas, Liquid, Solid, Flaring, Cement
; They are both also converted to gigagrams carbon. This removes some of the fiddly manipulation here.

; ccg_fread,file='/Users/john/EDGAR/CDIAC_historical/global/glb_fos_'+strtrim(yr2,2)+'.txt',nc=8,skip=0,result
; cdiacff=result[1:6,*]*1000.
ccg_fread, file = './processed_inputs/CDIAC_global_2020.csv', nc=7, skip=1, result
cdiacff = result[1:6, *]
cdiactime = result[0, *]

ranged_rows = where(cdiactime ge yr1 and cdiactime le yr2)
globtot = cdiacff[*, ranged_rows]

; 2024b: No need to switch, both files are in order Year, Total, Gas, Liquid, Solid, Flaring, Cement
;
; globaltot array has following order: tot,gas,liquid,solid,cement,flaring
; countryarr has this: tot,gas,liquid,solid,flaring,cement
; switch columns in globtot
; cem = globtot[4, *]
; flr = globtot[5, *]
; globtot[4, *] = flr
; globtot[5, *] = cem
globtot = transpose(globtot)

; read in cdiac country data
; File from read_cdiac_nation_csv.pro
; 2024b: file from injest.ipynb

;file = '/Users/john/EDGAR/CDIAC_historical/national/nation.1751_' + strtrim(yr2, 2) + '.mod2.csv'
;ccg_read, file = file, skip = 0, delimiter = ',', res
ccg_read, file='./processed_inputs/CDIAC_national_2020.csv', skip=1, delimiter=',', res
country = res.field1
;yr = res.field2
;fftot = res.field3

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
;

;
;
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

countryarr = lonarr(nyears1, ncountry1, 6)
countrystr = res[aa[1 : *]].field1
countrystr2 = countrystr

; fill up array of country ff data: total and 5 sectors:gas,liquid,solid,flaring,cement
for i=0,ncountry1-1 do begin
  temp = res[aa[i] + 1 : aa[i + 1]]
  ; print,countrystr[i],aa[i]+1,aa[i+1]
  bb = where(temp.field2 ge yr1)
  if bb[0] ne -1 then begin
    ; if i eq 71 then stop
    temp = temp[bb]
    for j = 0, 5 do begin
      countryarr[*, i, j] = temp.(j + 3) ; total, gas, oil, coal, flaring, cement
    endfor
  endif else begin
    countryarr[*, i, *] = -999 ; for countries with no data yr1 or after
    countrystr2[i] = 'NODATA'
  endelse
endfor

; eliminate countries with NODATA
dd = where(countrystr2 ne 'NODATA', complement = cc, countdd)
countryarr = countryarr[*, dd, *]
countrystr2 = countrystr2[dd]
ncountry2 = countdd
ccg_fwrite, file = './outputs/CDIAC_countries_2020' + strtrim(yr2, 2) + '.txt', nc = 1, countrystr2

; ;extrapolate country data through 20xx using BP country-fueltype data and then trends
; ;note that BP country data does not include all countries, only major ones
; ;with residuals added into, e.g. 'S. America other' or 'Europe other'
; ;to deal with this fact, I have used the % increase for '[Continent] other'
; ;for all countries not listed in the BP data.

; read header (should be the same for all fuels)
file = './processed_inputs/EI_frac_changes_2020-2023_gas.csv'
ccg_sread, file = file, res
ccg_strtok, str = res[0], delimiter = ',', header

bb = where(fix(header) ge yr2 + 1 and fix(header) le yr3 - 1, countbb)
if countbb ne bpyears then stop

fuel = ['gas', 'coal', 'oil']; the order of this array should align with the ordering of the categories in CDIAC national and global files
nfuel = fuel.length
bparr = fltarr(bpyears, ncountry2, nfuel)

; ratios for coal, oil, and gas
for i = 0, nfuel - 1 do begin
  file = './processed_inputs/EI_frac_changes_2020-2023_'+fuel[i]+'.csv'
  ccg_read, file = file, data, delimiter = ',', skip = 1
  
  for j = 0, bpyears - 1 do begin
    bparr[j, *, i] = data.(bb[j] + 1) ; e.g. 2009 increase over 2008, etc.    ;+1 is because field0 in ccg_read-produced structure is full string
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
cem_flare_yr = [ 2021, 2022, 2023]

; global portland cement production (1e3 tons) from USGS PDFs (2023 pdf will have 2021 and 2022 global data, etc.)
; Always take revised/updated yr when possible (except for most recent year).
; this is the "World Total" under "Cement Production" table on page 2 that has historically just listed the last two years
cem = [4.4e6, 4.1e6, 4.1e6] ; 4.2e6, 4.4e6, 4.1e6, 4.1e6
ceminc = cem / shift(cem, 1)

; flaring in billions of cubic m from BP
flare = [152.7, 146.8, 157.1] ; 148.8, 152.7, 146.8, 157.1

flareinc = flare / shift(flare, 1)
frac_inc_c = ceminc[1 : *]
frac_inc_f = flareinc[1 : *]

addarrbp = fltarr(bpyears + 2, ncountry2, 6)
addarrbp[0, *, *] = countryarr[-1, *, *]

frac_arr = fltarr(bpyears, ncountry2, 5)
frac_arr[*, *, 0 : 2] = bparr
frac_arr[*, *, 3] = rebin(frac_inc_c, 2, ncountry2)
frac_arr[*, *, 4] = rebin(frac_inc_f, 2, ncountry2)

; 1. extrapolate for coal, oil, and gas (indices 1:3)
for i = 0, bpyears - 1 do begin
  addarrbp[i + 1, *, 1 : 5] = addarrbp[i, *, 1 : 5] * frac_arr[i, *, *]
endfor

; now total is sum of all categories
addarrbp[1 : -1, *, 0] = total(addarrbp[1 : -1, *, 1 : 5], 3)

addarr = fltarr(bpyears + 2, ncountry2, 6)
addarr[0 : -2, *, *] = addarrbp[1 : -1, *, *]
; 2024 is identical to 2023
addarr[-1, *, *] = addarr[-2, *, *]

; concatenate
countryarr2 = [countryarr, addarr] ; [nyears3,ncountry2,6]

a = total(addarr[*, *, 0], 2)
print, 'addarr', a

countryarr2isof = countryarr2 * 0.
countryarr2isof[*, *, 1] = countryarr2[*, *, 1] * dcoal
countryarr2isof[*, *, 2] = countryarr2[*, *, 2] * doil
countryarr2isof[*, *, 3] = countryarr2[*, *, 3] * dgas
countryarr2isof[*, *, 4] = countryarr2[*, *, 4] * dcem
countryarr2isof[*, *, 5] = countryarr2[*, *, 5] * dflare
countryarr2isof[*, *, 0] = total(countryarr2isof[*, *, 1 : 5], 3)

countryarr2iso = countryarr2isof[*, *, 0] / countryarr2[*, *, 0]


; print out totals for all countries
; ; format1 = '(' + strtrim(fix(ncountry2 + 1)) + 'A52)'
; ; format2 = '(' + strtrim(fix(ncountry2 + 1)) + 'F52.8)'
; ; allarr = transpose(reform(countryarr2[*, *, 0]) / 1.e6)
; ; yrarr = findgen(nyears3) + yr1 + 0.5
; ; allarr = [reform(yrarr, 1, nyears3), [allarr]]
; ; outfile2 = './outputs/cdiacff_all' + strtrim(yr3, 2) + '_extrap.txt'
; ; openw, lun, outfile2, /get_lun
; ; printf, lun, ['year', countrystr2], format = format1
; ; printf, lun, allarr, format = format2
; ; free_lun, lun

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
;

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
; for which we'll use EDGAR patterns
; computed using edgar_fracv80.pro
restore, './processed_inputs/fracarr_2025a.sav' ; global EDGAR pixel fractions (at 1x1) from yr1 through yr3
fluxarr = dblarr(nyears3, 360, 180, 6)
delarr = dblarr(nyears3, 360, 180)
ncelltot = 0l
eetot = [0]
for ii = 0, nyears3 - 1 do begin
  tempfrac = reform(fracarr[ii, *, *]) ; TODO: 1 year short
  for i = 0, ncountry2 - 1 do begin
    ; for four cases below use subdivision codes, otherwise, not.
    if (codes[i].field1 / 100 eq 41 or $ ; Czech
      codes[i].field1 / 100 eq 172 or $ ; USSR
      codes[i].field1 / 100 eq 137 or $ ; St Kitts&Nevis
      codes[i].field1 / 100 eq 179) then $ ; Yemen
      temp = gissmap else temp = gissmap / 100 * 100
    ee = where(temp eq codes[i].field1, ncells)
    idx = array_indices(temp, ee)
    fractot = total(tempfrac[ee], /double)
    ; if ncells eq 1 then no geographic scaling required
    if ncells gt 1 then begin
      for j = 0, ncells - 1 do begin
        fluxarr[*, idx[0, j], idx[1, j], *] = countryarr2[*, i, *] * tempfrac[idx[0, j], idx[1, j]] / fractot
        delarr[*, idx[0, j], idx[1, j]] = countryarr2iso[*, i] * tempfrac[idx[0, j], idx[1, j]] / fractot
      endfor
    endif else if ncells eq 1 then begin
      for j = 0, ncells - 1 do begin
        fluxarr[*, idx[0, j], idx[1, j], *] = countryarr2[*, i, *]
        delarr[*, idx[0, j], idx[1, j]] = countryarr2iso[*, i]
      endfor
    endif
    ncelltot = ncelltot + ncells
    eetot = [eetot, ee]
    ; if i eq usaidx then stop
  endfor
endfor

fluxarrisof = fluxarr[*, *, *, 0] * delarr

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

ocearr = matrix_repeat(ocemask, nyears3) * fracarr
ocetot = total(total(ocearr, 2), 2)

; attribute difference between global and country totals to 'bunker' fuels and assign to
; oceanic shipping routes
; before calculating the difference, we need to extrapolate globtot to yr3
; we will do this by sector

; bp global increases for 2009, and 2010 (over previous years)
; bpoil=[0.997581559,  0.980353039]
; bpcoal=[1.032128388, 0.997535297]
; bpgas=[1.024584292, 0.976373606]
; bpglobinc=[[bpcoal],[bpoil],[bpgas]]

bpglobinc = fltarr(bpyears, 3)

for i = 0, 2 do begin ; loop over fuel types
  ; read in global percentage increase files
  ;ccg_fread, file = '/Users/john/EDGAR/bp/bp_' + fuel[i] + '_1966_' + strtrim(yr3 - 1, 2) + '_frac_global.txt', nc = 1, globperc
  ccg_fread, file = './processed_inputs/EI_frac_changes_2020-2023_global_'+fuel[i]+'.csv', nc = 1, skip=1, globperc
  bpglobinc[*, i] = globperc[-(bpyears) : -1]
endfor

addtot = fltarr(nyears2, 6)
; first extrapolate coal, oil, gas
addtot[0, 1 : 3] = globtot[nyears1 - 1, 1 : 3] * bpglobinc[0, *] ; 2022
addtot[1, 1 : 3] = addtot[0, 1 : 3] * bpglobinc[1, *] ; 2023
addtot[2, 1 : 3] = addtot[1, 1 : 3] * 1.00 ; 2024
addtot[3, 1 : 3] = addtot[2, 1 : 3] * 1.00 ; 2025

; now cement and flaring
; first cement
addtot[0, 5] = globtot[nyears1 - 1, 5] * frac_inc_c[0] ; 2022
addtot[1, 5] = addtot[0, 5] * frac_inc_c[1] ; 2023
addtot[2, 5] = addtot[1, 5] * 1.00 ; 2024
addtot[3, 5] = addtot[2, 5] * 1.00 ; 2025

; then flaring
addtot[0, 4] = globtot[nyears1 - 1, 4] * frac_inc_f[0] ; 2022
addtot[1, 4] = addtot[0, 4] * frac_inc_f[1] ; 2023
addtot[2, 4] = addtot[1, 4] * 1.00 ; 2024
addtot[3, 4] = addtot[2, 4] * 1.00 ; 2025

; now total
addtot[*, 0] = total(addtot, 2)

print, addtot[*, 0]

; concatenate with globtot
globtot2 = [globtot, addtot]

; write out glbtot2 for use by other programs
; addtot and cdiacff have different orders of cement and flaring
; globtot was earlier modified to match country data, but now switch back
; to keept to conform with cdiac global total order (http://cdiac.ornl.gov/ftp/ndp030/global.1751_2006.ems)
; small note: flr and cem labels below were incorrect in previous (pre-2018a?) versions:  i.e. addtot[*,4] is flaring and [*,5] is cement.
; this has now been corrected, BUT, previous version would not have resulted in an error, because cem and flr are just temp. labels.
flr = addtot[*, 4]
cem = addtot[*, 5]
addtot[*, 4] = cem
addtot[*, 5] = flr

cdiacffnew = [transpose(cdiacff), addtot] / 1.e6
cdiactimenew = [reform(cdiactime), findgen(nyears2) + yr2 + 1]
; outfile='/Users/john/idl/co2/cdiacff_glb'+strtrim(yr3,2)+'_extrap.txt'
;
; ccg_fwrite,file=outfile,nc=7,cdiactimenew,$
; cdiacffnew[*,0],$
; cdiacffnew[*,1],$
; cdiacffnew[*,2],$
; cdiacffnew[*,3],$
; cdiacffnew[*,4],$
; cdiacffnew[*,5]

globtot3 = total(countryarr2, 2)
bunker = globtot2 - globtot3
bunkarr = dblarr(nyears3, 360, 180, 6)
for i = 0, nyears3 - 1 do for j = 0, 5 do bunkarr[i, *, *, j] = bunker[i, j] * ocearr[i, *, *] / ocetot[i]

fluxarr1 = fluxarr + bunkarr ; this ensures that 'bunker' fuels do not replace other fuels but are added.
; otherwise, there could be potential problems for coastal areas.

bunkarrisof = bunkarr * doil
fluxarr1isof = fluxarrisof + bunkarrisof

fluxarr1 = reform(fluxarr1[*, *, *, 0]) ; redefine just as total flux (no need now for component sectors)
fluxarr1nb = reform(fluxarr[*, *, *, 0]) ; redefine just as total flux (no need now for component sectors)

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
; time span of interpolation is from 1992.0 to yr3, inclusive

; ;a. linear interpolation: note that this method does not conserve annual totals
; fluxarr2=congrid(fluxarr1,(nyears3-1)*12,360,180,/minus_one,/interp)
; time2=congrid(time,(nyears3-1)*12,/minus_one,/interp)

; b  cubic interpolation with conservation of annual totals using the 'piqs' algorithm
; this method produces some negative flux values when annual values for certain countries approach zero rapidly
; see Somalia, Namibia and Netherlands Antilles for examples.

; ;Original formulation to divide into months
; ;   To apply different lengths per month, we now need to interpolate daily and then recombine
; fluxarr2=fltarr((nyears3*12),360,180)
; fluxarr2nb=fltarr((nyears3*12),360,180)
; time2=findgen(nyears3*12)/12.+yr1+1/(12.*2.)
; x=indgen(nyears3+1)+yr1

; calculate ndays over nyears3, including leap years
leapdays = 0
for i = yr1, yr3 do leapdays = leapdays + ccg_leapyear(i)
ndays = nyears3 * 365. + leapdays

; initial jan1 index value
jan1 = 0

fluxarr15 = dblarr(ndays, 360, 180)
; fluxarr15isof = dblarr(ndays,360,180)
; fluxarr15nb=fltarr(ndays,360,180)
time15 = fltarr(ndays)
for i = 0, nyears3 - 1 do begin
  daysinyear = 365. + ccg_leapyear(yr1 + i)
  dec31 = jan1 + daysinyear - 1
  time15[jan1 : dec31] = findgen(daysinyear) / daysinyear + yr1 + i + 1 / (daysinyear * 2)
  ; increment day indices
  jan1 = dec31 + 1
endfor

x = indgen(nyears3 + 1) + yr1

for i = 0, 359 do begin
  for j = 0, 179 do begin
    fit = piqs(x, fluxarr1[*, i, j])
    ; fit2=piqs(x,fluxarr1isof[*,i,j])
    ; fitnb=piqs(x,fluxarr1nb[*,i,j])
    ; initialize jan1 index
    jan1 = 0
    for k = 0, nyears3 - 1 do begin
      ; define time indices
      daysinyear = 365. + ccg_leapyear(yr1 + k)
      dec31 = jan1 + daysinyear - 1
      year1 = x[k]
      year2 = x[k + 1]
      aa = where(time15 ge year1 and time15 le year2)
      fluxarr15[jan1 : dec31, i, j] = (time15[aa] - x[k]) ^ 2 * fit[0, k] + (time15[aa] - x[k]) * fit[1, k] + fit[2, k]
      ; fluxarr15isof[jan1:dec31,i,j]=(time15[aa]-x[k])^2*fit[0,k]+(time15[aa]-x[k])*fit2[1,k]+fit2[2,k]
      ; fluxarr15nb[jan1:dec31,i,j]=(time15[aa]-x[k])^2*fitnb[0,k]+(time15[aa]-x[k])*fitnb[1,k]+fitnb[2,k]
      ; overwrite negative values caused by piqs with constant annual values -- i.e. all months the same
      temp = fluxarr15[jan1 : dec31, i, j]
      ; temp2=fluxarr15isof[jan1:dec31,i,j]
      zz = where(temp lt 0, complement = yy, countzz)
      ; zz2=where(temp2 lt 0,complement=yy2,countzz2)
      if zz[0] ne -1 then fluxarr15[jan1 : dec31, i, j] = fluxarr1[k / 12, i, j] / 12.
      ; if zz2[0] ne -1 then fluxarr15isof[jan1:dec31,i,j]=fluxarr1isof[k/12,i,j]/12.

      ; increment day indices
      jan1 = dec31 + 1
    endfor
  endfor
endfor

; now place daily values in monthly bins
fluxarr2 = dblarr((nyears3 * 12), 360, 180)
; fluxarr2isof = dblarr((nyears3*12),360,180)
; fluxarr2nb=fltarr((nyears3*12),360,180)
time2 = findgen(nyears3 * 12) / 12. + yr1 + 1 / (12. * 2.)
x = indgen(nyears3 + 1) + yr1

idx0 = 0
for i = 0, nyears3 * 12 - 1 do begin
  res = month2sec(leapyr = ccg_leapyear(yr1 + i / 12), daysinmonth = daysinmonth)
  ndays = daysinmonth[i mod 12]
  ;ndays = daysinmonth(i mod 12)
  idx1 = idx0 + ndays - 1
  fluxarr2[i, *, *] = total(fluxarr15[idx0 : idx1, *, *], 1, /double) / ndays
  ; fluxarr2isof[i,*,*]=total(fluxarr15isof[idx0:idx1,*,*],1,/double)/ndays
  ; increment idx0 for next loop iteration
  idx0 = idx1 + 1
endfor

; destroy fluxarr15
fluxarr15 = 0
; fluxarr15isof = 0

; ;c no interpolation:  i.e. flat emissions for each month in a year, but with conservation of annual total and no negative values.
; time2=findgen(nyears3*12)/12.+yr1+1/24
; fluxarr2=fltarr((nyears3*12),360,180)
; fluxarr2nb=fltarr((nyears3*12),360,180)
; for k=0,nyears3*12-1 do begin
; fluxarr2[k,*,*]=fluxarr1[k/12,*,*]
; fluxarr2nb[k,*,*]=fluxarr1nb[k/12,*,*]
; endfor

; ;identify negative values created by piqs algorithm (which is a spline that preserves the integral flux for each year
; ;and at the same time prevents jumps in flux at the annual borders.)
; idxarr=[0,0]
; for i=0,nyears3*12-1 do begin
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
;
; ;plot locations on a map
; map_set,/continents
; for i=0,npts-1 do plots,idxarr[0,i]-180.,idxarr[1,i]-90.,psym=1,color=fsc_color('blue')

fluxarr3 = temporary(fluxarr2)
; fluxarr3isof=temporary(fluxarr2isof)
; fluxarr3nb=temporary(fluxarr2nb)
time3 = time2

; shift arrays by 1/2 month to correspond to middle of months
; fluxarr3=(fluxarr2[1:*,*,*]+fluxarr2[0:(nyears3-1)*12-1,*,*])/2
; time3=time2[1:*]-1./24

; 3 add seasonality
; put in seasonality for USA based on average seasonality of emissions
; from Blasing et al 2004 CDIAC
if keyword_set(season) then begin
  ; read in blasing data
  ccg_fread, file = './inputs/emis_mon_usatotal_2col.txt', nc = 2, monthff
  ; fit smooth curve to data and extract the average seasonal cycle (centered on zero)
  ccg_ccgvu, x = monthff[0, *], y = monthff[1, *], fsc = fsc, sc = sc, coef = coef, ftn = ftn
  ; normalize fsc as percentage of total
  seasff = fsc[1, 0 : 11] / mean(monthff[1, 12 : 23])
  ; apply seasonal cycle to all months in N. Am. [30 - 60N; 60-140W]
  seasff2 = matrix_repeat(seasff, nyears3, /vector, /swap) ; repeats average sc over nyears2-1

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
  seasff3 = fltarr((nyears3) * 12, nx, ny)
  for i = 0, nx - 1 do for j = 0, ny - 1 do seasff3[*, i, j] = seasff2
  region = fluxarr3[*, lonidxmin : lonidxmax, latidxmin : latidxmax]
  region_seas = temporary(region) * (seasff3 + 1)
  fluxarr3[*, lonidxmin : lonidxmax, latidxmin : latidxmax] = temporary(region_seas)

  if seas2 eq 'euras' and season ne 'glb' then begin
    ; read in seasonal adjustment factors -- provisional from EDGAR -- based on W. Europe
    ccg_fread, file = './inputs/eurasian_seasff.txt', skip = 3, nc = 1, seasffa
    seasff2a = matrix_repeat(seasffa, nyears3, /vector, /swap) ; repeats average sc over nyears2-1
    lonidxmin = 160
    lonidxmax = 350
    latidxmin = 120
    latidxmax = 150
    nx = lonidxmax - lonidxmin + 1
    ny = latidxmax - latidxmin + 1
    seasff3a = fltarr((nyears3) * 12, nx, ny)
    for i = 0, nx - 1 do for j = 0, ny - 1 do seasff3a[*, i, j] = seasff2a
    region = fluxarr3[*, lonidxmin : lonidxmax, latidxmin : latidxmax]
    region_seas = temporary(region) * (seasff3a)
    fluxarr3[*, lonidxmin : lonidxmax, latidxmin : latidxmax] = temporary(region_seas)
  endif
endif

; store fluxarr3 as .sav file
fluxarr3_filename = './outputs/fluxarr3_2025a.sav'
save, fluxarr3, filename = fluxarr3_filename
print, 'saved fluxarr3 as ', fluxarr3_filename

end
