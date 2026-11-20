;pro ff_country_new2023a, dev=dev, season=season

;intended to be an improvement on ff_hires, by using annual country data from
;cdiac (until 2004) and bp (2004-) instead of global edgar patterns


;;;this version *2009*  is slightly modified to take account of the 2008- global recession
;;;by not assuming a linearly increasing emission after 2007, but keeping 2008 and 2009 at the same total as 2007

;2009a does not assume flat emissions, but uses the BP numbers for 2008.  For 2009, in some cases, negative growth in FF will be extrapolated.
;Also:
;1. instead of using the glb_fos_2004 file, the CDIAC global and country updates through 2006 are available and will be used
;  the 2006 global numbers are significantly different (lower) than the 2004 numbers.  e.g., 1997-2004 appear to be ~0.3 PgC/yr less
; This is about 2.5Pg less emission integrated over that time.  This is primarilly due to adjustments in
; liquid fuel (petroleum) globally and specifically an increase in the amount of liquid fuel that is
; not oxidized quickly, but stays put, presumably as plastics or other long lasting HC forms.

;2009b updates the edgar fractional emissions maps to version 4.  Edgar4 covers every year from through 2005.
;After that an extrapolation is used (see edgar_frac.pro)
;The spatial distribution in Edgar v4 doesn't appear to be population based, but actually derived from
;locations of different sectoral emissions.

;2010a uses new CDIAC and BP totals

;2011a is an extrapolation only

;2011b now assigns weights to months based on actual days per month

;2011c uses new BP and CDIAC inputs, through 2010 and 2008, respectively
;(and additionally these inputs are now processed in idl, not excel, via read_bp.pro and read_cdiac.pro)
; and... extrapolate cement.  Previous versions held both at 1.0 factors.  Flaring still doesn't have enough info

;2012a uses new BP and CDIAC inputs, through 2011 and 2008, respectively
;also corrects bug that prevented emissions from being put over the ocean
;also corrects error where fractional cement/flaring increase was not updated in 2011 code.

;2012b adds 3% to 2012 fluxes (all categories)

;2013a uses BP 2013 World Review, but no CDIAC updates.  3% is added to 2012 for 2013 global fluxes

;2014a uses BP 2014 World Review, and CDIAC updated through 2010 (countries and global total)

;2015a uses BP 2015 World Review, and CDIAC updated through 2010 (countries and global total)

;2016a uses BP 2015 World Review, and CDIAC updated through 2010 (countries and global total)
;this is fully backwards compatible (in inputs) with 2015a

;2016b uses BP 2016 World Review, and CDIAC updated through 2013 (countries and global total)

;2017a uses BP 2017 World Review, and CDIAC updated through 2013 (countries and global total)

;2017b uses BP 2017 World Review, and CDIAC updated through 2014 (countries and global total)

;2018a uses BP 2018 World Review, and CDIAC updated through 2014 (countries and global total)
;fix minor bug: extrapolated increase in flaring was treated identically to that of cement
;now flaring is extrapolated at same rate as (oil + gas)

;2019a uses BP 2019 World Review, and CDIAC updated through 2014 (countries and global total)

;2019b applies "bunker" emissions (the difference between global and sum of country totals) to grid cells with *any* ocean, not just 100% ocean

;2021a  numerous changes
;1. Deal with 2020 COVID emissions by using relative differences between 2020 and 2019 from CarbonMonitor emissions for countries where available
; on a monthly basis -- for countries not specified use 'rest of world'
;2. Update CDIAC national and global totals to the latest from the Appalachian State CDIAC project — previously went through 2014 only, now up to 2017
;3. Update EDGAR in-contry spatial patterns by using EDGAR v 5.0 maps; previously used EDGAR v4.2 which ended in 2008.
;   also some small changes in what's included in EDGAR ff (see edgar_ff.pro for details)
;4. Use latest BP estimates to extend from the end of new CDIAC (i.e. yr 2017) through 2019.

;2022a
;1. minimal changes from 2021a: just extend period of CarbonMonitor emissions to 9/30/2022

;2023a
;1. minimal changes from 2021a: just extend period of CarbonMonitor emissions to 4/30/2023

;ALSO.... [not sure which year this is associated with!]
;after many years CDIAC made both country and global numbers gas,oil,coal
;previously, country totals had coal,oil,gas

season='nam'
;season=''
;season='glb'
;seas2=''
seas2='euras'


;will still use edgar 1x1 patterns for within country patterning of country totals

;time ranges
yr1=1992
yr2=2017    ;final year of cdiac data
yr3=2019   ;final year of BP data
yr4=2022   ;final full year of CM data
yr5=2023   ;partial year of CM data, if any

bpyears0=yr3-yr2   ;percent increases for 2018 and 2019
bpyrarr=indgen(bpyears0)+yr2+1
cmyears0=yr4-yr3   ;percent increases for 2020 and 2021
cmyrarr=indgen(cmyears0)+yr3+1
cmyears1=yr5-yr3

nyears1=yr2-yr1+1         ;cdiac years
nyears2=yr4-yr2           ;after cdiac full years
nyears2a=yr5-yr2          ;after cdiac including partial year
nyears3=nyears2+nyears1   ;total full years
nyears3a=nyears2a+nyears1 ;total partial years

yrarr4=indgen(nyears3)+yr1

;additionally, CM has monthly data beyond yr4 (as of 12/6/22 through September 2022), then that will be applied as well

;time=findgen(nyears4)+0.5+yr1 -- this doesn't appear to be needed


;read in cdiac global totals
;later, we will subtract from sum of country totals and this difference will be applied to the oceans
;NOTE!!! Order of columns has changed:  now Year, Total, Solid, Liquid, Gas, Cement, Flaring, Per Capita
;Was: Year, Total, Gas, Liquid, Solid, Cement, Flaring, Per Capita
ccg_fread,file='/Users/john/EDGAR/CDIAC_historical/global/glb_fos_2017.txt',nc=8,skip=0,result
cdiacff=result[1:6,*]*1000.
cdiactime=result[0,*]

aa=where(cdiactime ge yr1 and cdiactime le yr2)
globtot=cdiacff[*,aa]
;globaltot array has following order: Year, Total, Solid, Liquid, Gas, Cement, Flaring, Per Capita
;countryarr has following order: Year, Total, Solid, Liquid, Gas, Cement, Flaring, Per Capita, Bunker (not included in total)
;now need to switch columns as previous (see ff_country_new2019b.pro e.g.) -- ..."NOW need" or "NO need"???

;to allow concatenation with addtot(s)
globtot=transpose(globtot)

;read in cdiac country data
file='/Users/john/EDGAR/CDIAC_historical/national/nation.1751_2017.mod2.csv'
ccg_read,file=file,skip=0,delimiter=',',res
country=res.field1
yr=res.field2
fftot=res.field3

aa=uniq(country)
ncountry1=n_elements(aa)
aa=[-1,aa]

;As more up to date and higher-res country maps are available (especially if time-dependent), in the future I will not do as much lumping or deleting
;
;to simplyify things to start with, use only data from 1992 on, where country splitting/combining
;(e.g. USSR, Czechoslovakia, Yugoslavia, Germany, Yemen) has mainly stopped (some exceptions below).

;In order to conform to GISS spatial country definitions, I have combined various small countries with larger neighbors and deleted some small countries/territories
;Eritrea+Ethiopia
;Israel+Palestinian Terr.
;East Timor+Indonesia
;deleted 
;and  others (see read_cdiac_nation_csv.pro)

countryarr=lonarr(nyears1,ncountry1,6)
countrystr=res[aa[1:*]].field1
countrystr2=countrystr

;-- WARNING -- order of fuel types changes over the years... always double check source file.
;fill up array of country ff data: total and 5 sectors:solid,liquid,gas,cement,flaring
for i=0,ncountry1-1 do begin
    temp=res[aa[i]+1:aa[i+1]]
    ;print,countrystr[i],aa[i]+1,aa[i+1]
    bb=where(temp.field2 ge yr1, countbb)
    if countbb ge 1 then begin
        temp=temp[bb]
        for j=0,5 do countryarr[*,i,j]=temp.(j+3)   ;total, solid, liquid, gas, cement, flaring
    endif else begin
        countryarr[*,i,*]=-999   ;for countries with no data yr1 or after
        countrystr2[i]='NODATA'
    endelse
endfor

;eliminate countries with NODATA
dd=where(countrystr2 ne 'NODATA',complement=cc,countdd)
countryarr=countryarr[*,dd,*]
countrystr2=countrystr2[dd]
ncountry2=countdd
;ccg_fwrite,file='/Users/john/EDGAR/CDIAC_historical/national/cdiac_countries_1992_2017.txt',nc=1,countrystr2


;extrapolate country data through yr3 using BP country-fueltype data
;note that BP country data does not include all countries, only major ones
;with residuals added into, e.g. 'S. America other' or 'Europe other'
;to deal with this fact, I have used the % increase for '[Continent] other'
;for all countries not listed in the BP data.
;
;BP files are prepared manually from .xlsx files downloaded from: https://www.bp.com/en/global/corporate/energy-economics/statistical-review-of-world-energy.html
;.csv files created from consumption tables for oil, coal, and gas (in the past this was Million Tons Oil Equiv., now BP uses Exajoules)
;then read_bp_20xx.pro reads the .csv files and creates the fractional increase files (_frac.csv)

;read header (should be the same for all fuels)
file='/Users/john/EDGAR/bp/bp_oil_1966_'+strtrim(yr3,2)+'_frac.csv'
ccg_sread,file=file,res
ccg_strtok,str=res[0],delimiter=',',header
bb=where(fix(header) ge yr2+1 and fix(header) le yr3,bpyears)
if bpyears ne bpyears0 then stop



fuel=['coal','oil','gas']     ;the order of this array should align with the ordering of the categories in CDIAC national and global files
nfuel=fuel.length
bparr=fltarr(bpyears,ncountry2,nfuel)

;ratios for coal, oil, and gas
for i=0,nfuel-1 do begin
    file='/Users/john/EDGAR/bp/bp_'+fuel[i]+'_1966_'+strtrim(yr3,2)+'_frac.csv'
    ccg_read,file=file,data,delimiter=',',skip=1
    for j=0,bpyears-1 do begin
      bparr[j,*,i]=data.(bb[j]+1)   ;e.g. 2009 increase over 2008, etc.    ;+1 is because field0 in ccg_read-produced structure is full string
    endfor
endfor

;2. ratios for flaring and cement
;
;revert to using GCP global numbers this year for ease/speed
;from https://data.icos-cp.eu/licence_accept?ids=%5B%226QlPjfn_7uuJtAeuGGFXuPwz%22%5D
;;direct source for cement is  http://minerals.usgs.gov/minerals/pubs/commodity/cement/
;From USGS PDFs.  In thousand metric tons (of cement, not CO2) of global production:
frac_inc_year=indgen(bpyears)+yr2+1     ;not really needed
cem_flare_yr=[2017,2018,2019]
cem=[403., 413., 427]
ceminc=cem/shift(cem,1)
flare=[110., 117., 117.]
flareinc=flare/shift(flare,1)
frac_inc_c=ceminc[1:*]
frac_inc_f=flareinc[1:*]


addarrbp=fltarr(bpyears+1,ncountry2,6)
addarrbp[0,*,*]=countryarr[-1,*,*]
addarrbp[1:*,*,1:3]=bparr
;1. extrapolate for coal, oil, and gas (indices 1:3)
for i=0,bpyears-1 do begin
;addarr[0,*,1:3]=bparr[0,*,*]*countryarr[-1,*,1:3]            ;for 2018
;addarr[1,*,1:3]=bparr[1,*,*]*addarr[0,*,1:3]                 ;for 2019
  addarrbp[i+1,*,1:3]=addarrbp[i,*,1:3]*bparr[i,*,*]
  addarrbp[i+1,*,4]=addarrbp[i,*,4]*frac_inc_c[i]       ;cement
  addarrbp[i+1,*,5]=addarrbp[i,*,5]*frac_inc_f[i]       ;flaring
endfor



;delete if possible
;;Or... (this does it by country)
;;temporarily concatenate addarr to countryarr
;countryarrtmp=[countryarr,addarr[0:2,*,*]]            ;only use first (three of four) entries to addarr; skip last year where it was extrapolated with factor of 1.0
;countryarrtmp_ong=reform(countryarrtmp[*,*,1]+countryarrtmp[*,*,2])
;frac_inc_f=countryarrtmp_ong[-(bpyears):-1,*]/countryarrtmp_ong[-(bpyears+1):-2,*]
;;calculate global version
;globalarrtmp_ong=total(countryarrtmp_ong,2)
;frac_inc_f_glb=globalarrtmp_ong[-(bpyears):-1]/globalarrtmp_ong[-(bpyears+1):-2]

;
;WARNING -- make sure cement and flaring are in the correct order!  There have been many ordering changes in CDIAC source files over the years.
;
;starting with version 2018a extrapolate cement and flaring separately
;this is also now done later (around line 495??) for the global totals.
;;first cement
;for j=0,bpyears-1 do begin
;  for i=0,ncountry2-1 do begin
;;    addarr[0,i,4]=countryarr[-1,i,4]*frac_inc_c[0]      ;2018
;;    addarr[1,i,4]=addarr[0,i,4]*frac_inc_c[1]           ;2019
;    addarrbp[j+1,i,4]=addarrbp[j,i,4]*frac_inc_c[j]
;    addarrbp[j+1,i,5]=addarrbp[j,i,5]*frac_inc_f[j]
;  endfor
;endfor

;;then flaring
;for j=0,bpyears-1 do begin
;  for i=0,ncountry2-1 do begin
;;    addarr[0,i,5]=countryarr[nyears1-1,i,5]*frac_inc_f[0]  ;2018
;;    addarr[1,i,5]=addarr[0,i,5]*frac_inc_f[1]              ;2019
;  endfor
;endfor

;trim first year which is actually last year from countryarr
addarrbp=addarrbp[1:*,*,*]

;now total is sum of all categories
addarrbp[*,*,0]=total(addarrbp,3)

;;concatenate BP years
;countryarr2=[countryarr,addarrbp] ;[nyears1+bpyears,ncountry2,6]

;sum over countries
a=total(addarrbp[*,*,0],2)
print,'Global totals from CDIAC national and BP extrapolation'
print,'BP years', bpyrarr
print,'BP global totals', a

;apply Carbon Monitor (CM) fractional increases for individual countries and Rest Of World (ROW)
;see read_carbon_monitor.pro for details
;ratios of a) yr(i+1)/yr(i) and b) mo(i+12)/mo(i) for a subset countries in countryarrnew2
;countries are alphabetized with names spelled exactly as those used in this code
;additionally there are ROW (Rest of World) and WORLD categories
cmfile='/Users/john/EDGAR/carbon_monitor/carbon_monitor_frac_inc2_201901_202304.sav'
restore,filename=cmfile     ;,ratioarr_moynew2,ratioarr_mox,ratioarr_yrynew2,ratioarr_yrx,cm_countryarrnew2

cmyears=ratioarr_yrx.length
addarrcm=fltarr(cmyears+1,ncountry2,6)
addarrcm[0,*,*]=addarrbp[-1,*,*]
rowidx=where(cm_countryarrnew2 eq 'ROW',countrowidx)
if countrowidx ne 1 then stop
;no fuel type distinction so annual increases will be applied identically to all fuel types, cement and flaring (and thus total)
;for i=0,cmyears-1 do begin
;  temp=ratioarr_yrynew2[i,*]
;  for j=0,ncountry2-1 do begin
;    aa=where(cm_countryarrnew2 eq countrystr2[i],countaa)
;    if countaa gt 1 then stop
;    if countaa eq 0 then idx=rowidx else idx=aa
;    addarrcm[i+1,j,*]=addarrcm[i,j,*]*temp[idx]
;  endfor
;endfor

;for i=0,cmyears-1 do begin
;  temp=ratioarr_yrynew2[i,*]
  for i=0,ncountry2-1 do begin
    aa=where(cm_countryarrnew2 eq countrystr2[i],countaa)
    if countaa gt 1 then stop
    if countaa eq 0 then begin
      idx=rowidx 
    endif else begin
      ;checked this on 6/14/21 and it matched country file in CM directory so it looks like all matches are happening
      ;print,'Country matched to CarbonMonitor',countrystr2[i]
      idx=aa
    endelse
    rtemp=ratioarr_yrynew2[*,idx]
    ;nyrtmp = rtemp.length
    for j = 0,cmyears-1 do begin
      ;addarrcm[1:cmyears,i,*]=addarrcm[0:cmyears-1,i,*]*rtemp
      addarrcm[1+j,i,*]=addarrcm[j,i,*]*rtemp[j]
    endfor
  endfor
;endfor

;trim first year which is actually last year from addarrbp
addarrcm=addarrcm[1:*,*,*]

;concatenate CM year(s)
countryarr2=[countryarr,addarrbp,addarrcm] ;[nyears1+bpyears+cmyears,ncountry2,6]

;sum over countries
a=total(addarrcm[*,*,0],2)
print,'Global totals from CDIAC national and CM and BP extrapolation'
print,'CM years', cmyrarr
print,'CM global totals', a

;write out extrapolated UK for use by other programs
ukidx=where(countrystr2 eq 'UNITED KINGDOM')
ukarr=reform(countryarr2[*,ukidx,*])/1.e6
outfile='/Users/john/idl/co2/cdiacff_uk'+strtrim(yr4,2)+'_extrap.txt'

ccg_fwrite,file=outfile,nc=7,yrarr4,$
  ukarr[*,0],$
  ukarr[*,1],$
  ukarr[*,2],$
  ukarr[*,3],$
  ukarr[*,4],$
  ukarr[*,5]

stop
;write out extrapolated USA for use by other programs
usaidx=where(countrystr2 eq 'UNITED STATES OF AMERICA')   
usaarr=reform(countryarr2[*,usaidx,*])/1.e6
outfile='/Users/john/idl/co2/cdiacff_usa'+strtrim(yr4,2)+'_extrap.txt'

ccg_fwrite,file=outfile,nc=7,yrarr4,$
  usaarr[*,0],$
  usaarr[*,1],$
  usaarr[*,2],$
  usaarr[*,3],$
  usaarr[*,4],$
  usaarr[*,5]

;write out extrapolated China for use by other programs
chnidx=where(strmid(countrystr2,0,5) eq 'CHINA')
chinaarr=reform(countryarr2[*,chnidx,*])/1.e6
outfile='/Users/john/idl/co2/cdiacff_china'+strtrim(yr4,2)+'_extrap.txt'

ccg_fwrite,file=outfile,nc=7,yrarr4,$
  chinaarr[*,0],$
  chinaarr[*,1],$
  chinaarr[*,2],$
  chinaarr[*,3],$
  chinaarr[*,4],$
  chinaarr[*,5]

;print out totals for all countries for nyears3
format1='('+strtrim(fix(ncountry2+1))+'A52)'
format2='('+strtrim(fix(ncountry2+1))+'F52.8)'
allarr=transpose(reform(countryarr2[*,*,0])/1.e6)
allarr=[reform(yrarr4,1,nyears3),[allarr]]
outfile2='/Users/john/idl/co2/cdiacff_all'+strtrim(yr4,2)+'_extrap.txt'
openw,lun,outfile2,/get_lun
printf,lun,['year',countrystr2],format=format1
printf,lun,allarr,format=format2
free_lun,lun


;
;geographically assign countries to 1x1 grid based on NASA/GISS country grid
;https://data.giss.nasa.gov/landuse/country.html
;Note: This map is as of 1993, but doesn't provide country definitions for the countries that were Yugoslavia
;
;there was not a 1 to 1 match in the GISS country data set and the
;CDIAC country list, even after the modifications made above.
;To resolve this I did the following.
;...I modified the GISS grid below by:
;...At least after 1992, only a single Yemen and Italy includes San Marino in CDIAC
;Combining the two Yemens into a single code (17950)
;Add San Marino (14000) to Italy (8100)

;Combining Anguilla and St. Kitts & Nevis (13750) -- note that as of 6/10/2021 the country name is "ST. KITTS-NEVIS" to align with CDIAC naming conventions
;Anguilla (500) is in GISS but there is no St. Kitts and Nevis -- it looks like I made up the code 13750 because it doesn't exist in the original list

;---These don't have be this way, because they (at least now) exist both in the GISS grid and in CDIAC
;Add Lesotho to S. Africa
;Add Liechtenstein to Germany  (probably better to have added to Switzerland, but doesn't matter too much)
;---

;see read_cdiac_nation_csv.pro for more informaiton

;Add all Yugoslav countries back to Yugoslavia
;
;And the following regions will not have fluxes mapped onto them:
;ANTARCTICA
;GUAM
;KERGUELEN
;OCEAN
;TUVALU
;TURKS&C.I
;Am samoa
;
;Dealing with sub-division codes.
;
;The GISS country map contains many subdivision codes for states and provinces of:
;US, Canada, Brazil, India, former USSR, Germany, Czechoslovakia, and Australia
;We need the codes for USSR and Czechoslovakia, because these are separate countries
;but don't want the codes for the other countries.  We have deleted the unwanted sub-division
;codes from COUNTRY1X1.CODE.mod.csv, but not from COUNTRY1X1.1993.mod.txt.  This means, in this code,
;that we have to match these countries based only on the first 3 digits.
;The primary codes for these countries are:
;
;Additionally, the former Yugoslav republic is now divided into numerous countries.
; To deal with this, I have combined all the Yugoslav countries as mentioned above
;(Macedonia, Yugoslavia, Croatia, Slovenia, and Bosnia)
;

;read in modified GISS map and country identifiers
;note that the identifiers have been mapped offline into the order
;of countries as defined by CDIAC
;note: mod2 refers to aggregation of all of former Yugoslavia
;
;for more info (not complete) see COUNTRY1X1.CODE.giss_cdiac_comp.xlsx

file1='/Users/john/EDGAR/COUNTRY1X1.CODE.mod2.2020.csv'
ccg_read,file=file1,delimiter=',',codes

file2='/Users/john/EDGAR/COUNTRY1X1.1993.mod.txt'
gissmap=lonarr(360,180)
temp=''
openr,unit,file2,/get_lun
for i=0,2 do readf,unit,temp
readf,unit,gissmap
free_lun,unit

;country totals will be weighted by emissions in the country,
;for which we'll use EDGAR patterns
;computed using edgar_fracv50.pro
edgardir = '/Users/john/EDGAR/edgarv5/v50_CO2_excl_short-cycle_org_C_TOTALS_nc/'
edgarfile = 'fracarr'+'_'+strtrim(yr1,2)+'_'+strtrim(yr4,2)+'.sav'
;restore,'/Users/john/EDGAR/edgarv5/v50_CO2_excl_short-cycle_org_C_TOTALS_nc/fracarr.sav'      ;global EDGAR pixel fractions (at 1x1) from 1992-2021
restore,edgardir+edgarfile                                                                     ;global EDGAR pixel fractions (at 1x1) from yr1 through yr4
fluxarr=fltarr(nyears3,360,180,6)
ncelltot=0L
eetot=[0]
for ii=0,nyears3-1 do begin
  tempfrac=reform(fracarr[ii,*,*])      ;tempfrac is only sampling first 29 years of 30 of fracarr
  for i=0,ncountry2-1 do begin
    ;for four cases below use subdivision codes, otherwise, not.
    if (codes[i].field1/100 eq 41 or $      ;Czech
        codes[i].field1/100 eq 172 or $     ;USSR
        codes[i].field1/100 eq 137 or $     ;St Kitts&Nevis
        codes[i].field1/100 eq 179) then $   ;Yemen
        temp=gissmap else temp=gissmap/100*100
    ee=where(temp eq codes[i].field1,ncells)
    idx=array_indices(temp,ee)
    fractot=total(tempfrac[ee])
;   print,i,countrystr2[i],fractot,ncells
    ;if ncells eq 1 then no geographic scaling required
    if ncells gt 1 then begin
        for j=0,ncells-1 do $
            fluxarr[*,idx[0,j],idx[1,j],*]=countryarr2[*,i,*]*tempfrac[idx[0,j],idx[1,j]]/fractot
    endif else if ncells eq 1 then $
        for j=0,ncells-1 do fluxarr[*,idx[0,j],idx[1,j],*]=countryarr2[*,i,*]
    ncelltot=ncelltot+ncells
    eetot=[eetot,ee]
    ;if i eq usaidx then stop
  endfor
endfor


;now add bunker fuels
;these will be mapped onto oceanic areas according to EDGAR fracarr.

;NOTE: Using CT basis regions (in older versions) to define land/ocean was problematic because Black, Baltic and Mediterranean Seas were all included in land
;This meant that when 'convolving' ocemask calculated in this way with fracarr, no emissions were placed in these seas.
;Instead, we will use the ocean grid cells from the gissmap array (=0). This also has the advantage of ensuring consistency -- i.e. that
;there will not be any coastal gridcells neither defined as land nor ocean.

ocemask=gissmap
aa=where(gissmap eq 0,complement=bb)
ocemask[aa]=1
ocemask[bb]=0

ocearr=matrix_repeat(ocemask,nyears3)*fracarr[0:nyears3-1,*,*]
ocetot=total(total(ocearr,2),2)


;attribute difference between global and country totals to 'bunker' fuels and assign to
;oceanic shipping routes
;before calculating the difference, we need to extrapolate globtot to yr3
;we will do this by sector

;bp global increases for 2009, and 2010 (over previous years)
  ;bpoil=[0.997581559,  0.980353039]
  ;bpcoal=[1.032128388, 0.997535297]
  ;bpgas=[1.024584292, 0.976373606]
  ;bpglobinc=[[bpcoal],[bpoil],[bpgas]]

bpglobinc=fltarr(bpyears,3)

for i=0,2 do begin    ;loop over fuel types
  ;read in global percentage increase files
  ccg_fread,file='/Users/john/EDGAR/bp/bp_'+fuel[i]+'_1966_'+strtrim(yr3,2)+'_frac_global.txt',nc=1,globperc
  bpglobinc[*,i]=globperc[-(bpyears):-1]
endfor

addtotbp=fltarr(bpyears+1,6)
addtotbp[0,*]=globtot[-1,*]
addtotbp[1:*,1:3]=bpglobinc
;extrapolate coal, oil, gas
for i=0,bpyears-1 do begin
  addtotbp[i+1,1:3]=addtotbp[i,1:3]*bpglobinc[i,*]
  addtotbp[i+1,4]=addtotbp[i,4]*frac_inc_c[i]
  addtotbp[i+1,5]=addtotbp[i,5]*frac_inc_f[i]
endfor

;;now cement and flaring
;;first cement
;addtot[0,5]=globtot[nyears1-1,5]*frac_inc_c[0]     ;2015
;addtot[1,5]=addtot[0,5]*frac_inc_c[1]              ;2016
;addtot[2,5]=addtot[1,5]*frac_inc_c[2]              ;2017
;addtot[3,5]=addtot[2,5]*frac_inc_c[3]              ;2018
;addtot[4,5]=addtot[3,5]*1.00                       ;2019
;
;;then flaring
;addtot[0,4]=globtot[nyears1-1,4]*frac_inc_f_glb[0]     ;2015
;addtot[1,4]=addtot[0,4]*frac_inc_f_glb[1]              ;2016
;addtot[2,4]=addtot[1,4]*frac_inc_f_glb[2]              ;2017
;addtot[3,4]=addtot[2,4]*frac_inc_f_glb[3]              ;2018
;addtot[4,4]=addtot[3,4]*1.00                           ;2019

;trim first year of addtotbp which is last year of globtot
addtotbp=addtotbp[1:*,*]

;now total
addtotbp[*,0]=total(addtotbp,2)

print,'CDIAC global total (from cdiac global and BP global)'
print,'BP years',bpyrarr
print,addtotbp[*,0]

;now for CM -- just total, no fuel types, cement, or flaring
;cmfile='/Users/john/EDGAR/carbon_monitor/carbon_monitor_frac_inc2_201901_202104.sav'
;restore,filename=cmfile     ;,ratioarr_moynew2,ratioarr_mox,ratioarr_yrynew2,ratioarr_yrx,cm_countryarrnew2
worldidx=where(cm_countryarrnew2 eq 'WORLD',countrowidx)
cmglobinc=ratioarr_yrynew2[*,worldidx]

addtotcm=fltarr(cmyears+1,6)
addtotcm[0,*]=addtotbp[-1,*]

;extrapolate for all categories and total with same factor
for i=0,cmyears-1 do begin
  addtotcm[i+1,*]=addtotcm[i,*]*cmglobinc[i]
endfor

;trim first year of addtotbp which is last year of globtot
addtotcm=addtotcm[1:*,*]

print,'CDIAC global total (from cdiac global and BP and CM global extrap)'
print,'CM years',cmyrarr
print,addtotcm[*,0]

;concatenate with globtot
globtot2=[globtot,addtotbp,addtotcm]

;write out glbtot2 for use by other programs
cdiacffnew=[transpose(cdiacff),addtotbp,addtotcm]/1.e6
cdiactimenew=[reform(cdiactime),indgen(nyears2)+yr2+1]
outfile='/Users/john/idl/co2/cdiacff_glb'+strtrim(yr4,2)+'_extrap.txt'

ccg_fwrite,file=outfile,nc=7,cdiactimenew,$
  cdiacffnew[*,0],$
  cdiacffnew[*,1],$
  cdiacffnew[*,2],$
  cdiacffnew[*,3],$
  cdiacffnew[*,4],$
  cdiacffnew[*,5]

globtot3=total(countryarr2,2)
bunker=globtot2-globtot3
bunkarr=fltarr(nyears3,360,180,6)
for i=0,nyears3-1 do for j=0,5 do bunkarr[i,*,*,j]=bunker[i,j]*ocearr[i,*,*]/ocetot[i]

fluxarr1=fluxarr+bunkarr  ;this ensures that 'bunker' fuels do not replace other fuels but are added.
                           ;otherwise, there could be potential problems for coastal areas.

;redefine just as total flux (no need now for component sectors)
;with bunker fuels
fluxarr1=reform(fluxarr1[*,*,*,0])  
;without bunker fuels
fluxarr1nb=reform(fluxarr[*,*,*,0])  

;--at this point we have nyears3 (1992 through yr4 (2020)) of patterned 1x1 fluxes that total to cdiac totals
;we still need to:
;
;2. interpolate to create monthly arrays
;3. add seasonality using Blasing et al harmonics
;4. apply monthly ratios from Carbon Monitor to fluxes starting 2020.0

;Long-term (a very old list)... other possibile adjustments:
;4. for seasonality
;a. remove 2nd harmonic for fluxes outside of US
;b. adjust amplitude of 1st harmonic according to latitude
;or seasonal temperatures
;5. level of emissions
;a. break out airplane emissions into separate category
;and place at another level.

;2. create monthly arrays
;interpolate to create monthly values
;time span of interpolation is from yr1 (1992.0) to yr4 (2020.9999), inclusive

; cubic interpolation with conservation of annual totals using the 'piqs' algorithm
; this method produces some negative flux values when annual values for certain countries approach zero rapidly
; see Somalia, Namibia and Netherlands Antilles for examples.

;calculate ndays over nyears3, including leap years
leapdays=ccg_leapyear(yrarr4)
daysinyear=365.+leapdays
ndays=total(daysinyear)

;initial jan1 index value
jan1=0

fluxarr15=fltarr(ndays,360,180)       ;smoothed time series that conserves monthly totals
;fluxarr15nb=fltarr(ndays,360,180)
time15=fltarr(ndays)
for i=0,nyears3-1 do begin
  dec31=jan1+daysinyear[i]-1
  time15[jan1:dec31]=findgen(daysinyear[i])/daysinyear[i]+yr1+i+1./(daysinyear[i]*2)
  ;increment day indices
  jan1=dec31+1
endfor

x=indgen(nyears3+1)+yr1

for i=0,359 do begin
    for j=0,179 do begin
        fit=piqs(x,fluxarr1[*,i,j])
        ;fitnb=piqs(x,fluxarr1nb[*,i,j])
        ;initialize jan1 index
        jan1=0
        for k=0,nyears3-1 do begin
            ;define time indices
            dec31=jan1+daysinyear[k]-1
            year1=x[k] & year2=x[k+1]
            aa=where(time15 ge year1 and time15 le year2)
            fluxarr15[jan1:dec31,i,j]=(time15[aa]-x[k])^2*fit[0,k]+(time15[aa]-x[k])*fit[1,k]+fit[2,k]  
            ;fluxarr15nb[jan1:dec31,i,j]=(time15[aa]-x[k])^2*fitnb[0,k]+(time15[aa]-x[k])*fitnb[1,k]+fitnb[2,k]
            ;overwrite negative values caused by piqs with constant annual values -- i.e. all months the same
            temp=fluxarr15[jan1:dec31,i,j]
            zz=where(temp lt 0,complement=yy,countzz)
            if zz[0] ne -1 then fluxarr15[jan1:dec31,i,j]=fluxarr1[k/12,i,j]/12.

            ;increment day indices
            jan1=dec31+1     
        endfor
    endfor
endfor

;now place daily values in monthly bins
;redefine fluxarr2 (and time2 and fluxarr3) with an extra year but for the time being only fill it up through nyears3
fluxarr2=fltarr((nyears3a*12),360,180)
;fluxarr2nb=fltarr((nyears3*12),360,180)
time2=findgen(nyears3a*12)/12.+yr1+1/(12.*2.)     ;not actually used
;x=indgen(nyears3+1)+yr1

idx0=0
for i=0,nyears3*12-1 do begin
  res=month2sec(leapyr=ccg_leapyear(yr1+i/12),daysinmonth=daysinmonth)
  ndays=daysinmonth(i mod 12)
  idx1=idx0+ndays-1
  fluxarr2[i,*,*]=total(fluxarr15[idx0:idx1,*,*],1,/double)/ndays
  ;increment idx0 for next loop iteration
  idx0=idx1+1
endfor

;destroy fluxarr15
fluxarr15=0

;;c no interpolation:  i.e. flat emissions for each month in a year, but with conservation of annual total and no negative values.
;time2=findgen(nyears3*12)/12.+yr1+1/24
;fluxarr2=fltarr((nyears3*12),360,180)
;fluxarr2nb=fltarr((nyears3*12),360,180)
;for k=0,nyears3*12-1 do begin
;    fluxarr2[k,*,*]=fluxarr1[k/12,*,*]
;    fluxarr2nb[k,*,*]=fluxarr1nb[k/12,*,*]
;endfor

;;identify negative values created by piqs algorithm (which is a spline that preserves the integral flux for each year
;;and at the same time prevents jumps in flux at the annual borders.)
;idxarr=[0,0]
;for i=0,nyears3*12-1 do begin
;  aa=where(fluxarr2[i,*,*] lt 0)
;  bb=where(fluxarr2[i,*,*] eq 0)
;  if aa[0] ne -1 then begin
;    idx=array_indices([360,180],aa,/dimensions)
;    idxarr=[[idxarr],[idx]]
;  endif
;endfor
;idxarr=idxarr[*,1:*]
;a=size(idxarr)
;npts=a[2]
;
;;plot locations on a map
;map_set,/continents
;for i=0,npts-1 do plots,idxarr[0,i]-180.,idxarr[1,i]-90.,psym=1,color=fsc_color('blue')


fluxarr3=temporary(fluxarr2)
;fluxarr3nb=temporary(fluxarr2nb)
time3=time2

;shift arrays by 1/2 month to correspond to middle of months
;fluxarr3=(fluxarr2[1:*,*,*]+fluxarr2[0:(nyears3-1)*12-1,*,*])/2
;time3=time2[1:*]-1./24

;3 add seasonality
;put in seasonality for USA based on average seasonality of emissions
;from Blasing et al 2004 CDIAC
if keyword_set(season) then begin
    ;read in blasing data
    ccg_fread,file='/Users/john/idl/co2c13/plot2d/emis_mon_usatotal_2col.txt',nc=2,monthff
    ;fit smooth curve to data and extract the average seasonal cycle (centered on zero)
    ccg_ccgvu,x=monthff[0,*],y=monthff[1,*],fsc=fsc,sc=sc,coef=coef,ftn=ftn
    ;normalize fsc as percentage of total
    seasff=fsc[1,0:11]/mean(monthff[1,12:23])
    ;apply seasonal cycle to all months in N. Am. [30 - 60N; 60-140W]
    seasff2=matrix_repeat(seasff,nyears3,/vector,/swap) ;repeats average sc over nyears3

    if season eq 'nam' then begin
        lonidxmin=40 & lonidxmax=120 & latidxmin=120 & latidxmax=150
    endif else if season eq 'glb' then begin
        lonidxmin=0 & lonidxmax=359 & latidxmin=0 &latidxmax=179
    endif ;else if season eq 'glb2' then begin
        
    nx=lonidxmax-lonidxmin+1
    ny=latidxmax-latidxmin+1
    
    ;seasff3=fltarr((nyears3)*12,nx,ny)
    ;for i=0,nx-1 do for j=0,ny-1 do seasff3[*,i,j]=seasff2
    seasff3=rebin(seasff2,nyears3*12,nx,ny)
    region=fluxarr3[0:nyears3*12-1,lonidxmin:lonidxmax,latidxmin:latidxmax]
    ;regionnb=fluxarr3nb[0:nyears3*12-1,lonidxmin:lonidxmax,latidxmin:latidxmax]
    region_seas=temporary(region)*(seasff3+1)
    ;region_seasnb=temporary(regionnb)*(temporary(seasff3)+1)
    fluxarr3[0:nyears3*12-1,lonidxmin:lonidxmax,latidxmin:latidxmax]=temporary(region_seas)
    ;fluxarr3nb[0:nyears3*12-1,lonidxmin:lonidxmax,latidxmin:latidxmax]=temporary(region_seasnb)
    
    if seas2 eq 'euras' and season ne 'glb' then begin
        ;read in seasonal adjustment factors -- provisional from EDGAR -- based on W. Europe
        ccg_fread,file='/Users/john/EDGAR/eurasian_seasff.txt',skip=3,nc=1,seasffa
        seasff2a=matrix_repeat(seasffa,nyears3,/vector,/swap) ;repeats average sc over nyears2-1
        lonidxmin=160 & lonidxmax=350 & latidxmin=120 & latidxmax=150
        nx=lonidxmax-lonidxmin+1
        ny=latidxmax-latidxmin+1
        ;seasff3a=fltarr((nyears3)*12,nx,ny)
        ;for i=0,nx-1 do for j=0,ny-1 do seasff3a[*,i,j]=seasff2a
        seasff3a=rebin(seasff2a,nyears3*12,nx,ny)
        region=fluxarr3[0:nyears3*12-1,lonidxmin:lonidxmax,latidxmin:latidxmax]
        ;regionnb=fluxarr3nb[*,lonidxmin:lonidxmax,latidxmin:latidxmax]
        region_seas=temporary(region)*(seasff3a)
        ;region_seasnb=temporary(regionnb)*(temporary(seasff3a))
        fluxarr3[0:nyears3*12-1,lonidxmin:lonidxmax,latidxmin:latidxmax]=temporary(region_seas)
        ;fluxarr3nb[*,lonidxmin:lonidxmax,latidxmin:latidxmax]=temporary(region_seasnb)
    endif
endif

;JBM 6/21/23 -- this seems redundant but I'm keeping it for now so as to not potentially break anything in the code
;now apply monthly country factors from CM for Jan. 2020 through April 2023
;apply Carbon Monitor (CM) fractional increases for individual countries and Rest Of World (ROW)
;see read_carbon_monitor.pro for details
;ratios of a) yr(i+1)/yr(i) and b) mo(i+12)/mo(i) for a subset countries in countryarrnew2
;countries are alphabetized with names spelled exactly as those used in this code
;additionally there are ROW (Rest of World) and WORLD categories
cmfile='/Users/john/EDGAR/carbon_monitor/carbon_monitor_frac_inc2_201901_202304.sav'
restore,filename=cmfile     ;,ratioarr_moynew2,ratioarr_mox,ratioarr_yrynew2,ratioarr_yrx,cm_countryarrnew2
;rowidx=where(cm_countryarrnew2 eq 'ROW',countrowidx)

cmmos=ratioarr_mox.length
cmyrs=ratioarr_yrx.length

;create new ratioarr that is cmyears1*12 x ncountries
ratioarr_moynew3=fltarr(cmyears1*12,cm_countryarrnew2.length)+!values.f_nan
ratioarr_moynew3[0:cmmos-1,*]=ratioarr_moynew2
ratioarr_yrynew3=fltarr(cmyears1-1,cm_countryarrnew2.length)+!values.f_nan
ratioarr_yrynew3[0:cmyrs-1,*]=ratioarr_yrynew2

;we want to overwrite last 12 months of fluxarr3 (2020) with new monthly factors
; and add 4 new months of 2021
;first 12 factors of ratioarr_moynew2 are 2020/2019 and next 4 are for 2021/2020

; and add 9 new months of 2022
;first 24 factors of ratioarr_moynew2 are 2020/2019 and 2021/2020 and next 9 are for 2022/2021

;fluxarr3cm=fltarr((cmyears1)*12,360,180)                            ;2020 and 2021
fluxarr3tmp=fluxarr3[(yr3-yr1)*12:(yr3-yr1+cmyears1)*12-1,*,*]      ;2019 and 2020 and 2021

;countryfluxcm_mo=fltarr(ncountry2,cmyears1*12)
;countryfluxcm_yr=fltarr(ncountry2,cmyears1-1)

;scale months in fluxarr3 according CM ratios for countries
for i=0,ncountry2-1 do begin
  aa=where(cm_countryarrnew2 eq countrystr2[i],countaa)
  if countaa gt 1 then stop
  if countaa eq 0 then idx=rowidx else idx=aa
  rtemp=ratioarr_moynew3[*,idx]
  ;rtempyr=ratioarr_yrynew3[*,idx]
  ;first do country totals
  ;countryfluxcm_yr[i,*]=countryarr2[-1,i,0]*rtempyr         ;not robustly coded...
  
  ;apply rtemp to all grid cells for a given country
  ;for four cases below use subdivision codes, otherwise, not.
  if (codes[i].field1/100 eq 41 or $      ;Czech
    codes[i].field1/100 eq 172 or $     ;USSR
    codes[i].field1/100 eq 137 or $     ;St Kitts&Nevis
    codes[i].field1/100 eq 179) then $   ;Yemen
    temp=gissmap else temp=gissmap/100*100
  ee=where(temp eq codes[i].field1,ncells)
  idx=array_indices(temp,ee)
  ;   print,i,countrystr2[i],ncells
  ;rtemp and fluxarr3cm have time dimensions of cmyears1*12 months
  for j=0,ncells-1 do $
    fluxarr3[(yr3-yr1+1)*12:(yr3-yr1+1+cmyears1)*12-1,idx[0,j],idx[1,j]]=fluxarr3tmp[*,idx[0,j],idx[1,j]]*rtemp      ;rtemp is ratios for 2020/2019 and 2021/2020 and fluxarr3tmp is for 2019 and 2020
  ncelltot=ncelltot+ncells
  eetot=[eetot,ee]
endfor

;oceans/bunker
rtemp=ratioarr_moynew3[*,worldidx]
;scale oceans/bunker by WORLD ratios for last 24 months
;ocemask2=matrix_repeat(ocemask,cmyears1)
fluxarr3tmp=fluxarr3[(yr3-yr1)*12:(yr3-yr1+cmyears1)*12-1,*,*]      ;2019 and 2020 and 2021
ee=where(ocemask eq 1,ncells)
idx=array_indices(ocemask,ee)
for j=0,ncells-1 do $
  fluxarr3[(yr3-yr1+1)*12:(yr3-yr1+1+cmyears1)*12-1,idx[0,j],idx[1,j]]=fluxarr3tmp[*,idx[0,j],idx[1,j]]*rtemp

;store fluxarr3 and fluxarr3nb as .sav files
save,fluxarr3,filename='/Users/john/idl/co2/fluxarr3.sav'
;save,fluxarr3nb,filename='/Users/john/idl/co2/fluxarr3nb.sav'


;re-calculate annual country totals from grid and compare to originals
fluxarr4=fltarr(nyears3,ncountry2)       
fluxarr5=fltarr(nyears3,ncountry2)
;fluxarr4nb=fltarr(nyears3,ncountry2)       
eetot=[0]
for i=0,ncountry2-1 do begin
    ;for four cases below use subdivision codes, otherwise, not.
    if (codes[i].field1/100 eq 41 or $      ;Czech
        codes[i].field1/100 eq 172 or $     ;USSR
        codes[i].field1/100 eq 137 or $     ;St Kitts&Nevis
        codes[i].field1/100 eq 179)then $   ;Yemen
        temp=gissmap else temp=gissmap/100*100
    ee=where(temp eq codes[i].field1,ncells)
    ;idx=array_indices(temp,ee)
    print,i,countrystr2[i],ncells
    for j=0,nyears3-1 do begin
        temp2=total(fluxarr3[j*12:j*12+11,*,*],1)/12.
        ;temp2nb=total(fluxarr3nb[j*12:j*12+11,*,*],1)/12.
        ;temp3=total(fluxarr[j,*,*],1)
        temp3=reform(fluxarr1nb[j,*,*])
        fluxarr4[j,i]=total(temp2[ee])
        fluxarr5[j,i]=total(temp3[ee])
        ;fluxarr4nb[j,i]=total(temp2nb[ee])
    endfor
    eetot=[eetot,ee]
endfor

;convert from million tons to PgC
fluxarr3=fluxarr3/1.e6
;stop
;write out fluxarr3 into hdf files for tm5
;start at 1992+1/24 and end 2023+4/12-1/24     ;last 8 months of 2023 are NaNs
idate=[1992,1,1,0,0,0]
;sytem time
r=ccg_sysdate()
stop
for i=0,nyears3a*12-1 do begin
    file='/Users/john/EDGAR/country/ct_20230621/co2ff.country_cdiac_bp.seas'+season+seas2+'.1x1.hdf'+chardate(idate,cut=4)
    tag='ff_country_new2023a.pro'+r.s3+'_'+strtrim((i mod 12)+1,2)
    write_hdf,file,tag,reform(fluxarr3[i,*,*]),append=((i mod 12) ne 0),idate=idate,unit='GtC/yr'
    increase_time,idate,1,/month
endfor


end
