;pro read_bp_2024

;version _2020 ...
;version _2019 reflects small change to BP files
;version _2018 reflects significant changes to BP files

;read in bp csv files (created from sheets within '/Users/john/EDGAR/bp/statistical_review_of_world_energy_full_report_201?.xls')
;
;2024: Report now comes from the Energy Institute and file is EI-Stats-Review-All-Data.xlsx
;
;Notes on spreadsheet processing
;1. use consumption data for oil, gas, and coal in energy units (this is for relative scaling so doesn't matter too much)
;2. to get rid of diamonds, carrots, etc.: a) select all b) Format -> Cells, choose "number" c) select conditional formatting, then remove all rules from sheet
;...d) search and replace any remaining "-" (dashes) with zero
;3. save as .csv file named like "bp_[coal/oil/gas][year+1].csv"
;
;
;Code does this:
;1. clean up missing values
;2. match with CDIAC countries

fuel=['coal','oil','gas']
nfuel=n_elements(fuel)

;yr='2011'
;yr='2012'
;yr='2013'
;yr='2014'
;yr='2015'
;yr='2016'
;yr='2017'
;yr='2018'
;yr='2019'
;yr='2020'
;yr='2023'
yr='2024'

;note--> for 2016 BP, we need to delete a line in Europe for USSR.  This was formerly included in 'Other Europe and Eurasia'
; For now, delete USSR from the oil, gas and coal .csv files that are used as input for this code
; This makes global totals incorrect, but percentage increases will be ok.  The way I separate Belguim and Luxembourg (by repeating emissions) is also 'wrong' 
; Note that USSR row only has values (at least for 2017 BP report) for 1984 and earlier.  After that it appears that emissions are split among republics.
; So, as of 9/27/2017 I will still delete the USSR row.

;Update: 12/3/2018.  2018 BP spreadsheets have changed some:
;1.  separate block (as for a continent) for USSR/CIS/former Russian Republics:
;Azerbaijan
;Belarus
;Kazakhstan
;Russian Federation
;Turkmenistan
;Ukraine
;USSR         ; stops in 1984; all others start in 1985
;Uzbekistan
;Other CIS
;Note that Baltic states: Lituania, Latvia, Estonia are in Europe block and also start in 1985
;
;2. Within each continent, there is no longer just "Other Africa", but 'Eastern Africa', 'Middle Africa', etc.

;Update: 8/12/2019.  2019 BP spreadsheets have changed in the following way:
;1. Ukraine has been taken out of CIS "continent" block and placed in Europe. -- no change required to code
;2. Macedonia has been renamed 'North Macedonia' -- change required

;2020 countries are identical to 2019! for once!

dir='/Users/john/EDGAR/bp/'

for i=0,nfuel-1 do begin
  file=dir+'bp_'+fuel[i]+yr+'.csv'
  z=read_csv(file,n_table_header=2,header=header,num_records=108)    ;108 goes from the line just below the header to 'Total World'
  
  ;identify and trim bp country array
  countries_bp=z.(0)
  ;trim: a) blanks b)unit label c) total labels
  ;also:
  ;eliminate following four entries (for now) from BP list
  ;USSR (not present in CDIAC country list after 1992, which is when we presently start)
  ;Macedonia, Croatia, Slovenia (part of aggregated Yugoslavia, which is part of "Other Europe")
  aa=where(countries_bp eq '' or countries_bp eq 'Exajoules' or strmid(countries_bp,0,5) eq 'Total' $
    or countries_bp eq 'USSR' or countries_bp eq 'Croatia' or countries_bp eq 'North Macedonia' or countries_bp eq 'Slovenia',$
    countaa, complement=bb, ncomplement=countbb)
  countries_bp=countries_bp[bb]
  print,fuel[i],countbb

  ;
  ;match each year array to countries_bp -- this requires that year arrays be same length as countries, which should always be true assuming num_records
  ;is chosen appropriately
  ;
  
  ;
  ;make non numeric values ('-','^','n/a','w' (shows up as a diamond in excel),etc.) 0, but keep blanks as blank
  ;UPDATE as of 12/3/2018:  I realized that '-','^', and "diamond" in the spreadsheet all have numeric values behind them in excel.  Only n/a is a null value.
  ;so now, .csv files have the numeric values.
  ;
  
  ;get size of structure z
  fields=tag_names(z)
  nfields=n_elements(fields)
  tmparr=fltarr(nfields-4,countbb)
  for j=1,nfields-4 do begin   ;skip first field which is country array and last THREE which are percentage increase fields and can be skipped
    ;make array correspond to new countries_bp
    tmparr[j-1,*]=float(z.(j)[bb])     ;this will print 'Type conversion error' messages, but it will convert "n/a" to 0.0, as desired
  endfor
  tmparrglb=total(tmparr,2)
  
  ;create % increase array, because this is the basis upon which we can add other countries in '2' and is the final product we need
  tmparr_shift=shift(tmparr,1)
  percarr=tmparr/tmparr_shift
  percarr=percarr[1:*,*]
  
  tmparrglb_shift=shift(tmparrglb,1)
  percarrglb=tmparrglb/tmparrglb_shift
  percarrglb=percarrglb[1:*]  
  
  ;2. match array to CDIAC country list.  This requires expansion (e.g. 'Other S. America' will be applied to S. American countries not listed in BP)
  ;  and requires some other country manipulation

  ;as of 2020 BP stats
  ;i) unlisted countries in South and Central America have been further sub-divided into
  ;South America, Caribean, Central America; from definitions tab of BP excel spreadsheet:
  ;a) Caribean: Atlantic islands between the US Gulf Coast and and South America, including Puerto Rico, US Virgin Islands and Bermuda.
  ;b) Central Am.: Belize, Costa Rica, El Salvador, Guatemala,  Honduras, Nicaragua, Panama
  ;
  ;ii) in Africa now Eastern Africa, Middle Africa, Western Africa, Other Northern Africa, Other Southern Africa
  ;These divisions are much less clearly defined, so I will do my best.
  ;a) Northern Africa:  Territories on the north coast of Africa from Egypt to Western Sahara.
  ;b) Eastern Africa:  Territories on the east coast of Africa from Sudan to Mozambique. Also Madagascar, Malawi, Uganda, Zambia, Zimbabwe.
  ;c) Middle Africa:  Angola, Cameroon, Central African Republic, Chad, Democratic Republic of Congo, Republic of Congo, Equatorial Guinea, Gabon, Sao Tome & Principe.
  ;d) Western Africa:  Territories on the west coast of Africa from Mauritania to Nigeria, including Burkina Faso, Cape Verde, Mali and Niger.
  ;e) Southern Africa:  Botswana, Lesotho, Namibia, South Africa, Swaziland.
  ;f) following CDIAC countries were all added to eastern_africa: BURUNDI**
  ;COMOROS
  ;ETHIOPIA
  ;MAURITIUS
  ;REUNION
  ;RWANDA
  ;SEYCHELLES
  
  ;multi-country lines in spread sheet
;  othernames=['Central America',$
;    'Other Caribbean',$
;    'Other South America',$
;    'Other Europe',$
;    'Other CIS',$
;    'Other Middle East',$
;    'Eastern Africa',$
;    'Middle Africa',$
;    'Western Africa',$
;    'Other Northern Africa',$
;    'Other Southern Africa',$
;    'Other Asia Pacific']
  
;  nothers=n_elements(othernames)
  
  
  aa=where(strmid(countries_bp,0,5) eq 'Other' $
    or countries_bp eq 'Central America' $
    or countries_bp eq 'Eastern Africa' $
    or countries_bp eq 'Middle Africa' $
    or countries_bp eq 'Western Africa', nothers)
  othernames=countries_bp[aa]
  aa=[-1,aa]
  dummy=['']
  
  ;take 'othernames' and replace spaces with '_' and make lowercase.  This will now match with file names (minus .2017.txt)
  othernames2=othernames
  for j=0,nothers-1 do begin
    tmpstr=othernames2[j]
    ;z=strpos(othernames2[j],' ')
    z=strpos(tmpstr,' ')
    while z[0] gt -1 do begin
      ;strput,othernames2[j],'_',z[0]
      strput,tmpstr,'_',z[0]
      ;z=strpos(othernames2[j],' ')
      z=strpos(tmpstr,' ')
    endwhile
    othernames2[j]=tmpstr
  endfor
  othernames2=strlowcase(othernames2)
  
  ;ncountries_cdiac=191
  ;for 2013 verion, Netherland Antilles was eliminated (no data after 2010) and Western Sahara was not present
  ;so go from 191 to 189 countries
  ncountries_cdiac=189
  percnew=fltarr(nfields-5,ncountries_cdiac)      ;nfields-5 instead of nfields-4 because year-on-year increases will always have one less field than raw data
  
  offset=0
  for j=0,nothers-1 do begin
    ;read in file
    ;between 2018 and 2019 releases, countries not explicitly listed have not changed, so we can still use ".2017." files
    ;however, update ".2020.txt" files to reflect better knowledge of African splits
    file=dir+othernames2[j]+'.2020.txt'
    ccg_sread,file=file,tmpother,/nomessages
    ntmp=n_elements(tmpother)
    
    ;insert country names into 'dummy' and associated percentage increases into percarr
    ;start with adding parts of "countries_bp" to "dummy", but check to see if there are consecutive "othernames" in variable aa
    ; i.e. for S. and Central America, and Africa, there are no "countries_bp" to insert between the "others" 
    id0=aa[j]+1 & id1=aa[j+1]-1
    id2=id0+offset & id3=id1+offset
    if id0 le id1 then begin  
      dummy=[dummy,countries_bp[id0:id1]]
      percnew[*,id2:id3]=percarr[*,id0:id1]
    endif

    ;find location of othernames[j] in othernames and replace with list
      ;below (stregex) is redundant and just checks to make sure order is correct and no surprises
      bb=stregex(othernames,othernames[j],/fold_case)
      if bb[j] eq -1 then stop
    dummy=[dummy,tmpother]
    
    ;and new parts of percnew
    for k=0,nfields-6 do begin
      percnew[k,id3+1:id3+ntmp]=percarr[k,aa[j+1]]
    endfor
    offset=offset-1+ntmp ;subtract one
  endfor
  countries_new=dummy[1:*]
  n_countries=n_elements(countries_new)

  ;sort countries and percarr
  ;... but first, rename a few countries to conform with alphabetization in CDIAC list
  aa=where(countries_new eq 'China Hong Kong SAR',countaa)
  if countaa ne 0 then countries_new[aa]='Hong Kong' else stop
  aa=where(countries_new eq 'Iran',countaa)
  if countaa ne 0 then countries_new[aa]='Islamic Republic of Iran'  else stop
  
  ;---> starting in 2016 BP review, 'Republic of Ireland was changed to Ireland' so next two lines not necessary
  ;aa=where(countries_new eq 'Republic of Ireland',countaa)
  ;if countaa ne 0 then countries_new[aa]='Ireland' else stop
  aa=where(countries_new eq 'South Korea',countaa)
  if countaa ne 0 then countries_new[aa]='Republic of South Korea' else stop
  aa=where(countries_new eq 'US',countaa)
  if countaa ne 0 then countries_new[aa]='United States of America' else stop
  ;2018: just to be safe, change a few more names to make sure spelling is identical (don't worry about capitalization though)
  aa=where(countries_new eq 'Trinidad & Tobago',countaa)
  if countaa ne 0 then countries_new[aa]='Trinidad and Tobago' else stop
  aa=where(countries_new eq 'Vietnam',countaa)
  if countaa ne 0 then countries_new[aa]='Viet Nam' else stop
  aa=where(countries_new eq 'Italy',countaa)
  if countaa ne 0 then countries_new[aa]='ITALY (INCLUDING SAN MARINO)' else stop
  aa=where(countries_new eq 'France',countaa)
  if countaa ne 0 then countries_new[aa]='FRANCE (INCLUDING MONACO)' else stop
  aa=where(countries_new eq 'China',countaa)
  if countaa ne 0 then countries_new[aa]='CHINA (MAINLAND)' else stop
  
  aa=sort(strlowcase(countries_new))
  pp=percnew[*,aa]
  cc=countries_new[aa]
  nlines=n_elements(aa)
  
  ;replace NaNs and Infs with 0.0
  aa=where(finite(pp) ne 1, countaa)
  if countaa ne 0 then pp[aa]=1.0     ;i.e. assume same value as previous year when no data or actually close to zero emissions (consumption)
  
  ;write out file
  outfile=dir+'bp_'+fuel[i]+'_1966_'+strtrim(yr-1,2)+'_frac.csv'
  openw,unit,outfile,/get_lun
  ;write header
  printf,unit,'country'+','+strjoin(strtrim(indgen(yr-1966)+1966,2),',')
  for j=0,nlines-1 do $
    printf,unit,cc[j]+','+strjoin(strtrim(pp[*,j],2),',')
  free_lun,unit
  
  ;write out global percentage increases
  outfile=dir+'bp_'+fuel[i]+'_1966_'+strtrim(yr-1,2)+'_frac_global.txt'
  ccg_fwrite,file=outfile,nc=1,percarrglb
endfor


stop
end
