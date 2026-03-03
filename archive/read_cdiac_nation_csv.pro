;pro read_cdiac_nation_csv

;read in country file from cdiac; combine and eliminate soume countries and package in a form
;readable by ff_country_new20XXz.pro

;downloaded from https://energy.appstate.edu/sites/default/files/nation.1751_2017.xlsx
dir='/Users/john/EDGAR/CDIAC_historical/national/'

;file='nation.1751_2017.csv'
;res=read_csv(dir+file,count=nlines,n_table_header=4)
;res2=read_ascii(dir+file,header=header,delimiter=',',data_start=4)

;file='nation.1751_2019.csv'     
;res=read_csv(dir+file,count=nlines)
;res2=read_ascii(dir+file,header=header,delimiter=',',data_start=1,missing_value = 0)

file='nation.1751_2020.csv'
res=read_csv(dir+file,count=nlines)
res2=read_ascii(dir+file,header=header,delimiter=',',data_start=1,missing_value = 0)

arr1=res2.field01[1:9,*]

;years
yr1=1992
yr2=fix(strmid(file_basename(file,'.csv'),3,4,/reverse_offset))

;names of countries
namearr=res.field01     ;countries
;find uniq entries
uidx=uniq(namearr,sort(namearr))    ;should already be sorted alphabetically but just in case
ncountries=uidx.length
countries=namearr[uidx]

;nlines2=namearr.length
;arr1=fltarr(9,nlines2)

;Yugoslavia is split into Serbia and Montenegro starting in 2006
; we will recombine.
;In future, we may want to have all Yugoslav countries separate.
aa=where(namearr eq 'SERBIA',countaa)
bb=where(namearr eq 'MONTENEGRO',countbb)
azz=cmset_op(arr1[0,aa],'AND',arr1[0,bb],/index,count=nyrsa)
bzz=cmset_op(arr1[0,bb],'AND',arr1[0,aa],/index,count=nyrsb)
;now add Montenegro emissions to Serbia's
arr1[1:6,aa[azz]]=arr1[1:6,aa[azz]]+arr1[1:6,bb[bzz]]
;index 7, per capita will just be the one of the large country
;combine bunker fuels -- not being used now but should add
arr1[8,aa[azz]]=arr1[8,aa[azz]]+arr1[8,bb[bzz]]
;rename Serbia as Yugoslavia; Montenegro will be deleted below to avoid double counting
namearr[aa]='YUGOSLAVIA'

;Likewise Sudan was split into Republic of South Sudan and Republic of Sudan for 2012 and 2013
;We will maintain them both as "Sudan" for the entire record. 
aa=where(namearr eq 'REPUBLIC OF SUDAN',countaa)
bb=where(namearr eq 'REPUBLIC OF SOUTH SUDAN',countbb)
azz=cmset_op(arr1[0,aa],'AND',arr1[0,bb],/index,count=nyrsa)
bzz=cmset_op(arr1[0,bb],'AND',arr1[0,aa],/index,count=nyrsb)
;now add South Sudan's emissions to Sudan's
arr1[1:6,aa[azz]]=arr1[1:6,aa[azz]]+arr1[1:6,bb[bzz]]
;index 7, per capita will just be the one of the large country
;combine bunker fuels -- not being used now but should add
arr1[8,aa[azz]]=arr1[8,aa[azz]]+arr1[8,bb[bzz]]
;delete "Republic" from Sudan; South Sudan will be deleted below to avoid double counting
namearr[aa]='SUDAN'


;Some name changes for simplicity
aa=where(namearr eq 'PLURINATIONAL STATE OF BOLIVIA')
namearr[aa]='BOLIVIA'

aa=where(namearr eq 'HONG KONG SPECIAL ADMINSTRATIVE REGION OF CHINA')
namearr[aa]='HONG KONG'

aa=where(namearr eq 'CHINA (MAINLAND)')
namearr[aa]='CHINA'

aa=where(namearr eq 'MYANMAR (FORMERLY BURMA)')
namearr[aa]='MYANMAR'

aa=where(namearr eq 'BRUNEI (DARUSSALAM)')
namearr[aa]='BRUNEI'

aa=where(namearr eq 'DEMOCRATIC REPUBLIC OF THE CONGO (FORMERLY ZAIRE)')
namearr[aa]='DEMOCRATIC REPUBLIC OF THE CONGO'

aa=where(namearr eq 'FALKLAND ISLANDS (MALVINAS)')
namearr[aa]='FALKLAND ISLANDS'

aa=where(namearr eq 'FRANCE (INCLUDING MONACO)')
namearr[aa]='FRANCE'

aa=where(namearr eq 'LAO PEOPLE S DEMOCRATIC REPUBLIC')
namearr[aa]='LAOS'

aa=where(namearr eq 'LIBYAN ARAB JAMAHIRIYAH')
namearr[aa]='LIBYA'

aa=where(namearr eq 'RUSSIAN FEDERATION')
namearr[aa]='RUSSIA'

aa=where(namearr eq 'SYRIAN ARAB REPUBLIC')
namearr[aa]='SYRIA'

aa=where(namearr eq 'VIET NAM')
namearr[aa]='VIETNAM'

;following would be nice to change later, but will screw up list alphabetization...
;...possibly others too
;REPUBLIC OF CAMEROON
;REPUBLIC OF MOLDOVA
;UNITED REPUBLIC OF TANZANIA

;these need to be changed to unify the time series under a single name -- other former Yugoslav countries added to this below as well
aa=where(namearr eq 'YUGOSLAVIA (MONTENEGRO & SERBIA)')
namearr[aa]='YUGOSLAVIA'

aa=where(namearr eq 'YUGOSLAVIA (FORMER SOCIALIST FEDERAL REPUBLIC)')
namearr[aa]='YUGOSLAVIA'

;Add "little" countries to adjacent or form (Yugoslavia) "big" countries, because little countries are not on the GISS map grid
;
;-->The two exceptions are Lesotho and Anguilla, which can be corrected in future; this will also require re-adjusting the GISS Grid and Giss country name file.
;Note that Anguilla's and St. Kitts & Nevis's GISS codes were merged prior to 2021, but Anguilla was being deleted; this is fixed as of 6/10/21
;
;new countries/territories for _2017.xlsx data
;Kosovo -> Yugoslavia
;Isle of Man --> UK
;
;Not sure what was happening previously with Liechtenstein.  In future, it can be independent.  Probably should be added to Switzerland, but being added to
;Germany to conform with GISS grid modifications
;

bigcountry=['ETHIOPIA','ISRAEL','INDONESIA','CANADA','SPAIN','VENEZUELA','SPAIN','CHINA',$
  replicate('YUGOSLAVIA',5),'SOUTH AFRICA','UNITED KINGDOM','ST. KITTS-NEVIS','GERMANY']
littlecountry=['ERITREA','OCCUPIED PALESTINIAN TERRITORY','TIMOR-LESTE (FORMERLY EAST TIMOR)','ST. PIERRE & MIQUELON',$
  'GIBRALTAR','ARUBA','ANDORRA','MACAU SPECIAL ADMINSTRATIVE REGION OF CHINA',$
  'MACEDONIA','CROATIA','BOSNIA & HERZEGOVINA','SLOVENIA','KOSOVO','LESOTHO','ISLE OF MAN','ANGUILLA','LIECHTENSTEIN']

npairs=n_elements(bigcountry)

for i=0,npairs-1 do begin
  aa=where(namearr eq bigcountry[i],countaa)
  bb=where(namearr eq littlecountry[i],countbb)
  azz=cmset_op(arr1[0,aa],'AND',arr1[0,bb],/index,count=nyrs)
  bzz=cmset_op(arr1[0,bb],'AND',arr1[0,aa],/index,count=nyrs)
;  azz=cmset_op(reform(arr1[0,aa]),'AND',reform(arr1[0,bb]),/index,count=nyrs)
;  bzz=cmset_op(reform(arr1[0,bb]),'AND',reform(arr1[0,aa]),/index,count=nyrs)
  arr1[1:6,aa[azz]]=arr1[1:6,aa[azz]]+arr1[1:6,bb[bzz]]
  ;index 7, per capita will just be the one of the large country
  arr1[8,aa[azz]]=arr1[8,aa[azz]]+arr1[8,bb[bzz]]
endfor

;SOMALIA no longer needs interpolation as of 2014 CDIAC verion

;delete
;a) littlecountries from above
;and
;b) some other small ones, mainly islands

;in 2013 version, (BONAIRE, SAINT EUSTATIUS, AND SABA), CURACAO, LICHTENSTEIN, NETHERLAND ANTILLES,SAINT MARTIN (DUTCH PORTION) were added (they do not have full records from 1992 - 2013, which makes
;implementation in ff_country_new20xx.pro difficult.)
;in 2014 version, delete TUVALU
;
;Liechtenstein now appears fine in CDIAC data that goes through 2017.  So merge with Switzerland for now instead of deleting
;
;Deletion of first two countries (South Sudan and Montenegro) is related to historical country splits (see above)
delete=['REPUBLIC OF SOUTH SUDAN','MONTENEGRO',littlecountry,'CAYMAN ISLANDS','NIUE','MONTSERRAT','PALAU','BRITISH VIRGIN ISLANDS','ANTARCTIC FISHERIES','SAINT HELENA',$
  'WALLIS AND FUTUNA ISLANDS','MARSHALL ISLANDS','FEDERATED STATES OF MICRONESIA','TURKS AND CAICOS ISLANDS','BONAIRE, SAINT EUSTATIUS, AND SABA','CURACAO',$
  'NETHERLAND ANTILLES','SAINT MARTIN (DUTCH PORTION)','TUVALU','MAYOTTE']

;NOTE:  The following countries do have GISS codes -- although they may not have full records from 1992 onwards as mentioned above
;Liechtenstein, Netherlands Antilles, Turks and Caicos, Tuvalu, 

;these French departments have cement only reported as the total from 2015-2019, so delete these rows
fr_dept=['FRENCH GUIANA','GUADELOUPE','MARTINIQUE','REUNION']

;revise namearr and arr1
rmidx = [-999]
for i=0,fr_dept.length-1 do begin
  aa=where(namearr eq fr_dept[i] and arr1[0,*] ge 2015,countaa)
  rmidx = [rmidx,aa]
endfor
rmidx = rmidx[1:*]

newidx = cmset_op(indgen(namearr.length),'and',rmidx,/not2)

tmpname = namearr[newidx]
tmparr = arr1[*,newidx]

namearr = tmpname
arr1 = tmparr
;-----end of removal of 2015-2019 French departments

ndel=n_elements(delete)
ct=[0]
ct2=0
idx=[0]
idx2=[0]
for i=0,ndel-1 do begin
  aa=where(namearr eq delete[i],countaa)
  ct=[ct,countaa]
  idx=[idx,aa]
  bb=where(countries eq delete[i],countbb)
  ct2=ct2+countbb
  idx2=[idx2,bb]
endfor
idx=idx[1:*]
idx2=idx2[1:*]

namearrtmp=namearr
namearrtmp[idx]=''
countriestmp=countries
countries[idx2]=''

xx=where(namearrtmp ne '',nlines3)
yy=where(countries ne '',ncountries2)

namearr2=namearr[xx]
arr2=arr1[*,xx]
countries2=countries[yy]

;re-sort
k1=namearr2
yr=arr2[0,*]
i1=multisort(k1,yr)

namearr2=namearr2[i1]
arr2=arr2[*,i1]
yr=yr[i1]

;check to see that each country has data at least between yr1 and yr2, corresponding to latest run ff_country_new20xx.pro
cidx=uniq(namearr2)
namearr3=namearr2[cidx]
idx=[-1,cidx]

;
;TO TEST FOR THE PRESENCE OF INCOMPLETE RECORDS
;
;for i=0,cidx.length-1 do begin
;  tempyr=yr[idx[i]+1:idx[i+1]]
;  if (min(tempyr) le yr1 and max(tempyr) eq yr2) then continue else stop,namearr2[idx[i+1]]
;endfor

;for _2017 data potentially delete or extrapolate (and then subtract from France)
;all of these now end in 2010, and 2011 onwards are included in France totals
;
;For reference France in 2010 was 95878 x 10^9 g C
;
;Country -- (emissions in 10^9 g C in 2010)
;French Guiana  -- 174
;Guadeloupe -- 612
;Martinique -- 561
;Reunion -- 1096
;
;together, these countries are about 2% of France's total
;
;take 2010 value for these countries, and extend through 2019
;however, these departments have cement only reported as the total from 2015-2019, so delete these rows
;fr_dept=['FRENCH GUIANA','GUADELOUPE','MARTINIQUE','REUNION']

fr_dept_extrap_yr1=2011
fr_dept_extrap_yr2=yr2
fr_dept_nyr=fr_dept_extrap_yr2-fr_dept_extrap_yr1+1
nlines4=namearr2.length+fr_dept.length*fr_dept_nyr
fr_dept_yrarr=indgen(fr_dept_nyr)+fr_dept_extrap_yr1


namearr2a=strarr(nlines4)
arr2a=fltarr(9,nlines4)
namearr2a[0:nlines3-1]=namearr2
arr2a[*,0:nlines3-1]=arr2

;array for collecting fluxes
fluxarr=fltarr(8,fr_dept.length)

;add lines to end and then do multi-sort
for i=0,fr_dept.length-1 do begin
  ;indices for namearr2a and arr2a
  ;find last occurance of each country
  aa=where(namearr2 eq fr_dept[i],countaa)
  if countaa lt 2 then stop
  idx0a=nlines3+i*fr_dept_nyr
  idx1a=idx0a+fr_dept_nyr-1
  idx0=aa[-1]
  ;country name
  namearr2a[idx0a:idx1a]=namearr2[idx0]
  ;year
  arr2a[0,idx0a:idx1a]=fr_dept_yrarr
  ;fluxes
  for j=0,fr_dept_nyr-1 do arr2a[1:8,idx0a+j]=arr2[1:8,idx0]
  fluxarr[*,i]=arr2[1:8,idx0]
endfor

;subtract fluxes from France's
fluxarr2=total(fluxarr,2)
aa=where(namearr2a eq 'FRANCE' and yr ge fr_dept_extrap_yr1 and yr le fr_dept_extrap_yr2,countaa)
if countaa ne fr_dept_nyr then stop
for i=0,fr_dept_nyr-1 do begin
  ;fluxes total, etc.
  arr2a[1:6,aa[i]]=arr2a[1:6,aa[i]]-fluxarr2[0:5]
  ;skip per capita, then do bunker
  arr2a[8,aa[i]]=arr2a[8,aa[i]]-fluxarr2[7]
endfor

;re-sort
k1a=namearr2a
yra=arr2a[0,*]
i1a=multisort(k1a,yra)

namearr2a=namearr2a[i1a]
arr2a=arr2a[*,i1a]


;;start by filling up first part of arrays with old arrays
;namearr2a[0:idx1arr[0]]=namearr2[0:idx1arr[0]]
;arr2a[*,0:idx1arr[0]]=arr2[*,0:idx1arr[0]]
;for i=0,fr_dept.length-1 do begin
;  ;extend for fr_dept_nyr
;  idx0=idx1arr[i]+1
;  idx1=idx1arr[i]+1+fr_dept_nyr-1
;  idx0a=idx0+fr_dept_nyr*i
;  idx1a=idx1+fr_dept_nyr*i
;  ;country name
;  namearr2a[idx0a:idx1a]=namearr2[idx0-1]
;  ;year
;  arr2a[0,idx0a:idx1a]=indgen(fr_dept_nyr)+fr_dept_extrap_yr1
;  ;fluxes
;  for j=0,fr_dept_nyr-1 do arr2a[1:8,idx0a+j]=arr2[1:8,idx0-1]
;  ;between the end of an extended country and the start of the next, or the end of the file
;  ;len = number of lines after end of fr_dept[i] and before beginning of fr_dept[i+1]
;  len=idx1arr[i+1]-idx1arr[i]
;  namearr2a[idx1a+1:idx1a+1+len-1]=namearr2[idx0:idx0+len-1]
;  arr2a[*,idx1a+1:idx1a+1+len-1]=arr2[*,idx0:idx0+len-1]
;endfor


stop

;write out as txt or csv file
;outfile=dir+strmid(file,0,16)+'.mod2.txt'
;find maximum string length of countries2 element
;maxlen=max(strlen(countries2))
;format='(A'+strtrim(maxlen+1,2)+',I5,3I8,3I7,F8.2,I7)'
;openw,unit,outfile,/get_lun
;for i=0,nlines3-1 do $
;  printf,unit,namearr2[i],arr2[*,i],format=format
;free_lun,unit
outfile=dir+strmid(file,0,16)+'.mod2.csv'
openw,unit,outfile,/get_lun
for i=0,nlines4-1 do $
  printf,unit,namearr2a[i]+','+strjoin(strtrim(arr2a[*,i],2),',')
free_lun,unit


;write out as csv file
;NOTE:  Does not work well because write_csv.pro encapsulates all strings in quotes;
;   ok for excel, but not for general use
  ;outfile=dir+strmid(file,0,16)+'.mod2.csv'
  ;outarr=create_struct(replicate('field',10)+strtrim(indgen(10),2),namearr2,arr2[0,*],arr2[1,*],arr2[2,*],$
  ;  arr2[3,*],arr2[4,*],arr2[5,*],arr2[6,*],arr2[7,*],arr2[8,*])
  ;write_csv,outfile,outarr





end
