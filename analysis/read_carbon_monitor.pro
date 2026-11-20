;pro read_carbon_monitor

;read in carbon monitor near real time emissions to deal with COVID transitions

;data from https://carbonmonitor.org/

dir='/Users/john/EDGAR/carbon_monitor/'
;file='carbonmonitor-global_datas_2021-06-08.csv'   ;through 30/4/21
;file='carbonmonitor-global_datas_2022-12-06.csv    ;through 30/9/22
file='carbonmonitor-global_datas_2023-06-21.csv'    ;through 30/04/2023


;Units appear to be Tg CO2
;emissions are organized as daily values starting 1/1/19 ending [30/4/21; 30/9/22; 30/04/2023], separated into the following sectors:
;Power
;Industry
;Ground Transport
;Residential
;Domestic Aviation
;International Aviation
;-- we will ignore aviation for the time being
;
;and the following countries/regions:
;Brazil
;China
;EU27 & UK
;France
;Germany
;India
;Italy
;Japan
;ROW
;Russia
;Spain
;UK
;WORLD
;US


res=read_csv(dir+file,header=header,count=count)
country=res.field1
date=res.field2     ;note: day/month/year
sector=res.field3
emis=res.field4
timestamp=res.field5  ;seems to be seconds since Jan. 1, 1970 00:00 UTC

;create array of ncountries, nmonths, nsectors
;parse date
;didx=uniq(date,sort(date))
;ndate=didx.length
yr=intarr(count)
mo=intarr(count)
dy=intarr(count)
for i=0,count-1 do begin
  tmp=strsplit(date[i],'/',/extract)
  yr[i]=fix(tmp[2])
  mo[i]=fix(tmp[1])
  dy[i]=fix(tmp[0])
endfor

didx=uniq(date,sort(date))
yrarr=yr[didx]
moarr=mo[didx]
dyarr=dy[didx]


;find uniq months
yrmo=yr*100L+mo
midx=uniq(yrmo,sort(yrmo))
nmo=midx.length
yrmoarr=yrmo[midx]
;countries/regions
cidx=uniq(country,sort(country))
nco=cidx.length
countryarr=country[cidx]
;sectors
sidx=uniq(sector,sort(sector))
nsec=sidx.length
sectorarr=sector[sidx]

emisarr1=fltarr(nmo,nco,nsec)

for i=0,nmo-1 do begin
  aa=where(yrmo eq yrmoarr[i],countaa)
  tmpemisa=emis[aa]
  tmpcountrya=country[aa]
  tmpsectora=sector[aa]
  for j=0,nco-1 do begin
    bb=where(tmpcountrya eq countryarr[j],countbb)
    tempemisb=tmpemisa[bb]
    tmpsectorb=tmpsectora[bb]
    for k=0,nsec-1 do begin
      cc=where(tmpsectorb eq sectorarr[k],countcc)
      emisarr1[i,j,k]=total(tempemisb[cc],/double)
    endfor
  endfor
endfor

;replace 'EU27 & UK' with 'RO_EU28', i.e. EU28 minus sum(France,Germany,Italy,Spain,UK)
aa=where(countryarr eq 'EU27 & UK',complement=cc)
bb=where(countryarr eq 'France' or countryarr eq 'Germany' or countryarr eq 'Italy' or $
  countryarr eq 'Spain' or countryarr eq 'UK')

emisarr1[*,aa,*]=emisarr1[*,aa,*]-total(emisarr1[*,bb,*],2)
countryarr[aa]='RO_EU28'
countryarr=strupcase(countryarr)

;exclude aviation sectors and sum over rest of them
cc=where(sectorarr eq 'Domestic Aviation' or sectorarr eq 'International Aviation',countaa,complement=dd)
emisarr2=total(emisarr1[*,*,dd],3)

;time definitions for output
yr1=min(yrarr)
yr2=max(yrarr)
;for years arrays, full years are required
nyr=floor(nmo/12.)
yrarr2=indgen(nyr)+yr1

ratioarr_mox=findgen(nmo-12)/12.+1./24.+yr1+1    ;ratio time starts one full year after 2019
ratioarr_yrx=findgen(nyr-1)+1./2.+yr1+1    ;ratio time starts one full year after 2019

;create monthly ratio array -- last gap months of last year will be NaN
ratioarr_moy=fltarr(nmo-12,nco)+!values.f_nan
for i=0,nmo-1-12 do $
  ratioarr_moy[i,*]=emisarr2[i+12,*]/emisarr2[i,*]


;create yearly ratio array
emisarr2a=fltarr(nyr,nco)
for i=0,nyr-1 do begin
  zz=where(yrmoarr/100L eq yr1+i)
  emisarr2a[i,*]=total(emisarr2[zz,*],1,/double)
endfor

;create monthly ratio array -- last gap months of last year will be NaN
ratioarr_yry=fltarr(nyr-1,nco)+!values.f_nan
for i=0,nyr-1-1 do $
  ratioarr_yry[i,*]=emisarr2a[i+1,*]/emisarr2a[i,*]


;explicitly define RO_EU8 and add these ratios to the arrays
;read in file of these countries (Croatia and Slovenia are part of 'Yugoslavia' so they are excluded in 'mod1')
file='RO_EU28.mod1.txt'
ccg_sread,file=dir+file,skip=2,ro_eu28
nco2=ro_eu28.length

;
;New arrarys
;
;
;Country
nco3=nco-1+nco2
countryarrnew=strarr(nco3)
;part prior to RO_EU28
countryarrnew[0:aa[0]-1]=countryarr[0:aa[0]-1]   ;not robust because aa[0] could be 0
;RO_EU28
countryarrnew[aa[0]:aa[0]+nco2-1]=strupcase(ro_eu28)
;part after
countryarrnew[aa[0]+nco2:*]=countryarr[aa[0]+1:*]

;Monthly ratios
ratioarr_moynew=fltarr(nmo-12,nco3)
;part prior to RO_EU28
ratioarr_moynew[*,0:aa[0]-1]=ratioarr_moy[*,0:aa[0]-1]   ;not robust because aa[0] could be 0
;RO_EU28
for i=0,nco2-1 do begin
  ratioarr_moynew[*,aa[0]+i]=ratioarr_moy[*,aa[0]]
endfor
;part after
ratioarr_moynew[*,aa[0]+nco2:*]=ratioarr_moy[*,aa[0]+1:*]

;Annual ratios
ratioarr_yrynew=fltarr(nyr-1,nco-1+nco2)
;part prior to RO_EU28
ratioarr_yrynew[*,0:aa[0]-1]=ratioarr_yry[*,0:aa[0]-1]   ;not robust because aa[0] could be 0
;RO_EU28
for i=0,nco2-1 do begin
  ratioarr_yrynew[*,aa[0]+i]=ratioarr_yry[*,aa[0]]
endfor
;part after
ratioarr_yrynew[*,aa[0]+nco2:*]=ratioarr_yry[*,aa[0]+1:*]


;sort arrays alphabetically putting ROW and WORLD at the end
;1st. modify names in countryarr to be as with CDIAC and GISS naming
;Italy -> ITALY (INCLUDING SAN MARINO)
;UK -> UNITED KINGDOM
;US -> UNITED STATES OF AMERICA
orig=['ITALY','UK','US']
new=['ITALY (INCLUDING SAN MARINO)',$
  'UNITED KINGDOM',$
  'UNITED STATES OF AMERICA']
for i=0,2 do begin
  aa=where(countryarrnew eq orig[i],countaa)
  if countaa ne 1 then stop
  countryarrnew[aa]=new[i]
endfor

aa=where(countryarrnew eq 'WORLD' or countryarrnew eq 'ROW',countaa,complement=bb,ncomplement=countbb)
a1=where(countryarrnew eq 'ROW',counta1)
a2=where(countryarrnew eq 'WORLD',counta2)
if countaa ne 2 then stop
if counta1 ne 1 then stop
if counta1 ne 1 then stop

;now alphabetically sorted
countryarrnew2=strarr(nco3)
ratioarr_moynew2=fltarr(nmo-12,nco3)
ratioarr_yrynew2=fltarr(nyr-1,nco3)

sidx=sort(countryarrnew[bb])

countryarrnew2[0:countbb-1]=countryarrnew[bb[sidx]]
countryarrnew2[countbb]=countryarrnew[a1]
countryarrnew2[countbb+1]=countryarrnew[a2]

ratioarr_moynew2[*,0:countbb-1]=ratioarr_moynew[*,bb[sidx]]
ratioarr_moynew2[*,countbb]=ratioarr_moynew[*,a1]
ratioarr_moynew2[*,countbb+1]=ratioarr_moynew[*,a2]

ratioarr_yrynew2[*,0:countbb-1]=ratioarr_yrynew[*,bb[sidx]]
ratioarr_yrynew2[*,countbb]=ratioarr_yrynew[*,a1]
ratioarr_yrynew2[*,countbb+1]=ratioarr_yrynew[*,a2]

stop
;write out emission, ratio, and country arrays
;
;ORIGINAL -- UNSORTED AND WITHOUT RO_EU28
;
;0. country
countryfile=dir+'carbon_monitor_countries_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
ccg_swrite,file=countryfile,countryarr

;emissions
;1. annual
annfile=dir+'carbon_monitor_emis_ann_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
colwidth=10
;header
openw,lun,annfile,/get_lun
printf,lun,yrarr2,format='('+strtrim(nyr,2)+'I'+strtrim(colwidth,2)+')'
;emissions
for i=0,nco-1 do printf,lun,emisarr2a[*,i],format='('+strtrim(nyr,2)+'F'+strtrim(colwidth,2)+'.3)'
free_lun,lun
;2. monthly
mofile=dir+'carbon_monitor_emis_mo_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
colwidth=10
;header
openw,lun,mofile,/get_lun
printf,lun,yrmoarr,format='('+strtrim(nmo,2)+'I'+strtrim(colwidth,2)+')'
;emissions
for i=0,nco-1 do printf,lun,emisarr2[*,i],format='('+strtrim(nmo,2)+'F'+strtrim(colwidth,2)+'.3)'
free_lun,lun

;ratios
;3. annual
annfiler=dir+'carbon_monitor_ratio_ann_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
colwidth=10
;header
openw,lun,annfiler,/get_lun
printf,lun,yrarr2[1:*],format='('+strtrim(nyr-1,2)+'I'+strtrim(colwidth,2)+')'
;emissions
for i=0,nco-1 do printf,lun,ratioarr_yry[*,i],format='('+strtrim(nyr-1,2)+'F'+strtrim(colwidth,2)+'.3)'
free_lun,lun
;2. monthly
mofiler=dir+'carbon_monitor_ratio_mo_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
colwidth=10
;header
openw,lun,mofiler,/get_lun
printf,lun,yrmoarr[12:*],format='('+strtrim(nmo-12,2)+'I'+strtrim(colwidth,2)+')'
;emissions
for i=0,nco-1 do printf,lun,ratioarr_moy[*,i],format='('+strtrim(nmo-12,2)+'F'+strtrim(colwidth,2)+'.3)'
free_lun,lun

;write out emission, ratio, and country arrays
;
;NEW -- ALPHABETICALLY SORTED AND WITH RO_EU28
;
;0. country
countryfile2=dir+'carbon_monitor_countries2_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
ccg_swrite,file=countryfile2,countryarrnew2

;
;SORT EMISSIONS LATER
;
;;emissions
;;1. annual
;annfile=dir+'carbon_monitor_emis_ann2_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
;colwidth=10
;;header
;openw,lun,annfile,/get_lun
;printf,lun,yrarr2,format='('+strtrim(nyr,2)+'I'+strtrim(colwidth,2)+')'
;;emissions
;for i=0,nco-1 do printf,lun,emisarr2a[*,i],format='('+strtrim(nyr,2)+'F'+strtrim(colwidth,2)+'.3)'
;free_lun,lun
;;2. monthly
;mofile=dir+'carbon_monitor_emis_mo2_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
;colwidth=10
;;header
;openw,lun,mofile,/get_lun
;printf,lun,yrmoarr,format='('+strtrim(nmo,2)+'I'+strtrim(colwidth,2)+')'
;;emissions
;for i=0,nco-1 do printf,lun,emisarr2[*,i],format='('+strtrim(nmo,2)+'F'+strtrim(colwidth,2)+'.3)'
;free_lun,lun

;ratios
;3. annual
annfiler2=dir+'carbon_monitor_ratio_ann2_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
colwidth=10
;header
openw,lun,annfiler2,/get_lun
printf,lun,yrarr2[1:*],format='('+strtrim(nyr-1,2)+'I'+strtrim(colwidth,2)+')'
;emissions
for i=0,nco-1 do printf,lun,ratioarr_yrynew2[*,i],format='('+strtrim(nyr-1,2)+'F'+strtrim(colwidth,2)+'.3)'
free_lun,lun
;2. monthly
mofiler2=dir+'carbon_monitor_ratio_mo2_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.txt'
colwidth=10
;header
openw,lun,mofiler2,/get_lun
printf,lun,yrmoarr[12:*],format='('+strtrim(nmo-12,2)+'I'+strtrim(colwidth,2)+')'
;emissions
for i=0,nco-1 do printf,lun,ratioarr_moynew2[*,i],format='('+strtrim(nmo-12,2)+'F'+strtrim(colwidth,2)+'.3)'
free_lun,lun



;output to .sav file
;with RO_EU28, unsorted
cm_countryarrnew=countryarrnew
file=dir+'carbon_monitor_frac_inc_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.sav'
save,filename=file,ratioarr_moynew,ratioarr_mox,ratioarr_yrynew,ratioarr_yrx,cm_countryarrnew

;also sorted
cm_countryarrnew2=countryarrnew2
file=dir+'carbon_monitor_frac_inc2_'+strtrim(min(yrmo),2)+'_'+strtrim(max(yrmo),2)+'.sav'
save,filename=file,ratioarr_moynew2,ratioarr_mox,ratioarr_yrynew2,ratioarr_yrx,cm_countryarrnew2


end
