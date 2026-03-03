;pro edgar_ffv80



dir='/Users/john/EDGAR/edgarv8/TOTALS_flx_nc/'
fileroot0='v8.0_FT2022_GHG_CO2_'
fileend0='_TOTALS_flx.nc'
yr1=1992
yr2=2022
nyears=yr2-yr1+1
yrstrarr=strtrim(indgen(nyears)+yr1,2)

ffarr0=fltarr(3600,1800,nyears)
m2=grid_area2(0.1,0.1)

for i=0,nyears-1 do begin
;for i=0,0 do begin
  ;seconds per year
  s2yr=secperyear(yr=strtrim(yrstrarr[i],2))
  ;read in files -- Units are kg CO2/m2/s
  ccg_ncread,file=dir+fileroot0+yrstrarr[i]+fileend0,res0,/silent
  tmp=res0.fluxes.data*m2*1000.*s2yr*12./44.  ;now in gC/cell/yr
  ;for v8, fluxes start at dateline
  ;;flux starts at Prime Meridian, not dateline, so shift array by 180 deg.
  ;ffarr0[*,*,i]=shift(tmp,1800,0)
  ffarr0[*,*,i]=tmp
endfor

ffarr1=rebin(ffarr0,360,180,nyears)*10.*10.

savefile=dir+'edgarffv80_1992_2022.sav'
save,filename=savefile,ffarr1


end
