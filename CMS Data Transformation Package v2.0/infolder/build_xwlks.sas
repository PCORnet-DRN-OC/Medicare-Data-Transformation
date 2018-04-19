%macro build_xwlks;

  /* Redirect listing of control flow info to its own log file */ ;
  proc printto new log="&epath/outfolder/build_xwlks.log" ; run ;

  %do yr = &startyr %to &endyr;
    proc sql; 
      create table carln as 
        select distinct memname from dictionary.tables 
        where lowcase(memname) ? "&CARLNTBL" and memname ? "&yr" and memtype='DATA';
      quit;

    data _null_;
      set carln end=end;
      cnt+1;
      call symputx('cl'||put(cnt,4.-l),memname);
      if end then call symputx('clmax',cnt);
    run;

    proc sql;
      create table pcordata.facility_temp&yr as 
        select 'PROVIDER' as facility_class, PROVIDER as source_id 
        from lib_ip.&IPTBL&yr (keep=&FAC_KEEP %if &nametype eq L %then %do; rename=(&FAC_RN) %end;)
        where PROVIDER is not missing 
	    %if %length(&OPTBL) > 0 %then %do; 
        union
        select 'PROVIDER' as facility_class, PROVIDER as source_id 
        from lib_op.&OPTBL&yr(keep=&FAC_KEEP %if &nametype eq L %then %do; rename=(&FAC_RN) %end;)
        where PROVIDER is not missing   
        %end;;

	  create table pcordata.provider_temp&yr as    
        select 'AT_NPI' as provider_class, AT_NPI as source_id  
        from lib_ip.&IPTBL&yr (keep=&AT_KEEP %if &nametype eq L %then %do; rename=(&AT_RN) %end;)
        where AT_NPI is not missing 
	    union
        select 'AT_NPI' as provider_class, AT_NPI as source_id  
        from lib_op.&OPTBL&yr(keep=&AT_KEEP %if &nametype eq L %then %do; rename=(&AT_RN) %end;)
        where AT_NPI is not missing
	    %if %length(&CARTBL) > 0 %then %do;
          union
	      %do i=1 %to &clmax;
            select 'PRF_NPI' as provider_class, PRF_NPI as source_id
	        from lib_carr.&&cl&i(keep=&PRF_KEEP %if &nametype eq L %then %do; rename=(&PRF_RN) %end;)
            where PRF_NPI is not missing
	        %if %eval(&i < &clmax) %then %do; union %end;
          %end;
        %end;; 
    quit;
  %end;

  proc sql; 
    create table pcordata.facility as 
      %do yr = &startyr %to &endyr;
        select * from pcordata.facility_temp&yr.
		%if %eval(&yr < &endyr) %then %do; union %end;
      %end;;
  quit;

  data pcordata.facility;
    set pcordata.facility;
	FACILITYID = put(crcxx1(source_id), hex8.);
  run;

  proc sql; 
    create table pcordata.provider as 
      %do yr = &startyr %to &endyr;
         select * from pcordata.provider_temp&yr.
		 %if %eval(&yr < &endyr) %then %do; union %end;
      %end;;
  quit;

  proc datasets library=pcordata nolist;
    delete facility_temp: provider_temp:;		
  quit;

  data pcordata.provider;
    set pcordata.provider;
	PROVIDERID = put(crcxx1(source_id), hex8.);
  run;

  /* Restoring the default destination */;
  proc printto log=log; run;  

%mend;

%build_xwlks;

