/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  build_provider.sas                          
*  Create Date:  08/20/2018      
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to create CDM provider table  
*           based on clinical provider information in CMS claims
*
*  Inputs:  
*           1) Inpatient, outpatient, carrier and snf claim tables
*                                       
*  Output:  
*           1) Provider table at /etl/cdm_v41 
*           2) SAS log file in /etl/outfolder
*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;
%macro assignvars_prov;   
  PROVIDER_NPI_FLAG = 'Y';
%mend;
%macro temp(clm=);
   proc sql; 
		create table &clm._temp&yr as
	      select distinct AT_NPI as NPI, input(AT_NPI,10.) as PROVIDER_NPI,%if &yr ge 2016 %then %do; AT_PHYSN_SPCLTY_CD %end; 
               %else %do; "" %end; as RAW_PROVIDER_SPECIALTY_PRIMARY  
          from lib_&clm..&tbnm(keep=&AT_KEEP %if &yr ge 2016 %then %do; AT_PHYSN_SPCLTY_CD %end; %if &nametype eq L %then %do; rename=(&AT_RN) %end;)
          where AT_NPI is not missing;
      quit;
%mend;
%macro arrange_prov(clm= );
   proc sql;
	  create table &clm._prov&yr as
	     select a.*, COALESCE(b.target_value, 'ZZ') as PROVIDER_SPECIALTY_PRIMARY
         from &clm._temp&yr a left join ref.provider_specialty_mapping b
         on lowcase(a.RAW_PROVIDER_SPECIALTY_PRIMARY) = lowcase(b.Source_Code);
   quit;

   data &clm._prov&yr;
      %add_newvars_prov;
      set &clm._prov&yr;
      PROVIDERID = strip(put(md5(NPI), hex10.));
      /*- To assign variable names from source to target -*/;
      %assignvars_prov;
    run;

	proc sort data = &clm._prov&yr; by PROVIDERID PROVIDER_SPECIALTY_PRIMARY; run;

   data &clm._prov&yr;
     set &clm._prov&yr;
	 by PROVIDERID;
	 if first.PROVIDERID;
   run;

    /* clean data, append data to the target table */;
     proc sql;
      create table pcordata.&PROVTBL&yr as
        select &provider_order
        from &clm._prov&yr;        
     quit; 

	 %clean_labels(pcordata, &PROVTBL&yr);  
  

   proc contents data=pcordata._all_ noprint out=pcordata; run;

   proc sort data=pcordata(keep=memname) nodupkey; by memname; run;

   proc sql noprint;
     select count(*) into :cnt from pcordata
            where memname="PROVIDER";
   quit;
   
	%if %eval(&cnt > 0) %then %do;
      proc sql;
       create table &clm._lookup as
	     select PROVIDERID from pcordata.&PROVTBL&yr
	     except
         select PROVIDERID from pcordata.&PROVTBL;
      quit;

	  %if %eval(&sqlobs > 0) %then %do; 
  
       data pcordata.&PROVTBL&yr;  
         if _N_ eq 1 then do;
	      declare hash h(hashexp:16, dataset: "&clm._lookup");
          h.defineKey('PROVIDERID');
          h.defineDone();
         end;
         set pcordata.&PROVTBL&yr;
         if h.find() = 0 then
         output;
       run;

	   proc append base = pcordata.&PROVTBL data=pcordata.&PROVTBL&yr; run;
      %end;

	  proc sql;
       create table update_lookup as
	     select distinct a.PROVIDER_NPI, a.PROVIDER_SPECIALTY_PRIMARY, a.RAW_PROVIDER_SPECIALTY_PRIMARY
		 from pcordata.&PROVTBL&yr a, pcordata.&PROVTBL b
          where b.PROVIDER_SPECIALTY_PRIMARY in ('ZZ', 'UN', 'OT') 
	      and a.PROVIDER_SPECIALTY_PRIMARY ^in ('ZZ', 'UN', 'OT')
          and a.PROVIDERID = b.PROVIDERID;
      quit;

	  %if %eval(&sqlobs > 0) %then %do; 
	   data pcordata.&PROVTBL ;
        if _n_ = 1 then do ;
        dcl hash h(hashexp:16, dataset: "update_lookup") ;
        h.definekey('PROVIDERID') ;
        h.DEFINEDATA("PROVIDER_SPECIALTY_PRIMARY", "RAW_PROVIDER_SPECIALTY_PRIMARY") ;
        h.definedone() ;
       end ;
       set pcordata.&PROVTBL ;
       if h.FIND() = 0 then do; put "provider updated"; end;
      run ;
     %end;  
	%end; /*end if */
    %else %if %eval(&cnt = 0) %then %do;
      data pcordata.&PROVTBL; set pcordata.&PROVTBL&yr; run;
    %end;

    %clean;
    proc datasets library=pcordata nolist; delete &PROVTBL&yr;	
    quit;
%mend;
%macro build_provider;

  /* Redirect listing of control flow info to its own log file */ ;
  proc printto new log="&epath/outfolder/build_provider.log" ; run ;

  proc datasets library=pcordata nolist; delete &PROVTBL; quit;
  %if %length(&CARTBL) > 0 %then %do;
    %do yr = &endyr %to &startyr %by -1;
    proc sql; 
      create table carln as 
        select distinct memname from dictionary.tables 
        where lowcase(libname)="lib_carr" and lowcase(memname) ? "&CARLNTBL" 
        and memname ? "&yr" and memtype='DATA';
    quit;

    data _null_;
      set carln end=end;
      cnt+1;
      call symputx('cl'||put(cnt,4.-l),memname);
      if end then call symputx('clmax',cnt);
    run;

	proc sql;
	  create table carr_temp&yr as
	    %do i=1 %to &clmax;
            select distinct PRF_NPI as NPI, input(strip(PRF_NPI),10.) as PROVIDER_NPI,HCFASPCL as RAW_PROVIDER_SPECIALTY_PRIMARY
	        from lib_carr.&&cl&i(keep=&PRF_KEEP %if &nametype eq L %then %do; rename=(&PRF_RN) %end;)
            where PRF_NPI is not missing
	        %if %eval(&i < &clmax) %then %do; union %end;
          %end;;
    quit;

    %arrange_prov(clm=carr);
   %end;/*end of year*/
   %end; /*end of carr line table*/

   %if %length(&OPTBL) > 0 %then %do;
    %do yr = &endyr %to &startyr %by -1;
	  %find_fname(flib=lib_op, memnm=&OPTBL);
	  %temp(clm=op);
      %arrange_prov(clm=op);
	 %end;/*end of year*/
   %end; /*end of op table*/

   %if %length(&IPTBL) > 0 %then %do;
    %do yr = &endyr %to &startyr %by -1;
	  %find_fname(flib=lib_ip, memnm=&IPTBL);
	  %temp(clm=ip);
	  %arrange_prov(clm=ip);
	 %end;/*end of year*/
   %end; /*end of ip table*/

   %if %length(&SNFTBL) > 0 %then %do;
     %do yr = &endyr %to &startyr %by -1;
	  %find_fname(flib=lib_snf, memnm=&SNFTBL);
	  %temp(clm=snf);
      %arrange_prov(clm=snf);
	 %end;/*end of year*/
   %end; /*end of ip table*/

   data pcordata.provider; 
     set pcordata.provider; 
     if PROVIDER_NPI ^= .; 
     if PROVIDER_SPECIALTY_PRIMARY = 'ZZ' then PROVIDER_SPECIALTY_PRIMARY='NI'; run;
   run;

  /* Restoring the default destination */;
  proc printto log=log; run;  

  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_PROVIDER_DATE = input("&sysdate9", date9.); 
  run;

%mend;

%build_provider;

