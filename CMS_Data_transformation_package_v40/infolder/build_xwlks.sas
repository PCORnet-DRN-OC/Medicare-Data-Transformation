%macro build_xwlks;

  /* Redirect listing of control flow info to its own log file */ ;
  proc printto new log="&epath/outfolder/build_xwlks.log" ; run ;

  %do yr = &startyr %to &endyr;

    proc sql;
	 %if %length(&IPTBL) > 0 %then %do;
	  select distinct memname into :iptb from dictionary.tables 
        where lowcase(libname)="lib_ip" and lowcase(memname) ? "&IPTBL" 
        and memname ? "&yr" and memtype='DATA';
	 %end;
	 %else %if %length(&MPTBL) > 0 %then %do;
	  select distinct memname into :mptb from dictionary.tables 
        where lowcase(libname)="lib_mp" and lowcase(memname) ? "&MPTBL" 
        and memname ? "&yr" and memtype='DATA';
	 %end;

	  %if %length(&OPTBL) > 0 %then %do; 
	   select distinct memname into :optb from dictionary.tables 
        where lowcase(libname)="lib_op" and lowcase(memname) ? "&OPTBL" 
        and memname ? "&yr" and memtype='DATA';
      %end;

	  %if %length(&SNFTBL) > 0 %then %do; 
	   select distinct memname into :snftb from dictionary.tables 
        where lowcase(libname)="lib_snf" and lowcase(memname) ? "&SNFTBL" 
        and memname ? "&yr" and memtype='DATA';
      %end;

      create table pcordata.facility_temp&yr as 
	    %if %length(&IPTBL) > 0 %then %do;
         select 'PROVIDER' as facility_class, PROVIDER as source_id 
         from lib_ip.&iptb (keep=&FAC_KEEP %if &nametype eq L %then %do; rename=(&FAC_RN) %end;)
         where PROVIDER is not missing 
		%end;
	    %else %if %length(&MPTBL) > 0 %then %do;
	     select 'PROVIDER' as facility_class, PRVDRNUM as source_id 
         from lib_mp.&mptb (keep=&FAC_MP_KEEP %if &nametype eq L %then %do; rename=(&FAC_MP_RN) %end;)
         where PRVDRNUM is not missing 
		%end;
	    %if %length(&OPTBL) > 0 %then %do; 
        union
        select 'PROVIDER' as facility_class, PROVIDER as source_id 
        from lib_op.&optb(keep=&FAC_KEEP %if &nametype eq L %then %do; rename=(&FAC_RN) %end;)
        where PROVIDER is not missing   
        %end;
        %if %length(&SNFTBL) > 0 %then %do; 
        union
        select 'PROVIDER' as facility_class, PROVIDER as source_id 
        from lib_snf.&snftb(keep=&FAC_KEEP %if &nametype eq L %then %do; rename=(&FAC_RN) %end;)
        where PROVIDER is not missing   
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
	FACILITYID = put(md5(source_id), hex8.);
  run;

  proc datasets library=pcordata nolist;
    delete facility_temp: ;		
  quit;

  /* Restoring the default destination */;
  proc printto log=log; run;  

%mend;

%build_xwlks;

