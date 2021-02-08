
%macro create_cdm_v51_tablesviews;

  %let dsview = %lowcase(&dsview);
  %let n = 1;
  %if &dsview eq b %then %do; %let n = 2; %end;
  
  %do i=1 %to &n;
    %if %eval(&i = 2) %then %do;
      %create_cdm_tbl; 
      %let dsview = y; 
    %end;
    %if &dsview eq y %then %do;
	  libname pcordata clear;
	  libname pcordata "&epath.cdm_v51/dsview";

        data pcordata.&DEMTBL / view=pcordata.&DEMTBL;
         set "&epath.cdm_v51/&DEMTBL";
	    run;

	    data pcordata.&DEATTBL / view=pcordata.&DEATTBL;
         set "&epath.cdm_v51/&DEATTBL";
	    run;

		data pcordata.&ADDRTBL / view=pcordata.&ADDRTBL;
         set "&epath.cdm_v51/&ADDRTBL";
	    run;

	    data pcordata.&ENRTBL / view=pcordata.&ENRTBL;
         set "&epath.cdm_v51/&ENRTBL";
	    run;

	    data pcordata.HARVEST / view=pcordata.HARVEST;
         set "&epath.cdm_v51/HARVEST";
	    run;

		data pcordata.PROVIDER / view=pcordata.PROVIDER;
         set "&epath.cdm_v51/PROVIDER";
	    run;

	%end;

      data pcordata.&ENCTBL %if &dsview eq y %then %do;/ view=pcordata.&ENCTBL %end;
                            %else %do; (compress=yes) %end;;
        set %do yr = &startyr %to &endyr; "&epath.cdm_v51/&ENCTBL&yr." %end;;
	  run;
	
	  data pcordata.&DIATBL %if &dsview eq y %then %do;/ view=pcordata.&DIATBL %end;
                            %else %do; (compress=yes) %end;;
        set %do yr = &startyr %to &endyr; "&epath.cdm_v51/&DIATBL&yr." %end;;
	  run;

	  data pcordata.&PROCTBL %if &dsview eq y %then %do;/ view=pcordata.&PROCTBL %end;
                             %else %do; (compress=yes) %end;;
        set %do yr = &startyr %to &endyr; "&epath.cdm_v51/&PROCTBL&yr." %end;;
	  run;
    
     %if %length(&PDETBL) > 0 %then %do;
	  data pcordata.&DISTBL %if &dsview eq y %then %do;/ view=pcordata.&DISTBL %end;
                            %else %do; (compress=yes) %end;;
        set %do yr = &pdstartyr %to &pdendyr; "&epath.cdm_v51/&DISTBL&yr." %end;;
	  run;
    %end;
  %end;
  
    %create_cdm_tbl;

	ods listing close;
    ods pdf file="&epath.outfolder/create_cdm_v51_tablesviews.pdf" style = PCORNET_CDMTL;	

      title1 "Final Data Transformation Review";
	
	  title2 'Encounter Table';
	  title3 'Enc_type and (admitting_source or discharge_disposition or discharge_status or drg_type) crosstab';
      %stats(pcordata.encounter, ENC_TYPE*(ADMITTING_SOURCE DISCHARGE_DISPOSITION DISCHARGE_STATUS DRG_TYPE));

      data encounter(drop=ADMIT_DATE);
       set pcordata.encounter (keep=Enc_Type ADMIT_DATE DISCHARGE_DATE);
	   Encounter_Admission_year = year(ADMIT_DATE);
	   Encounter_Discharge_year = year(DISCHARGE_DATE);
      run; 

      proc sql;
        title3 'Enc_type and admit_date year crosstab';
	    select Enc_Type, Encounter_Admission_year, count('x') format=12.0 as Record_n 
        from encounter
        group by Enc_Type, Encounter_Admission_year;

	    title3 'Enc_type and discharge_date year crosstab';
	    select Enc_Type, Encounter_Discharge_year, count('x') format=12.0 as Record_n 
        from encounter
        group by Enc_Type, Encounter_Discharge_year;
      quit;

	  
     proc freq data=pcordata.diagnosis;
       title2 'Diagnosis Table';
	   title3 'Frequencies for dx_type, dx_source and pdx';
       table dx_type dx_source pdx/ sparse;    
     run;

     title2 'Procedures Table - px_type and enc_type crosstab';
     title3 h=1;
     %stats(pcordata.procedures, PX_TYPE*ENC_TYPE);

  data run_time_tracking;
     set run_time_tracking;
	 if programs="PCORNET CDM v51 ETL" then do;
        end_time=datetime();
        processing_time=end_time-start_time;
		data_year = "&startyr. - &endyr.";
     end;
  run;

  proc sort data = run_time_tracking; by start_time programs; run; 

  proc print data=run_time_tracking;
   title2 'ETL programs execution times';
  run;

  ods pdf close;
  ods listing;

%mend;

%create_cdm_v51_tablesviews;
