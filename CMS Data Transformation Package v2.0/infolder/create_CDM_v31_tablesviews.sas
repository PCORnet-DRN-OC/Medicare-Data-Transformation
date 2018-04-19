
%macro create_CDM_v31_tablesviews;

  %let dsview = %lowcase(&dsview);
  %let n = 1;
  %if &dsview eq b %then %do; %let n = 2; %end;
  
  %do i=1 %to &n;
    %if %eval(&i = 2) %then %do;  %let dsview = y; %end;
    %if &dsview eq y %then %do;
	  libname pcordata clear;
	  libname pcordata "&epath.cdm_v31/dsview";

      data pcordata.&DEMTBL / view=pcordata.&DEMTBL;
        set "&epath.cdm_v31/&DEMTBL";
	  run;

	  data pcordata.&DEATTBL / view=pcordata.&DEATTBL;
        set "&epath.cdm_v31/&DEATTBL";
	  run;

	  data pcordata.&ENRTBL / view=pcordata.&ENRTBL;
        set "&epath.cdm_v31/&ENRTBL";
	  run;

	  data pcordata.HARVEST / view=pcordata.HARVEST;
        set "&epath.cdm_v31/HARVEST";
	  run;
	%end;

    data pcordata.&ENCTBL %if &dsview eq y %then %do;/ view=pcordata.&ENCTBL %end;;
        set %do yr = &startyr %to &endyr; "&epath.cdm_v31/&ENCTBL&yr." %end;;
	run;

	data pcordata.&DIATBL %if &dsview eq y %then %do;/ view=pcordata.&DIATBL %end;;
        set %do yr = &startyr %to &endyr; "&epath.cdm_v31/&DIATBL&yr." %end;;
	run;

	data pcordata.&PROCTBL %if &dsview eq y %then %do;/ view=pcordata.&PROCTBL %end;;
        set %do yr = &startyr %to &endyr; "&epath.cdm_v31/&PROCTBL&yr." %end;;
	run;

	data pcordata.&DISTBL %if &dsview eq y %then %do;/ view=pcordata.&DISTBL %end;;
        set %do yr = &startyr %to &pdendyr; "&epath.cdm_v31/&DISTBL&yr." %end;;
	run;

  %end;

	ods listing close;
    ods pdf file="&epath.outfolder/create_CDM_v31_tablesviews.pdf" style = PCORNET_CDMTL;	

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

  proc sql;
   title2 'Checking counts for encounter, diagnosis, procedures and dispensing';
      select 'ENCOUNTER' as Table, 'All_N' as Statistic, count('x') format=12.0 as Record_n from pcordata.encounter
	  union
	  select 'ENCOUNTER' as Table, 'DISTINCT_N' as Statistic, count(distinct encounterid) format=12.0 as Record_n from pcordata.encounter
      union
	  select 'DIAGNOSIS' as Table, 'All_N' as Statistic, count('x') format=12.0 as Record_n from pcordata.diagnosis
	  union
	  select 'DIAGNOSIS' as Table, 'DISTINCT_N' as Statistic, count(distinct diagnosisid) format=12.0 as Record_n from pcordata.diagnosis
      union
	  select 'PROCEDURES' as Table, 'All_N' as Statistic, count('x') format=12.0 as Record_n from pcordata.procedures
	  union
	  select 'PROCEDURES' as Table, 'DISTINCT_N' as Statistic, count(distinct proceduresid) format=12.0 as Record_n from pcordata.procedures
      union
	  select 'DISPENSING' as Table, 'All_N' as Statistic, count('x') format=12.0 as Record_n from pcordata.dispensing
	  union
	  select 'DISPENSING' as Tablename, 'DISTINCT_N' as Statistic, count(distinct dispensingid) format=12.0 as Record_n from pcordata.dispensing;
  quit;

  data run_time_tracking;
     set run_time_tracking;
	 if programs="PCORNET CDM v31 ETL" then do;
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

%create_CDM_v31_tablesviews;
