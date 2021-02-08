/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_ip.sas                          
*  Create Date:  12/15/2017 
*     Modified:  07/03/2020  
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to transform yearly Medicare  
*           Medpar data to annual CDM Encounter, Diagnosis and Procedures data
*
*  Inputs:   
*           1) Medpar claim table      
*
*           2) SAS programs:
*               /etl/infolder/build_dx_px.sas
*                             
*  Output:
*           1) Annual CDM Encounter, Diagnosis and Procedures data in /etl/cdm_v41 
*           2) SAS log files in /etl/outfolder
*           3) SAS output files per year in PDF format stored in /etl/outfolder
*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;
%macro assignvars_mp; 
  PATID = BENE_ID; 
  ADMIT_DATE = ADMSNDT; 
  DISCHARGE_DATE = DSCHRGDT;
  DRG = DRG_CD;
  DRG_TYPE = '02';
  FACILITY_LOCATION = ifc(length(CLM_SRVC_FAC_ZIP_CD)>5, substr(CLM_SRVC_FAC_ZIP_CD, 1,5), CLM_SRVC_FAC_ZIP_CD);
  SELECT (SSLSSNF); 
    WHEN ('S','L') DO; ENC_TYPE  = 'IP'; FACILITY_TYPE = 'HOSPITAL_COMMUNITY'; END; 
    WHEN ('N')     DO; ENC_TYPE  = 'IS'; FACILITY_TYPE = 'SKILLED_NURSING_FACILITY'; END; 
  END; 
  
%mend;

%macro ETL_MEDPAR;

  /*- Redirect listing of control flow info to its own log file -*/ ;
  proc printto new log="&epath/outfolder/etl_medpar.log";
  run;

  %process_begin(ETL_MEDPAR);
  /*- transform encounter table -*/; 
  %find_fname(flib=lib_mp, memnm=&MPTBL);

  data mp_raw_&yr(rename=(PRVDRNUM=PROVIDER));
    set lib_mp.&tbnm(keep=&MP_KEEP %if &nametype eq L %then %do; rename=(&MP_RN) %end;);
  run;

  proc sql;
    %if %sysfunc(exist(pcordata.&ENCTBL&yr)) > 0 %then %do; 
	  select count(*) format=12.0, 
             max(ENCOUNTERID)  
      into :ecnt, :epremax 
      from pcordata.&ENCTBL&yr;  

	  %let eoffset = &epremax;
	%end;
	%else %if %sysfunc(exist(pcordata.&ENCTBL&prev_yr)) = 0 %then %do; 
	  %let ecnt = 0;
	%end;
	%else %do;
	  select count(*) format=12.0, 
             max(ENCOUNTERID)  
      into :ecnt, :epremax 
      from pcordata.&ENCTBL&prev_yr;  

	  %let eoffset = &epremax;
	%end;
  quit;

  proc sort data = mp_raw_&yr; by PROVIDER; run; 
  
  data mp_raw_&yr;
    merge mp_raw_&yr(in=a) facility(in=b);
    by PROVIDER;

	if a then output; 
  run;

  data mp_raw_&yr;
    %add_newvars_encounter;;
    set mp_raw_&yr;
  
    /*- To assign variable names from source to target -*/;
    %assignvars_mp;

    %make_mapping_stus_ipsnfmp(SRC_ADMS, DSCHRGCD, DSTNTNCD);
	%mapping_enc_share(PRPAY_CD);

     %if %eval(&ecnt > 0) %then %do;
       ENCOUNTERIDn = _n_ + input(&eoffset,12.);
     %end;
     %else %do;
       ENCOUNTERIDn = _n_;
     %end;

	  ENCOUNTERID = put(ENCOUNTERIDn,z12.);
 
  run;

  /*- Diagnosis table transform -*/;
  proc sort data = mp_raw_&yr out=pcordata.mp_dx_h&yr(keep=&dx_mp_keep); by ENCOUNTERID; run;  

  %build_diagnosis(inds=mp_dx_h, ds=mp, n=25);
  
  /*- Procedure table transform -*/;
  data pcordata.mp_px_h&yr;
    set mp_raw_&yr(keep=&px_mp_keep);
  run;
  %build_procedure(inds=mp_px_h, ds=mp, n=25);
  
  /*- clean date, and append data to the target table -*/;
  proc sql;
    create table pcordata.encounter_mp&yr as
      select distinct &encounter_order
      from mp_raw_&yr a,demographic b
      where a.PATID = b.PATID;   
  quit;

  %clean_labels(pcordata, encounter_mp&yr );

  ods listing close;
  ods pdf file="&epath/outfolder/etl_mp_&yr..pdf" style = PCORNET_CDMTL;	

  title1 "&yr Medpar Data Transformation";
  proc print data=pcordata.encounter_mp&yr (obs=5);
    title2 "Encounter Sample Listing - 5 Rows";
  run;

  title2 "&srcadmp (input) to Admitting_source (output) mapping crosstab";
  %stats(mp_raw_&yr %if &nametype eq L %then %do;(rename=(src_adms=&srcadmp)) %end;, &srcadmp.*ADMITTING_SOURCE);
  title2 "&pstusmp (input) to Discharge_disposition (output) mapping crosstab";
  %stats(mp_raw_&yr %if &nametype eq L %then %do;(rename=(dschrgcd=&pstusmp)) %end;, &pstusmp.*DISCHARGE_DISPOSITION);
  title2 "&scdmp (input) to Discharge_status (output) mapping crosstab";
  %stats(mp_raw_&yr %if &nametype eq L %then %do;(rename=(dstntncd=&scdmp)) %end;, &scdmp.*DISCHARGE_STATUS);
  title2 "Encounter Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.encounter_mp&yr);
  
  proc print data=pcordata.diagnosis_mp&yr (obs=5);
    title2 "Diagnosis Sample Listing - 5 Rows";
  run;

  title2 'Diagnosis Table - dx_type and enc_type crosstab';
  %stats(pcordata.diagnosis_mp&yr, DX_TYPE*ENC_TYPE);
  title2 "Diagnosis Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.diagnosis_mp&yr);

  proc print data=pcordata.procedure_mp&yr (obs=5);
    title2 "Procedure Sample Listing - 5 Rows";
  run;

  title2 'Procedures - px_type and enc_type crosstab';
  %stats(pcordata.procedure_mp&yr, PX_TYPE*ENC_TYPE);
  title2 "Procedure Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.procedure_mp&yr);

  ods pdf close;
  ods listing;

  proc sort data = pcordata.encounter_mp&yr; by encounterid descending DISCHARGE_DATE; run;
  proc sort data = pcordata.encounter_mp&yr nodupkey; by encounterid; run;

  %if %sysfunc(exist(pcordata.&ENCTBL&yr)) %then %do; 
    proc append base = pcordata.&ENCTBL&yr data=pcordata.encounter_mp&yr; run;
  %end;
  %else %do;
    data pcordata.&ENCTBL&yr; set pcordata.encounter_mp&yr; run;
  %end;

  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_DIAGNOSIS_DATE = input("&sysdate", date9.); 
	REFRESH_PROCEDURES_DATE = input("&sysdate", date9.);   
	REFRESH_ENCOUNTER_DATE = input("&sysdate", date9.);
  run;

  %process_end;
  %clean(facility provider demographic);

  proc datasets library=pcordata nolist;
    delete encounter_mp&yr diagnosis_mp&yr procedure_mp&yr;	
  quit;

  /* Restoring the default destination */;
  proc printto log=log;
  run;  
%mend ETL_MEDPAR;

%ETL_MEDPAR;



