/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_snf.sas                          
*         Date:  08/21/2017                                                
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to transform yearly Medicare  
*           Inpatient claims data to annual CDM Encounter, Diagnosis and Procedures data
*
*  Inputs:   
*           1) Snf claim table
*              Snf revenue table        
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
%macro assignvars_snf; 
  PATID = BENE_ID; 
  ADMIT_DATE = ADMSN_DT; 
  DISCHARGE_DATE = DSCHRGDT;
  DRG = DRG_CD;
  DRG_TYPE = '02';
  ENC_TYPE = 'IS';
  FACILITY_TYPE = 'SKILLED_NURSING_FACILITY';
  FACILITY_LOCATION = ifc(length(CLM_SRVC_FAC_ZIP_CD)>5, substr(CLM_SRVC_FAC_ZIP_CD, 1,5), CLM_SRVC_FAC_ZIP_CD);
%mend;

%macro ETL_SNF;

  /*- Redirect listing of control flow info to its own log file -*/ ;
  proc printto new log="&epath/outfolder/etl_snf.log" ;
  run ;

  %process_begin(ETL_SNF);
  /*- transform encounter table -*/;

  %find_fname(flib=lib_snf, memnm=&SNFTBL);

  data snf_raw_&yr;
    set lib_snf.&tbnm(keep=&SNF_KEEP %if &nametype eq L %then %do; rename=(&SNF_RN) %end;);
	/*AT_NPI = left(AT_NPI);*/
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

  proc sort data = snf_raw_&yr; by PROVIDER; run; 
  
  data snf_raw_&yr;
    merge snf_raw_&yr(in=a) facility(in=b);
    by PROVIDER;

	if a then output; 
  run;
  
  proc sort data = snf_raw_&yr; by AT_NPI; run; 

  data snf_raw_&yr;
    %add_newvars_encounter;;
    merge snf_raw_&yr(in=a) provider(in=b);
    by AT_NPI;

	if a then do;
  
    /*- To assign variable names from source to target -*/;
    %assignvars_snf;

    %make_mapping_stus_ipsnfmp(SRC_ADMS, PTNTSTUS, STUS_CD);
	%mapping_enc_share(PRPAY_CD);

     %if %eval(&ecnt > 0) %then %do;
       ENCOUNTERIDn = _n_ + input(&eoffset,12.);
     %end;
     %else %do;
       ENCOUNTERIDn = _n_;
     %end;

	  ENCOUNTERID = put(ENCOUNTERIDn,z12.);
       output;
	  end;
  run;

  /*- Retrive revenue table -*/;
  %find_fname(flib=lib_snf, memnm=&SNFREVTBL);
  proc sql;
    create table snf_rev_&yr as   
      select distinct a.ENCOUNTERID, 
			 b.REV_CNTR
	  from snf_raw_&yr a, lib_snf.&tbnm(keep=&IPREV_KEEP %if &nametype eq L %then %do; rename=(&IPREV_RN) %end;) b
	  where a.CLM_ID = b.CLM_ID
      and b.REV_CNTR ne '0001';
  quit;
  
  /*- Diagnosis table transform -*/;
  proc sort data = snf_raw_&yr out=pcordata.snf_dx_h&yr(keep=&dx_keep); by ENCOUNTERID; run;  

  %build_diagnosis(snf_dx_h, snf, 25);
  
  /*- Procedure table transform -*/;
  data pcordata.snf_px_h&yr;
    set snf_raw_&yr(keep=&px_keep);
  run;

  %build_procedure(snf_px_h,snf,25);

  proc sql;
     create table pcordata.snf_px_rev&yr as   
       select distinct a.CLM_ID,	        
              a.PATID,             
              a.ENCOUNTERID, 
              a.ENC_TYPE,
              a.ADMIT_DATE,  
              a.PROVIDERID,
			  a.THRU_DT,
			  b.REV_CNTR
		from snf_raw_&yr a, snf_rev_&yr b
		where a.ENCOUNTERID = b.ENCOUNTERID;
  quit;

  %build_procedure_revhcpcs(snf_px_rev, snf);

  /*- clean date, and append data to the target table -*/;
  proc sql;
    create table pcordata.encounter_snf&yr as
      select distinct &encounter_order
      from snf_raw_&yr a, demographic b
      where a.PATID = b.PATID;   
  quit;

  %clean_labels(pcordata, encounter_snf&yr );

  proc append base = pcordata.procedure_snf&yr data=pcordata.procedurerev_snf&yr; run;

  ods listing close;
  ods pdf file="&epath/outfolder/etl_snf_&yr..pdf" style = PCORNET_CDMTL;	

  title1 "&yr SNF Data Transformation";
  proc print data=pcordata.encounter_snf&yr (obs=5);
    title2 "Encounter Sample Listing - 5 Rows";
  run;

  title2 "&srcadm (input) to Admitting_source (output) mapping crosstab";
  %stats(snf_raw_&yr %if &nametype eq L %then %do;(rename=(src_adms=&srcadm)) %end;, &srcadm*ADMITTING_SOURCE);
  title2 "&pstus (input) to Discharge_disposition (output) mapping crosstab";
  %stats(snf_raw_&yr %if &nametype eq L %then %do;(rename=(ptntstus=&pstus)) %end;, &pstus*DISCHARGE_DISPOSITION);
  title2 "&scd (input) to Discharge_status (output) mapping crosstab";
  %stats(snf_raw_&yr %if &nametype eq L %then %do;(rename=(stus_cd=&scd)) %end;, &scd*DISCHARGE_STATUS);
  title2 "Encounter Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.encounter_snf&yr);
  
  proc print data=pcordata.diagnosis_snf&yr (obs=5);
    title2 "Diagnosis Sample Listing - 5 Rows";
  run;

  title2 'Diagnosis Table - dx_type and enc_type crosstab';
  %stats(pcordata.diagnosis_snf&yr, DX_TYPE*ENC_TYPE);
  title2 "Diagnosis Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.diagnosis_snf&yr);

  proc print data=pcordata.procedure_snf&yr (obs=5);
    title2 "Procedure Sample Listing - 5 Rows";
  run;

  title2 'Procedures - px_type and enc_type crosstab';
  %stats(pcordata.procedure_snf&yr, PX_TYPE*ENC_TYPE);
  title2 "Procedure Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.procedure_snf&yr);

  ods pdf close;
  ods listing;

  proc sort data = pcordata.encounter_snf&yr; by encounterid descending DISCHARGE_DATE; run;
  proc sort data = pcordata.encounter_snf&yr nodupkey; by encounterid; run;

  %if %sysfunc(exist(pcordata.&ENCTBL&yr)) %then %do; 
    proc append base = pcordata.&ENCTBL&yr data=pcordata.encounter_snf&yr; run;
  %end;
  %else %do;
    data pcordata.&ENCTBL&yr; set pcordata.encounter_snf&yr; run;
  %end;

  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_DIAGNOSIS_DATE = input("&sysdate9", date9.); 
	REFRESH_PROCEDURES_DATE = input("&sysdate9", date9.);   
	REFRESH_ENCOUNTER_DATE = input("&sysdate9", date9.);
  run;

  %process_end;
  %clean(facility provider demographic);

  proc datasets library=pcordata nolist;
    delete encounter_snf&yr diagnosis_snf&yr procedure_snf&yr procedurerev_snf&yr;	
  quit;

  /* Restoring the default destination */;
  proc printto log=log;
  run;  
%mend ETL_SNF;

%ETL_SNF;



