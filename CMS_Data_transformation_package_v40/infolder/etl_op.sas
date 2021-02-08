/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_op.sas                          
*  Create Date:  12/15/2017 
*     Modified:  08/20/2018  
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to transform yearly Medicare  
*           Outpatient claims data to annual CDM Encounter, Diagnosis and Procedures data
*
*  Inputs:   
*           1) Outpatient claim table
*              Outpatient revenue table        
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
%macro assignvars_op;
  PATID = BENE_ID; 
  ADMIT_DATE = FROM_DT; 
  DISCHARGE_DATE = .;
  FACILITY_LOCATION = ifc(length(CLM_SRVC_FAC_ZIP_CD)>5, substr(CLM_SRVC_FAC_ZIP_CD, 1,5), CLM_SRVC_FAC_ZIP_CD); 
%mend;

%macro make_mapping_encounter;
  SELECT (compress(FAC_TYPE)); 
    WHEN ('1','7','8') ENC_TYPE  = 'AV';   
    WHEN ('2','3') ENC_TYPE  = 'OA'; 
    WHEN ('') ENC_TYPE  = 'NI';
  END; 
%mend;

%macro ETL_OP;

  /*- Redirect listing of control flow info to its own log file -*/ ;
  proc printto new log="&epath/outfolder/etl_op.log" ;run;

  %process_begin(ETL_OP);
  /*- transform encounter table -*/;
  %find_fname(flib=lib_op, memnm=&OPTBL);
  data op_raw_&yr;
    set lib_op.&tbnm(keep=&OP_KEEP %if &nametype eq L %then %do; rename=(&OP_RN) %end;);
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

   proc sort data = op_raw_&yr; by PROVIDER; run; 
  
   data op_raw_&yr;
     merge op_raw_&yr(in=a) facility(in=b);
     by PROVIDER;

	 if a then output; 
   run;

   proc sort data = op_raw_&yr; by AT_NPI; run; 

   data op_raw_&yr;
     %add_newvars_encounter;;
     merge op_raw_&yr(in=a) provider(in=b);
     by AT_NPI;

	 if a then do;
  
       /*- To assign variable names from source to target -*/;
       %assignvars_op;

       %make_mapping_encounter;
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
  %find_fname(flib=lib_op, memnm=&OPREVTBL);
  proc sql;
    create table op_rev_&yr as   
      select distinct a.ENCOUNTERID,
	          a.CLM_ID,
              b.HCPCS_CD, 
			  b.REV_CNTR,
			  b.REV_DT
	  from op_raw_&yr a, lib_op.&tbnm(keep=&OPREV_KEEP %if &nametype eq L %then %do; rename=(&OPREV_RN) %end;) b
	  where a.CLM_ID = b.CLM_ID
      and b.REV_CNTR ne '0001';
   quit;

  data op_raw_&yr;  
    if _N_ eq 1 then do;
	  declare hash h(hashexp:16, dataset: "op_rev_&yr(where=(REV_CNTR in ('0450','0451','0452','0459','0981')))");
      h.defineKey('CLM_ID');
      h.defineDone();
	  declare hash h2(dataset: "op_rev_&yr(where=(REV_CNTR ='0762'))");
      h2.defineKey('ENCOUNTERID');
      h2.defineDone();
    end;
    set op_raw_&yr;
	FACILITY_TYPE = 'HOSPITAL_BASED_OUTPATIENT_CLINIC_OR_DEPARTMENT_OTHER';
    if h.find() = 0 then do; 
      ENC_TYPE = 'ED';
	  FACILITY_TYPE = 'EMERGENCY_DEPARTMENT_HOSPITAL';
	  DISCHARGE_DATE = THRU_DT;
    end;
    else if h2.find() = 0 then do;
      ENC_TYPE  = 'OS'; 
      DISCHARGE_DATE = THRU_DT;
    end;
  run;

  /*- Diagnosis table transform -*/;
  proc sort data = op_raw_&yr out=pcordata.op_dx_h&yr(keep=&dx_keep); by ENCOUNTERID; run;  

  %build_diagnosis(op_dx_h, op, 25);
  
  /*- Procedure table transform -*/;
 
  proc sql;
    create table pcordata.op_px_rev&yr as   
      select distinct a.CLM_ID,	        
             a.PATID,             
             a.ENCOUNTERID, 
             a.ENC_TYPE,
             a.ADMIT_DATE,  
             a.PROVIDERID,
			 a.THRU_DT,
			 b.HCPCS_CD,
			 b.REV_CNTR,
			 b.REV_DT
	  from op_raw_&yr a, op_rev_&yr b
	  where a.ENCOUNTERID = b.ENCOUNTERID;
  quit;

  %build_procedure_revhcpcs(op_px_rev, op);

  /*- clean date, and append data to the target table -*/;
  proc sql;
    create table pcordata.encounter_op&yr as
      select distinct &encounter_order
      from op_raw_&yr a,demographic b
      where a.PATID = b.PATID;   
  quit;

  %clean_labels(pcordata, encounter_op&yr );
 
  ods listing close;
  ods pdf file="&epath/outfolder/etl_op_&yr..pdf" style = PCORNET_CDMTL;	

  title1 "&yr Outpatient Data Transformation";
  proc print data=pcordata.encounter_op&yr (obs=5);
    title2 "Encounter Sample Listing - 5 Rows";
  run;

  title2 "&ftype (input) to Admitting_source (output) mapping crosstab";
  %stats(op_raw_&yr %if &nametype eq L %then %do;(rename=(fac_type=&ftype)) %end;, &ftype*Enc_type);
  title2 "Encounter Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.encounter_op&yr);

  proc print data=pcordata.diagnosis_op&yr (obs=5);
    title2 "Diagnosis Sample Listing - 5 Rows";
  run;

  title2 'Diagnosis Table - dx_type and enc_type crosstab';
  %stats(pcordata.diagnosis_op&yr, DX_TYPE*ENC_TYPE);
  title2 "Diagnosis Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.diagnosis_op&yr);
  title2 'Procedures - px_type and enc_type crosstab';
  %stats(pcordata.procedurerev_op&yr, PX_TYPE*ENC_TYPE);

  proc print data=pcordata.procedurerev_op&yr (obs=5);
    title2 "Sample Listing for Procedure with HCPCS/Revenue - 5 Rows";
  run;

  title2 "Procedure Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.procedurerev_op&yr);

  ods pdf close;
  ods listing;

  proc datasets library=work nolist;delete op: quit;

  proc sort data = pcordata.encounter_op&yr; by encounterid descending DISCHARGE_DATE; run;
  proc sort data = pcordata.encounter_op&yr nodupkey; by encounterid; run;

  %if %sysfunc(exist(pcordata.&ENCTBL&yr)) %then %do; 
    proc append base = pcordata.&ENCTBL&yr data=pcordata.encounter_op&yr; run;
  %end;
  %else %do;
    data pcordata.&ENCTBL&yr; set pcordata.encounter_op&yr; run;
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
    delete encounter_op&yr diagnosis_op&yr procedurerev_op&yr;		
  quit;

  /* Restoring the default destination */;
  proc printto log=log; run;  
%mend ETL_OP;

%ETL_OP;



