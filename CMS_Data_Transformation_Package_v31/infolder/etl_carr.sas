/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_carr.sas                          
*  Create Date:  12/15/2017 
*     Modified:  08/20/2018 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to transform yearly Medicare  
*           Carrier claims data to annual CDM Encounter, Diagnosis and Procedures tables
*
*  Inputs:   
*           1) Carrier claim table 
*              Carrier line table          
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

%macro assignvars_carr;
  PATID = BENE_ID; 
  ADMIT_DATE = FROM_DT; 
  DISCHARGE_DATE = ifn(ENC_TYPE='AV' or ENC_TYPE='OA', ., THRU_DT);
  FACILITY_LOCATION = substr(PROVZIP, 1, 3);
  RAW_ENC_TYPE = PLCSRVC; 

%mend;

%macro make_mapping_encounter_carr;

 SELECT (compress(PLCSRVC)); 
   WHEN ('11') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'PRIVATE_PHYSICIANS_GROUP_OFFICE'; END; 
   WHEN ('21') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'HOSPITAL_COMMUNITY'; END; 
   WHEN ('19', '22') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'HOSPITAL_BASED_OUTPATIENT_CLINIC_OR_DEPARTMENT_OTHER'; END; 
   WHEN ('23') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'EMERGENCY_DEPARTMENT_HOSPITAL'; END; 
   WHEN ('41') DO; ENC_TYPE  = 'OT'; FACILITY_TYPE  = 'AMBULANCE_BASED_CARE'; END; 
   WHEN ('24') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'AMBULATORY_SURGERY_CENTER'; END;  
   WHEN ('65') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'DIALYSIS_UNIT_HOSPITAL'; END; 
   WHEN ('25') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'FREE_STANDING_BIRTHING_CENTER'; END; 
   WHEN ('53') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'FREE_STANDING_MENTAL_HEALTH_CENTER'; END; 
   WHEN ('81') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'FREE_STANDING_LABORATORY_FACILITY'; END; 
   WHEN ('42') DO; ENC_TYPE  = 'OT'; FACILITY_TYPE  = 'HELICOPTER_BASED_CARE'; END; 
   WHEN ('34') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'HOSPICE_FACILITY'; END; 
   WHEN ('62') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'HOSPITAL_OUTPATIENT_REHABILITATION_CLINIC'; END; 
   WHEN ('09') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'HOSPITAL_PRISON'; END;  
   WHEN ('51', '52') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'HOSPITAL_PSYCHIATRIC'; END; 
   WHEN ('61') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'HOSPITAL_REHABILITATION'; END; 
   WHEN ('49') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'INDEPENDENT_AMBULATORY_CARE_PROVIDER_SITE_OTHER'; END; 
   WHEN ('50', '71') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'LOCAL_COMMUNITY_HEALTH_CENTER'; END; 
   WHEN ('01', '02', '15') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'OT'; END;
   WHEN ('04', '06', '08', '16', '26', '33', '54', '56') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'OT'; END;
   WHEN ('05', '07') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'OT'; END; 
   WHEN ('18', '99') DO; ENC_TYPE  = 'OT'; FACILITY_TYPE  = 'OT'; END;  
   WHEN ('12') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'PRIVATE_RESIDENTIAL_HOME'; END;  
   WHEN ('13', '14', '32') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'RESIDENTIAL_INSTITUTION'; END; 
   WHEN ('03') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'RESIDENTIAL_SCHOOL_INFIRMARY'; END;  
   WHEN ('72') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'RURAL_HEALTH_CENTER'; END;  
   WHEN ('31') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'SKILLED_NURSING_FACILITY'; END; 
   WHEN ('55') DO; ENC_TYPE  = 'IC'; FACILITY_TYPE  = 'SUBSTANCE_ABUSE_TREATMENT_CENTER'; END; 
   WHEN ('57') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'SUBSTANCE_ABUSE_TREATMENT_CENTER'; END; 
   WHEN ('60') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'VACCINATION_CLINIC'; END; 
   WHEN ('17') DO; ENC_TYPE  = 'OA'; FACILITY_TYPE  = 'WALK_IN_CLINIC'; END; 
   WHEN ('20') DO; ENC_TYPE  = 'AV'; FACILITY_TYPE  = 'WALK_IN_CLINIC'; END; 
   WHEN ('') DO; ENC_TYPE  = 'NI'; FACILITY_TYPE  = 'NI'; END;
   OTHERWISE DO; ENC_TYPE  = 'UN'; FACILITY_TYPE  = 'UN'; END;
  END;

%mend;

%macro ETL_CARR;

  /*- Redirect listing of control flow info to its own log file -*/ ;
  proc printto new log="&epath/outfolder/etl_carr.log" ;
  run ;
  %process_begin(ETL_CARR);

  /*- transform encounter table -*/;
  %find_fname(flib=lib_carr, memnm=&CARTBL);

  data data carr_raw_&yr;
    set lib_carr.&tbnm(keep=&CARR_KEEP %if &nametype eq L %then %do; rename=(&CARR_RN) %end;);
  run;

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
    create table carr_line&yr as
	  %do i=1 %to &clmax;
	   select distinct CLM_ID,	         
                strip(PRF_NPI) as AT_NPI length=10,
                PROVZIP,  
                PLCSRVC,
                LINE_ICD_DGNS_CD,
                EXPNSDT1,  
                HCPCS_CD,
                LPRPAYCD
	   from lib_carr.&&cl&i (keep=&CLINE_KEEP %if &nametype eq L %then %do; rename=(&CLINE_RN) %end;)
	   %if %eval(&i < &clmax) %then %do; union %end;
      %end;
       order by clm_id;
  quit;

  proc sort data = carr_raw_&yr; by clm_id; run; 

  data carr_raw_&yr;
    merge carr_raw_&yr(in=a) carr_line&yr(in=b);        
    by clm_id;
	if a;
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

  proc sort data = carr_raw_&yr; by AT_NPI; run; 
  
  data carr_raw_&yr;
     %add_newvars_encounter;
     merge carr_raw_&yr(in=a) provider(in=b);
     by AT_NPI;

	 if a then do;
  
      /*- To assign variable names from source to target -*/;
	  %make_mapping_encounter_carr;
      %assignvars_carr;
	  %mapping_enc_share(LPRPAYCD);

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

  /*- Diagnosis table transform -*/;
  proc sort data = carr_raw_&yr out=pcordata.carr_dx_h&yr(keep=&dx_keep LINE_ICD_DGNS_CD ICD_DGNS_VRSN_CD:); by ENCOUNTERID; run;  

  %build_diagnosis(carr_dx_h, carr, 12);
  
  /*- Procedure table transform -*/;

  proc sql;
    create table pcordata.carr_px_rev&yr as   
      select distinct a.CLM_ID,	        
             a.PATID,             
             a.ENCOUNTERID, 
             a.ENC_TYPE,
             a.ADMIT_DATE,  
             a.PROVIDERID,
			 a.THRU_DT,
			 b.HCPCS_CD,
			 b.EXPNSDT1
		from carr_raw_&yr a, carr_line&yr b
        where a.CLM_ID = b.CLM_ID;
  quit;


  %build_procedure_revhcpcs(carr_px_rev, carr);

  /*- clean date, and append data to the target table -*/;

  proc sql;
    create table pcordata.encounter_carr&yr as
      select distinct &encounter_order
      from carr_raw_&yr a, demographic b
      where a.PATID = b.PATID;   
  quit;

  %clean_labels(pcordata, encounter_carr&yr );
  
  ods listing close;
  ods pdf file="&epath/outfolder/etl_carr_&yr..pdf" style = PCORNET_CDMTL;	

  title1 "&yr Carrier Data Transformation";
  proc print data=pcordata.encounter_carr&yr (obs=5);
    title2 "Encounter Sample Listing - 5 Rows";
  run;

  title2 "&psrvc (input) to Enc_type (output) mapping crosstab";
  %stats(carr_raw_&yr %if &nametype eq L %then %do;(rename=(plcsrvc=&psrvc)) %end;, &psrvc*ENC_TYPE);
  title2 "Encounter Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.encounter_carr&yr);

  proc print data=pcordata.diagnosis_carr&yr (obs=5);
    title2 "Diagnosis Sample Listing - 5 Rows";
  run;

  title2 'Diagnosis Table - dx_type and enc_type crosstab';
  %stats(pcordata.diagnosis_carr&yr, DX_TYPE*ENC_TYPE);
  title2 "Diagnosis Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.diagnosis_carr&yr);

  proc print data=pcordata.procedurerev_carr&yr (obs=5);
    title2 "Sample Listing for Procedure with HCPCS/Revenue - 5 Rows";
  run;

  title2 'Procedures - px_type and enc_type crosstab';
  %stats(pcordata.procedurerev_carr&yr, PX_TYPE*ENC_TYPE);
  title2 "Procedure Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.procedurerev_carr&yr);

  ods pdf close;
  ods listing;

  proc sort data = pcordata.encounter_carr&yr; by encounterid descending DISCHARGE_DATE; run;
  proc sort data = pcordata.encounter_carr&yr nodupkey; by encounterid; run;

  %if %sysfunc(exist(pcordata.&ENCTBL&yr)) %then %do; 
    proc append base = pcordata.&ENCTBL&yr data=pcordata.encounter_carr&yr; run;
  %end;
  %else %do;
    data pcordata.&ENCTBL&yr; set pcordata.encounter_carr&yr; run;
  %end;

  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_DIAGNOSIS_DATE = input("&sysdate", date9.); 
	REFRESH_PROCEDURES_DATE = input("&sysdate", date9.);   
	REFRESH_ENCOUNTER_DATE = input("&sysdate", date9.);
  run;

  proc datasets library=pcordata nolist;
    delete encounter_carr&yr diagnosis_carr&yr procedurerev_carr&yr;		
  quit;

  /* Restoring the default destination */;
  proc printto log=log; run;  
  %process_end;
  %clean(facility provider demographic);

%mend ETL_CARR;

%ETL_carr;



