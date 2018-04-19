/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_ip.sas                          
*         Date:  12/15/2017                                                
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to transform yearly Medicare  
*           Inpatient claims data to annual CDM Encounter, Diagnosis and Procedures data
*
*  Inputs:   
*           1) Inpatient claim table
*              Inpatient revenue table        
*
*           2) SAS programs:
*               /etl/infolder/build_dx_px.sas
*                             
*  Output:
*           1) Annual CDM Encounter, Diagnosis and Procedures data in /etl/cdm_v31 
*           2) SAS log files in /etl/outfolder
*           3) SAS output files per year in PDF format stored in /etl/outfolder
*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;
%macro assignvars_ip; 
  PATID = BENE_ID; 
  ADMIT_DATE = ADMSN_DT; 
  DISCHARGE_DATE = DSCHRGDT;
  DRG = DRG_CD;
  DRG_TYPE = '02';
%mend;

%macro make_mapping_encounter;
  SELECT (compress(SRC_ADMS)); 
    WHEN ('2','E') ADMITTING_SOURCE  = 'AV';
    WHEN ('7') ADMITTING_SOURCE  = 'ED'; 
    WHEN ('B','C') ADMITTING_SOURCE  = 'HH'; 
    WHEN ('1') ADMITTING_SOURCE  = 'HO';
    WHEN ('F') ADMITTING_SOURCE  = 'HS'; 
    WHEN ('4','D') ADMITTING_SOURCE  = 'IP'; 
    WHEN ('3','6','8','A') ADMITTING_SOURCE  = 'OT';
    WHEN ('5') ADMITTING_SOURCE  = 'SN'; 
    WHEN ('0','9') ADMITTING_SOURCE  = 'UN'; 
    WHEN ('') ADMITTING_SOURCE   = 'NI'; 
  END;

  SELECT (compress(PTNTSTUS)); 
    WHEN ('A') DISCHARGE_DISPOSITION  = 'A'; 
    WHEN ('B') DISCHARGE_DISPOSITION  = 'E'; 
    WHEN ('C') DISCHARGE_DISPOSITION  = 'OT';
    WHEN ('') DISCHARGE_DISPOSITION  = 'NI';
  END; 

  SELECT (compress(STUS_CD)); 
    WHEN ('07') DISCHARGE_STATUS  = 'AM'; 
    WHEN ('20','40','41','42') DISCHARGE_STATUS  = 'EX'; 
    WHEN ('06','86') DISCHARGE_STATUS  = 'HH';   
    WHEN ('01','81') DISCHARGE_STATUS  = 'HO';   
    WHEN ('50','51') DISCHARGE_STATUS  = 'HS'; 
    WHEN ('02','05','43','65','66','82','85','88','93','94') DISCHARGE_STATUS  = 'IP'; 
    WHEN ('04','64','84','92') DISCHARGE_STATUS  = 'NH'; 
    WHEN ('08','21','63','69','70','71','72','87','91','95') DISCHARGE_STATUS  = 'OT'; 
    WHEN ('62','90') DISCHARGE_STATUS  = 'RH'; 
    WHEN ('09','30') DISCHARGE_STATUS  = 'SH'; 
    WHEN ('03','61','83','89') DISCHARGE_STATUS  = 'SN'; 
    WHEN ('0') DISCHARGE_STATUS  = 'UN';
	WHEN ('') DISCHARGE_STATUS  = 'NI';
  END; 
%mend;

%macro ETL_IP;

  /*- Redirect listing of control flow info to its own log file -*/ ;
  proc printto new log="&epath/outfolder/etl_ip.log" ;
  run ;

  %process_begin(ETL_IP);
  /*- transform encounter table -*/;
  data ip_raw_&yr;
    set lib_ip.&IPTBL&yr(keep=&IP_KEEP %if &nametype eq L %then %do; rename=(&IP_RN) %end;);
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

  proc sort data = ip_raw_&yr; by PROVIDER; run; 
  
  data ip_raw_&yr;
    merge ip_raw_&yr(in=a) facility(in=b);
    by PROVIDER;

	if a then output; 
  run;
  
  proc sort data = ip_raw_&yr; by AT_NPI; run; 

  data ip_raw_&yr;
    %add_newvars_encounter;;
    merge ip_raw_&yr(in=a) provider(in=b);
    by AT_NPI;

	if a then do;
  
    /*- To assign variable names from source to target -*/;
    %assignvars_ip;

    %make_mapping_encounter;

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
  proc sql;
    create table ip_rev_&yr as   
      select distinct a.ENCOUNTERID, 
			 b.REV_CNTR
	  from ip_raw_&yr a, lib_ip.&IPREVTBL&yr(keep=&IPREV_KEEP %if &nametype eq L %then %do; rename=(&IPREV_RN) %end;) b
	  where a.CLM_ID = b.CLM_ID
      and b.REV_CNTR ne '0001';
  quit;

  data ip_raw_&yr;  
     if _N_ eq 1 then do;
	   declare hash h(hashexp:16, dataset: "ip_rev_&yr(where=(REV_CNTR in ('0450','0451','0452','0459','0981')))");
       h.defineKey('ENCOUNTERID');
       h.defineDone();
	   declare hash h2(dataset: "ip_rev_&yr(where=(REV_CNTR ='0762'))");
       h2.defineKey('ENCOUNTERID');
       h2.defineDone();
     end;
     set ip_raw_&yr;
     ENC_TYPE = ifc((h.find() = 0), 'EI', ifc(h2.find() = 0, 'OS', 'IP')); 
  run;

  /*- Diagnosis table transform -*/;
  proc sort data = ip_raw_&yr out=pcordata.ip_dx_h&yr(keep=&dx_keep CLM_POA_IND_SW:); by ENCOUNTERID; run;  

  %build_diagnosis(ip_dx_h, ip, 25);
  
  /*- Procedure table transform -*/;
  data pcordata.ip_px_h&yr;
    set ip_raw_&yr(keep=&px_keep);
  run;

  %build_procedure(ip_px_h,ip,25);

  proc sql;
     create table pcordata.ip_px_rev&yr as   
       select distinct a.CLM_ID,	        
              a.PATID,             
              a.ENCOUNTERID, 
              a.ENC_TYPE,
              a.ADMIT_DATE,  
              a.PROVIDERID,
			  a.THRU_DT,
			  b.REV_CNTR
		from ip_raw_&yr a, ip_rev_&yr b
		where a.ENCOUNTERID = b.ENCOUNTERID;
  quit;

  %build_procedure_revhcpcs(ip_px_rev, ip);

  /*- clean date, and append data to the target table -*/;
  proc sql;
    create table pcordata.encounter_ip&yr as
      select distinct &encounter_order
      from ip_raw_&yr a,
            (select PATID from pcordata.demographic) b
      where a.PATID = b.PATID;   
  quit;

  %clean_labels(pcordata, encounter_ip&yr );

  proc append base = pcordata.procedure_ip&yr data=pcordata.procedurerev_ip&yr; run;

  ods listing close;
  ods pdf file="&epath/outfolder/etl_ip_&yr..pdf" style = PCORNET_CDMTL;	

  title1 "&yr Inpatient Data Transformation";
  proc print data=pcordata.encounter_ip&yr (obs=5);
    title2 "Encounter Sample Listing - 5 Rows";
  run;

  title2 "&srcadm (input) to Admitting_source (output) mapping crosstab";
  %stats(ip_raw_&yr %if &nametype eq L %then %do;(rename=(src_adms=&srcadm)) %end;, &srcadm*ADMITTING_SOURCE);
  title2 "&pstus (input) to Discharge_disposition (output) mapping crosstab";
  %stats(ip_raw_&yr %if &nametype eq L %then %do;(rename=(ptntstus=&pstus)) %end;, &pstus*DISCHARGE_DISPOSITION);
  title2 "&scd (input) to Discharge_status (output) mapping crosstab";
  %stats(ip_raw_&yr %if &nametype eq L %then %do;(rename=(stus_cd=&scd)) %end;, &scd*DISCHARGE_STATUS);
  title2 "Encounter Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.encounter_ip&yr);
  
  proc print data=pcordata.diagnosis_ip&yr (obs=5);
    title2 "Diagnosis Sample Listing - 5 Rows";
  run;

  title2 'Diagnosis Table - dx_type and enc_type crosstab';
  %stats(pcordata.diagnosis_ip&yr, DX_TYPE*ENC_TYPE);
  title2 "Diagnosis Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.diagnosis_ip&yr);

  proc print data=pcordata.procedure_ip&yr (obs=5);
    title2 "Procedure Sample Listing - 5 Rows";
  run;

  title2 'Procedures - px_type and enc_type crosstab';
  %stats(pcordata.procedure_ip&yr, PX_TYPE*ENC_TYPE);
  title2 "Procedure Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.procedure_ip&yr);

  ods pdf close;
  ods listing;

  proc sort data = pcordata.encounter_ip&yr; by encounterid descending DISCHARGE_DATE; run;
  proc sort data = pcordata.encounter_ip&yr nodupkey; by encounterid; run;

  %if %sysfunc(exist(pcordata.&ENCTBL&yr)) %then %do; 
    proc append base = pcordata.&ENCTBL&yr data=pcordata.encounter_ip&yr; run;
  %end;
  %else %do;
    data pcordata.&ENCTBL&yr; set pcordata.encounter_ip&yr; run;
  %end;

  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_DIAGNOSIS_DATE = input("&sysdate", date9.); 
	REFRESH_PROCEDURES_DATE = input("&sysdate", date9.);   
	REFRESH_ENCOUNTER_DATE = input("&sysdate", date9.);
  run;

  %process_end;
  %clean(facility provider provider_prfnpi);

  proc datasets library=pcordata nolist;
    delete encounter_ip&yr diagnosis_ip&yr procedure_ip&yr procedurerev_ip&yr;	
  quit;

  /* Restoring the default destination */;
  proc printto log=log;
  run;  
%mend ETL_IP;

%ETL_IP;



