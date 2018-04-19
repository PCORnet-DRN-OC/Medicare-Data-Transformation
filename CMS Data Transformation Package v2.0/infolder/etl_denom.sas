/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_denom.sas                          
*         Date:  12/12/2017                                               
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to transform Medicare  
*           MBSF data to CDM Demographic and Death tables
*
*  Inputs:  
*           MBSF_AB/MBSF_ABD  
*                             
*  Output:  
*           1) Annual CDM Demographic and Death tables at /etl/cdm_v31 
*           2) SAS log files in /etl/outfolder
*           3) SAS output files per year in PDF format stored in /etl/outfolder
*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;

%macro make_mapping_denom;
  
  SELECT (compress(RACE_CMS)); 
    WHEN ('0') HISPANIC  = 'UN'; 
    WHEN ('5') HISPANIC  = 'Y'; 
	WHEN ('')  HISPANIC  = 'NI'; 
    OTHERWISE  HISPANIC  = 'OT'; 
  END;

  SELECT (compress(RACE_CMS)); 
    WHEN ('6') RACE  = '01'; 
    WHEN ('4') RACE  = '02'; 
    WHEN ('2') RACE  = '03'; 
    WHEN ('1') RACE  = '05'; 
    WHEN ('3') RACE  = 'OT';
    WHEN ('5') RACE  = 'OT';  
    WHEN ('0') RACE  = 'UN'; 
	WHEN ('')  RACE  = 'NI';
  END;

  SELECT (compress(SEX)); 
    WHEN ('2') SEX  = 'F'; 
    WHEN ('1') SEX  = 'M';  
    WHEN ('0') SEX  = 'UN';
	WHEN ('')  SEX  = 'NI';
  END;  

%mend;

%macro make_mapping_death;

  SELECT (compress(V_DOD_SW)); 
    WHEN ('') DEATH_DATE_IMPUTE  = 'D';
    WHEN ('V') DEATH_DATE_IMPUTE  = 'N';
  END;

%mend;
    
%macro ETL_DENOM;

  /* Redirect listing of control flow info to its own log file */ ;
  proc printto new log="&epath/outfolder/etl_denom.log" ;
  run ;

  %do yr = &endyr %to &startyr %by -1;
    %process_begin(ETL_DENOM);

    /* transform demographic table */;
    data pcordata.denom_raw&yr(rename=(bene_id=patid race=RACE_CMS));
      set lib_den.&MBSFTBL.&yr(keep=&DENOM_KEEP. %if &nametype eq L %then %do; rename=(&rn_list) %end;);
    run;

    %if &yr = &endyr %then %do; %let cnt = 0; %end;
    %else %do;
      proc sql;
        select count(*) format=12.0 into :cnt from pcordata.&DEMTBL; 
      quit;
    %end;

    /* only add new person to denom table */;
    %if %eval(&cnt > 0) %then %do;
      proc sql;
       create table id_lookup as
	     select patid from pcordata.denom_raw&yr
	     except
         select patid from pcordata.&DEMTBL
      quit;
  
      data pcordata.denom_raw&yr;  
        if _N_ eq 1 then do;
	      declare hash h(hashexp:16, dataset: "id_lookup");
          h.defineKey('patid');
          h.defineDone();
        end;
        set pcordata.denom_raw&yr;
        if h.find() = 0 then
        output;
      run;
    %end;

    data pcordata.denom_raw&yr;

      %add_newvars_denom;;

      set pcordata.denom_raw&yr;
      BIRTH_DATE = BENE_DOB;  
  
      /* To mapping codes from source to target */;
      %make_mapping_denom;
    run;

    /* clean data, append data to the target table */;
    proc sql;
      create table pcordata.&DEMTBL&yr as
        select &denom_order
        from pcordata.denom_raw&yr;        
    quit; 

	%clean_labels(pcordata, &DEMTBL&yr);  

    %if &yr = &endyr %then %do;
      data pcordata.&DEMTBL; set pcordata.&DEMTBL&yr; run;
    %end; 
    %else %do;
      proc append base = pcordata.&DEMTBL data=pcordata.&DEMTBL&yr; run;
    %end;

    /* transform death table */;
    data pcordata.death_raw&yr;
      set pcordata.denom_raw&yr(keep=&DEATH_KEEP);
	  if DEATH_DT ne .;   
    run;
    %if &yr = &endyr %then %do; %let dcnt = 0; %end;
    %else %do; 
      proc sql;
        select count(*) format=12.0 into :dcnt from pcordata.&DEATTBL; 
      quit;
    %end;

    %if %eval(&dcnt > 0) %then %do;
      data pcordata.death_raw&yr;  
        if _N_ = 1 then do;
	      declare hash h(hashexp:16, dataset:"pcordata.&DEATTBL(keep=patid)");
          h.defineKey('patid');
          h.defineDone();
        end;
        set pcordata.death_raw&yr;
        if h.find() ^= 0 then
        output;
      run;
    %end;

    data pcordata.death_raw&yr;

      %add_newvars_death;;

      set pcordata.death_raw&yr;
      DEATH_DATE = DEATH_DT;
      DEATH_SOURCE = 'L';  
  
      /* To map codes from source to target */;
      %make_mapping_death;
    run;

    /* clean data, append data to the target table */;
    proc sql;
      create table pcordata.&DEATTBL&yr as
      select &death_order
      from pcordata.death_raw&yr;         
    quit;

    %clean_labels(pcordata, &DEATTBL&yr); 

    %if &yr = &endyr %then %do;
      data pcordata.&DEATTBL; set pcordata.&DEATTBL&yr; run;
    %end; 
    %else %do;
      proc append base = pcordata.&DEATTBL data=pcordata.&DEATTBL&yr; run;
    %end;

    ods listing close;
    ods pdf file="&epath/outfolder/etl_denom_&yr..pdf" style = PCORNET_CDMTL;	

    title1 "&yr MBSF_AB Data Transformation";
	title2 "Race (input) to Hispanic (output) mapping crosstab";
	%stats(pcordata.denom_raw&yr, RACE_CMS*HISPANIC);
	title2 "Race (input) to Race (output) mapping crosstab";
    %stats(pcordata.denom_raw&yr, RACE_CMS*RACE);
	title2 "Sex (input) to Sex (output) mapping crosstab";
    %stats(pcordata.denom_raw&yr, SEX*SEX);

    proc print data=pcordata.&DEMTBL&yr (obs=5);
      title2 "Demographic Sample Listing - 5 Rows";
    run;

	title2 "Demographic Variables - Missing and Non-missing Frequencies";
	%stats_all(pcordata.&DEMTBL&yr);
	title2 "V_dod_sw (input) to Death_date_impute (output) mapping crosstab";
    %stats(pcordata.death_raw&yr, V_DOD_SW*DEATH_DATE_IMPUTE);

    proc print data=pcordata.&DEATTBL&yr (obs=5);
      title2 "Death Sample Listing - 5 Rows";
    run;

	title2 "Death Variables - Missing and Non-missing Frequencies";
	%stats_all(pcordata.&DEATTBL&yr);

    ods pdf close;
    ods listing;

    proc datasets library=pcordata nolist;
      delete &DEMTBL&yr denom_raw&yr &DEATTBL&yr death_raw&yr;		
    run;
    %process_end;
    %clean
  %end;/*end of yr loop*/

  /*record the refresh date to harvest table*/
  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_DEMOGRAPHIC_DATE = input("&sysdate", date9.);
	REFRESH_DEATH_DATE = input("&sysdate", date9.);
  run;

  /* Reset listing */;
  proc printto log=log;
  run;  

%mend;

%ETL_DENOM;



