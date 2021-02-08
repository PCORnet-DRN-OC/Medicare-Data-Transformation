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
  run;

  %do yr = &endyr %to &startyr %by -1;
    %process_begin(ETL_DENOM);

	%find_fname(flib=lib_den, memnm=&MBSFTBL);

	/* transform demographic table */;
	%macro DENOM;
    data pcordata.denom_raw&yr(rename=(bene_id=patid race=RACE_CMS));
      set lib_den.&tbnm.(keep=&DENOM_KEEP. %if &nametype eq L %then %do; rename=(&rn_list) %end;);
    run;

	/* transform lds_address_history table */;
   %macro ADDRESS_RAW;
    data pcordata.address_raw&yr;
      set pcordata.denom_raw&yr(keep=&ADDRESS_KEEP);
	  if compress(STATE_CD) ne '' or compress(ZIP_CD) ne '';   
    run;

    %if &yr = &endyr %then %do; %let adcnt = 0; %end;
    %else %do; 
      proc sql;
        select count(*) format=12.0 into :adcnt from pcordata.address_raw; 
      quit;
    %end;

	proc sort data = pcordata.address_raw&yr; by STATE_CD; run;
	proc sort data = ref.state_code_mapping; by STATE_CD; run;

	data pcordata.address_raw&yr;
      merge pcordata.address_raw&yr(in=a) ref.state_code_mapping(in=m);
	  by STATE_CD;
      
	  if a;

	  if a and not b then do; 
         if strip(STATE_CD) ^= '' then  ADDRESS_STATE='OT';
	  end;
	 
    run;

    %clean_labels(pcordata, address_raw&yr); 

    %if &yr = &endyr %then %do;
      data pcordata.address_raw; set pcordata.address_raw&yr; run;
    %end; 
    %else %do;
      proc append base = pcordata.address_raw data=pcordata.address_raw&yr; run;
    %end;

   %mend ADDRESS_RAW;
   %address_raw;

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
   %mend DENOM;
   %denom;

    /* transform death table */;
	%macro DEATH;
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

   %mend DEATH;
   %death;

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

	title2 "Address Source Variables - Missing and Non-missing Frequencies";
	%stats_all(pcordata.address_raw&yr);

    ods pdf close;
    ods listing;

    proc datasets library=pcordata nolist;
      delete &DEMTBL&yr denom_raw&yr &DEATTBL&yr death_raw&yr address_raw&yr;		
    run;
    %process_end;
    %clean
  %end;/*end of yr loop*/

%macro LDS_ADDRESS_HISTORY;

 proc sort data = pcordata.address_raw; by PATID RFRNC_YR ZIP_CD ADDRESS_STATE; run;

 data pcordata.address_raw; 
  retain re_syr re_eyr re_addr;
  set pcordata.address_raw; 
  by PATID RFRNC_YR ZIP_CD ADDRESS_STATE;

   addr=cats(coalescec(ZIP_CD,'none'),"_",coalescec(ADDRESS_STATE,'none'));
   if first.PATID then do;
     re_syr=RFRNC_YR;  
     re_addr=addr; 
   end;

   if re_addr^=addr then do;
      re_syr=RFRNC_YR;  
      re_addr=addr;
   end;
 
run;

proc sql;
  create table &ADDRTBL as
    select PATID, ADDRESS_STATE, ZIP_CD,re_syr as startyr, max(RFRNC_YR) as endyr
    from pcordata.address_raw
    group by PATID, ADDRESS_STATE, ZIP_CD, re_syr;         
quit;

 proc sort data = &ADDRTBL; by PATID startyr; run;

 data &ADDRTBL;
  %add_newvars_address;;
  set &ADDRTBL;
  by PATID startyr;
  ADDRESSID = left(put(md5(cats(patid,"_",put(startyr,best.),"_",coalescec(ZIP_CD,'none'),"_",coalescec(ADDRESS_STATE,'none'))),hex16.));
  ADDRESS_USE = 'HO';
  ADDRESS_TYPE = 'BO';
  ADDRESS_PREFERRED = 'Y'; 
  ADDRESS_ZIP5 = left(ZIP_CD);
  ADDRESS_PERIOD_START = mdy(1,1,startyr);

  if last.PATID then ADDRESS_PERIOD_END =.;
  else ADDRESS_PERIOD_END = mdy(12,31,endyr);
 run;

  proc sql;
      create table pcordata.&ADDRTBL. as
      select &address_order
      from &ADDRTBL;         
    quit;

  proc datasets library=work nolist; delete &ADDRTBL; 
  run;

  proc datasets library=pcordata nolist; delete address_raw; run;
 %mend LDS_ADDRESS_HISTORY;
 %lds_address_history;

  /*record the refresh date to harvest table*/
  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_DEMOGRAPHIC_DATE = input("&sysdate9", date9.);
	REFRESH_DEATH_DATE = input("&sysdate9", date9.);
	REFRESH_LDS_ADDRESS_HX_DATE = input("&sysdate9", date9.);
  run;

  /* Reset listing */;
  proc printto log=log;
  run; 

%mend;

%ETL_DENOM;



