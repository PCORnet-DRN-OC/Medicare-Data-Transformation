/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_enrollment.sas                          
*         Date:  12/13/2017                                               
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to create CDM enrollment table  
*           based on Medicare part A, B and D plan information
*
*  Inputs:  
*           1) MBSF_AB&MBSF_D/MBSF_ABD 
*           
*           2) SAS programs:
*               /etl/infolder/build_enroll.sas 
*                             
*  Output:  
*           1) Enrollment table at /etl/cdm_v31 
*           2) SAS log file in /etl/outfolder
*           3) SAS output file in PDF format stored in /etl/outfolder
*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;
%macro etl_enrollment;
  /*- Redirect listing of control flow info to its own log file -*/ ;
  proc printto new log="&epath/outfolder/etl_enrollment.log";

  /*- Run ffs-eligibility for each denominator year -*/;
  %do yr = &startyr %to &endyr;

    data denom&yr(keep=BENE_ID BUYIN HMOIND);
      set lib_den.&MBSFTBL.&yr(keep=&ENROLL_KEEP. %if &nametype eq L %then %do; rename=(&enroll_rn) %end;);
	  length BUYIN HMOIND $ 12;
	  BUYIN = cat(of BUYIN01-BUYIN12);
	  HMOIND = cat(of HMOIND01-HMOIND12);
	  output;
    run;

    %denomelig(denom&yr, pcordata.elig&yr, bene_id, &yr);

    /*- Just tweaking for the first time when enrollment table is not -*/;
    %if %cmpres(&yr)=&startyr %then %do; 
      %denomelig_append(_null_, pcordata.elig&yr, pcordata.denomelig, Bene_id);
    %end;
    %else %do;
      %denomelig_append(pcordata.denomelig, pcordata.elig&yr, pcordata.denomelig, Bene_id);
    %end;
  %end;

  proc sql;
    create table pcordata.&ENRTBL as
      select distinct BENE_ID as PATID,
             FIRST_DT format=date9. as ENR_START_DATE,
             LAST_DT format=date9. as ENR_END_DATE,
             'N' as CHART,
			 'I' as ENR_BASIS
      from pcordata.denomelig; 
  quit;

  proc datasets library=pcordata nolist;
    delete denomelig elig:;		
  quit;
 
  %if %length(&PTDENOMTBL) > 0 %then %do; /* begin-if mbsf-D data is available to transform */
    proc sql;
     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :enroll_keep separated by ' ', 
                   :enroll_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :enroll_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(strip(name_grp)) = "ptdenroll";     
    quit;

    %do yr = &pdstartyr %to &pdendyr;

      data ptdenom&yr;
        set lib_pden.&PTDENOMTBL&yr(keep=&ENROLL_KEEP. %if &nametype eq L %then %do; rename=(&enroll_rn) %end;);
      run;

      %ptdelig(ptdenom&yr, pcordata.elig&yr, &yr);

      %if %cmpres(&yr)=&startyr %then %do; 
        %ptdenomelig_append(_null_, pcordata.elig&yr, pcordata.ptdelig, Bene_id);
      %end;
      %else %do;
        %ptdenomelig_append(pcordata.ptdelig, pcordata.elig&yr, pcordata.ptdelig, Bene_id);
      %end;
    %end;

    proc sql;
      create table pcordata.ptdenrollment as
        select distinct BENE_ID as PATID,
            FIRST_DT format=date9. as ENR_START_DATE,
            LAST_DT format=date9. as ENR_END_DATE,
            'N' as CHART,
			'D' as ENR_BASIS
        from pcordata.ptdelig; 
    quit;

    proc append base = pcordata.&ENRTBL data=pcordata.ptdenrollment; run;
  %end; /* end-if mbsf-d data is available to transform */ 

  %clean_labels(pcordata, &ENRTBL);

  ods listing close;
  ods pdf file="&epath/outfolder/etl_enrollment.pdf" style = PCORNET_CDMTL;	

  data &ENRTBL (keep=CHART ENR_BASIS STARTYR);
    set pcordata.&ENRTBL;
    STARTYR = year(ENR_START_DATE);
  run;

  title1 'Summary of Enrollment table';
  title2 'Frequencies for chart and enr_basis';
  %stats(&ENRTBL, CHART ENR_BASIS);
  title2 'Startyr and enr_basis crosstab';
  %stats(&ENRTBL, STARTYR*ENR_BASIS);

  ods pdf close;
  ods listing;

  /*record the refresh date*/
  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_ENROLLMENT_DATE = input("&sysdate", date9.); 
  run;

  proc datasets library=pcordata nolist;
    delete denomelig elig:;		
  quit;
%mend;

%etl_enrollment;
