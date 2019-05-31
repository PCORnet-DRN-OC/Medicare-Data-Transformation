/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  etl_op.sas                          
*  Create Date:  12/15/2017 
*     Modified:  08/22/2018  
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to transform yearly Part D Drug Event tables    
*           to CDM Dispensing table 
*
*  Inputs:   
*           Part D drug event table        
*                             
*  Output:
*           1) Annual CDM Dispensing table in /etl/cdm_v41 
*           2) SAS log files in /etl/outfolder
*           3) SAS output files per year in PDF format stored in /etl/outfolder
*
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;
%macro assignvars_pde;
      PATID = BENE_ID; 
      DISPENSE_DATE = SRVC_DT;  
      NDC = PRDSRVID;  
      DISPENSE_SUP = DAYSSPLY;  
      DISPENSE_AMT = QTYDSPNS; 
%mend;

%macro ETL_PDE;

  /*- Redirect listing of control flow info to its own log file -*/ ;
  proc printto new log="&epath/outfolder/etl_pde.log" ;run;
  %process_begin(ETL_PDE);
 
  proc sql noprint; 
    create table pde as 
      select distinct memname from dictionary.tables 
      where lowcase(memname) ? "&PDETBL" and memname ? "&yr" and memtype='DATA';
  quit;

  data _null_;
    set pde end=end;
    cnt+1;
    call symputx('de'||put(cnt,4.-l),memname);
    if end then call symputx('pdemax',cnt);
  run;

  /*- transform dispensing table -*/;
  proc sql;
    create table pcordata.pde_raw&yr as
	  %do i=1 %to &pdemax;
	    select distinct BENE_ID   
               ,SRVC_DT 
               ,PRDSRVID 
               ,DAYSSPLY  
               ,QTYDSPNS
	    from lib_pde.&&de&i(keep=&PDE_KEEP %if &nametype eq L %then %do; rename=(&PDE_RN) %end;)
	    %if %eval(&i < &pdemax) %then %do; union %end;
      %end;;

    %if %sysfunc(exist(pcordata.&DISTBL&prev_yr)) = 0 %then %do; 
	   %let dcnt = 0;
    %end;
    %else %do;
       select count(*) format=12.0, 
              max(dispensingid)  
       into :dcnt, :dpremax 
       from pcordata.&DISTBL&prev_yr;  

	   %let doffset = &dpremax;
    %end;
  quit;

  data pcordata.pde_raw&yr;
  
    %add_newvars_pde;;
  
    set pcordata.pde_raw&yr;

    %if %eval(&dcnt > 0) %then %do;
      DISPENSINGIDn = _n_ + input(&doffset,12.);
    %end;
    %else %do;
      DISPENSINGIDn = _n_;
    %end;

	 DISPENSINGID = put(DISPENSINGIDn,z12.);
  
   /*- To assign variable names from source to target -*/; 
   %assignvars_pde;
  run;

  /*- clean data, append data to the target table -*/;
  proc sql;
    create table pcordata.&DISTBL&yr as
      select &DISPENSING_ORDER
      from pcordata.pde_raw&yr a,
           (select PATID from pcordata.demographic) b
      where a.PATID = b.PATID;   
  quit; 

  %clean_labels(pcordata, &DISTBL&yr);

  /*- remove transaction tables -*/;
  proc datasets library=pcordata nolist;
    delete pde_raw&yr;		
  quit;

  data pcordata.harvest;
    set pcordata.harvest;
	REFRESH_&DISTBL._DATE = input("&sysdate", date9.);
  run;

  ods listing close;
  ods pdf file="&epath/outfolder/etl_pde_&yr..pdf" style = PCORNET_CDMTL;	

  title1 "&yr Part D drug event Data Transformation";
  proc print data=pcordata.&DISTBL&yr (obs=5);
    title2 "Dispensing Sample Listing - 5 Rows";
  run;

  title2 "Dispensing Variables - Missing and Non-missing Frequencies";
  %stats_all(pcordata.&DISTBL&yr);

  ods pdf close;
  ods listing;

  %process_end;
  %clean;

  proc printto new log=log ;run;

%mend ETL_PDE;

%ETL_PDE;




