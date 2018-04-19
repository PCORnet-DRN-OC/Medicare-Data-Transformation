/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  pcncdm_formats.sas                          
*         Date:  12/15/2017                                               
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to store macros used repeatedly in ETL programs       
*          for transforming Diagnosis and Procedure tables.           
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;
%macro build_diagnosis(inds, ds, n);

  /*arrange diagnosis code vertically*/
  data dx_raw&yr()/ view= dx_raw&yr;
    %add_newvars_diagnosis;;

    set pcordata.&inds&yr;
	%if &ds = ip %then %do;
	 by ENCOUNTERID;
    %end;
    array _dx(&n)     $ ICD_DGNS_CD1-ICD_DGNS_CD&n;
	%if &ds = carr %then %do;
	  array _dx_vrs(&n) $ ICD_DGNS_VRSN_CD1-ICD_DGNS_VRSN_CD&n;
    %end;
	%if &ds = ip %then %do;
	  array _POA(&n)   $ CLM_POA_IND_SW1-CLM_POA_IND_SW&n;
	%end;

    year = year(thru_dt);
	mon = month(thru_dt);
    DX_TYPE  = ifc((year >= 2015 and mon >= 10), '10', '09');
    DX_ORIGIN = 'CL';

    do j = 1 to &n;
	  if not missing(_dx(j)) then do;
		DX = _dx(j);
        %if &ds = carr %then %do;
		   if year >= 2015 and mon >= 10 then do;
              DX_TYPE  = ifc(_dx_vrs(j) = '9', '09', ifc(_dx_vrs(j) = '0', '10', ''));
		   end;
		%end;
		%if &ds = ip %then %do;
		   RAW_DX_SOURCE = ifc((_POA(j)='Y'), 'Y', ''); 
		   DX_SOURCE = 'DI';
		   if first.ENCOUNTERID then do;
			 PDX = ifc((j = 1), 'P', 'S');
		   end;
		   else do;
			 PDX = 'S';
		   end;
		%end;
		%else %do; 
          DX_SOURCE = 'FI';
          PDX  = 'X';  
         %end;
		  output;
      end;/*end if*/
    end;/*end do*/

	%if &ds = carr %then %do;
	  DX = LINE_ICD_DGNS_CD;
	%end;
  run;

  proc sql;
    %if %sysfunc(exist(pcordata.&DIATBL&yr)) > 0 %then %do; 
	  select count(*) format=12.0, 
             max(DIAGNOSISID)  
      into :dcnt, :dpremax 
      from pcordata.&DIATBL&yr;  

	  %let doffset = &dpremax;
	%end;
	%else %if %sysfunc(exist(pcordata.&DIATBL&prev_yr)) = 0 %then %do; 
	  %let dcnt = 0;
	%end;
	%else %do;
        select count(*) format=12.0, max(DIAGNOSISID) 
        into :dcnt, :dpremax 
        from pcordata.&DIATBL&prev_yr; 
  
        %let doffset = &dpremax; 
     %end; 
  quit;

  data pcordata.dx_raw&yr;
    set dx_raw&yr;
  
    %if %eval(&dcnt > 0) %then %do;
      DIAGNOSISIDn = _n_ + input(&doffset, 12.);
    %end;
    %else %do;
      DIAGNOSISIDn = _n_;
    %end;

	 DIAGNOSISID = put(DIAGNOSISIDn,z12.);
  run; 

   /*clean date, and append data to the target table*/
  proc sql;
   create table pcordata.diagnosis_&ds.&yr
   as
   select distinct &diagnosis_order
   from pcordata.dx_raw&yr a,
        (select PATID from pcordata.demographic) b
   where a.PATID = b.PATID;   
  quit; 

  %clean_labels(pcordata, diagnosis_&ds.&yr); 

  %if %sysfunc(exist(pcordata.&DIATBL&yr)) %then %do;  
     proc append base = pcordata.&DIATBL&yr data=pcordata.diagnosis_&ds.&yr; run;
  %end;
  %else %do;
    data pcordata.&DIATBL&yr; set pcordata.diagnosis_&ds.&yr; run;
  %end;

  proc datasets library=pcordata nolist;
    delete &inds&yr dx_raw&yr;		
  quit;
 
%mend;

%macro build_procedure(inds, ds, n);

  /*arrange diagnosis code vertically*/
  data px_raw&yr()/ view= px_raw&yr;
    %add_newvars_procedure;;

    set pcordata.&inds&yr;
       array _px(&n)     $ ICD_PRCDR_CD1-ICD_PRCDR_CD&n;
	   array _pxdt(&n)   $ PRCDR_DT1-PRCDR_DT&n;

	   year = year(thru_dt);
	   mon = month(thru_dt);

	   if year >= 2015 and mon >= 10 then do;
	     PX_TYPE  = '10';
	   end;
	   else do;
	     PX_TYPE  = '09';
	   end;
	 do j = 1 to &n;
	   if not missing(_px(j)) then do;
		 PX = _px(j);
         PX_DATE = _pxdt(j);	
		 PX_SOURCE = 'CL';		
		 output;
	   end;/*end if*/
	 end;/*end do*/
   run;

   proc sql;
     %if %sysfunc(exist(pcordata.&PROCTBL&yr)) > 0 %then %do; 
		 select count(*) format=12.0, 
               max(PROCEDURESID)  
         into :pcnt, :ppremax 
         from pcordata.&PROCTBL&yr;  

	     %let poffset = &ppremax;
	  %end;
	  %else %if %sysfunc(exist(pcordata.&PROCTBL&prev_yr)) = 0 %then %do; 
	     %let pcnt = 0;
	  %end;
	  %else %do;
        select count(*) format=12.0, max(PROCEDURESID) 
        into :pcnt, :ppremax 
        from pcordata.&PROCTBL&prev_yr; 
    
        %let poffset = &ppremax; 
      %end; 
   quit;

   data pcordata.px_raw&yr;
     set px_raw&yr;
  
     %if %eval(&pcnt > 0) %then %do;
       PROCEDURESIDn = _n_ + input(&poffset, 12.);
     %end;
     %else %do;
       PROCEDURESIDn = _n_;
     %end;

	 PROCEDURESID = put(PROCEDURESIDn,z12.);
   run; 


  /*clean date, and append data to the target table*/
  proc sql;
   create table pcordata.procedure_&ds.&yr
   as
   select &procedure_order
   from pcordata.px_raw&yr a,
        (select PATID from pcordata.demographic) b
   where a.PATID = b.PATID;   
  quit; 

  %clean_labels(pcordata, procedure_&ds.&yr); 

  %if %sysfunc(exist(pcordata.&PROCTBL&yr)) %then %do; 
    proc append base = pcordata.&PROCTBL&yr data=pcordata.procedure_&ds.&yr;
    run;
  %end;
  %else %do;
    data pcordata.&PROCTBL&yr; set pcordata.procedure_&ds.&yr; run;
  %end;
  proc datasets library=pcordata nolist;
    delete &inds&yr px:;		
  quit;
 
%mend;

%macro build_procedure_revhcpcs(inds, ds);

  proc sql;
    %if %sysfunc(exist(pcordata.&PROCTBL&yr)) > 0 %then %do; 
	  select count(*) format=12.0, 
             max(PROCEDURESID)  
      into :pcnt, :ppremax 
      from pcordata.&PROCTBL&yr;  

	  %let poffset = &ppremax;
	 %end;
	 %else %if %sysfunc(exist(pcordata.&PROCTBL&prev_yr)) = 0 %then %do; 
	     %let pcnt = 0;
	 %end;
	 %else %do;
       select count(*) format=12.0, max(PROCEDURESID) 
       into :pcnt, :ppremax 
       from pcordata.&PROCTBL&prev_yr; 
    
       %let poffset = &ppremax; 
      %end; 
   quit;

   data px_raw_rev&yr()/ view= px_raw_rev&yr;
     %add_newvars_procedure;;
     set pcordata.&inds&yr;

	 %if %eval(&pcnt > 0) %then %do;
       PROCEDURESIDn = _n_ + input(&poffset, 12.);
     %end;
     %else %do;
       PROCEDURESIDn = _n_;
     %end;

	 PROCEDURESID = put(PROCEDURESIDn,z12.);

	 %if &ds = op %then %do;
	   if compress(HCPCS_CD) eq '' then do;
	    PX = REV_CNTR;
		PX_DATE = coalesce(REV_DT,THRU_DT);
	    PX_TYPE  = 'RE';
	   end;
	   else do;
	    PX = HCPCS_CD;
		PX_DATE = THRU_DT;
	    /*determine CPT4 or HCPCS level II*/
	    PX_TYPE = 'CH';
	   end;
	 %end;
	 %else %if &ds = carr %then %do;
	    PX = HCPCS_CD;
	    PX_DATE = EXPNSDT1;
	    PX_TYPE = 'CH';
	 %end;
	 %else %do;
	    PX = REV_CNTR;
	    PX_DATE = THRU_DT;
	    PX_TYPE  = 'RE';
	 %end;
 
   run;

  /*clean date, and append data to the target table*/
  proc sql;
   create table pcordata.procedurerev_&ds.&yr
   as
   select &procedure_order
   from px_raw_rev&yr a,
        (select PATID from pcordata.demographic) b
   where a.PATID = b.PATID;   
  quit; 

  %clean_labels(pcordata, procedurerev_&ds.&yr); 

  %if %sysfunc(exist(pcordata.&PROCTBL&yr)) %then %do; 
    proc append base = pcordata.&PROCTBL&yr data=pcordata.procedurerev_&ds.&yr; run;
  %end;
  %else %do;
    data pcordata.&PROCTBL&yr; set pcordata.procedurerev_&ds.&yr; run;
  %end;

  proc datasets library=pcordata nolist;
    delete &inds&yr;		
  quit;
 
%mend;

