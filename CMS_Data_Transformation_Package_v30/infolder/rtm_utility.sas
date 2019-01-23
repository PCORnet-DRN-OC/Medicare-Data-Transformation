/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                     
* Program Name:  rtm_utility.sas                          
*         Date:  12/15/2017                                                
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to store macros repeatly used
*           in data transformation programs                
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;
%macro process_begin(prgnm);
  data runtime;
    length programs $100 data_year $20;
    format start_time end_time datetime19. processing_time time8.;
	array _char_ programs data_year;
    array _num_  start_time end_time processing_time;
    programs="%upcase(&prgnm)";
    start_time=datetime();
	data_year = "&yr";
    output;        
  run;
%mend process_begin;

%macro process_end;
  data runtime;        
    set runtime;
	end_time=datetime();
    processing_time=end_time-start_time;             
  run;
    
  proc append base=run_time_tracking data=runtime; run;  
%mend process_end;

%macro find_fname(flib=, memnm=);
  %global tbnm;
  proc sql; 
     select distinct memname into :tbnm from dictionary.tables 
     where lowcase(libname)="&flib" and lowcase(memname) ? "&memnm" 
     and memname ? "&yr" and memtype='DATA';
  quit;
%mend;


%macro clean(savedsn);
  proc datasets  noprint;
    save run_time_tracking &savedsn / memtype=data;
    save formats / memtype=catalog;
  quit;
%mend clean;

%macro clean_labels(lib, cdm);
  proc datasets nolist lib=&lib. memtype=data;
    modify &cdm.; 
    attrib _all_ label=' '; 
  quit;
%mend clean_labels;

%macro stats(inds, xtab);
  proc freq data=&inds;
    table &xtab/ nocum list;
  run;
%mend stats;

%macro stats_all(inds);
  proc freq data=&inds;
      tables _ALL_ / missing nocum list;
      format _numeric_ msck.
             _char_ $msck.;
  run;
%mend stats_all;

    
