/*USE Pcornet CDM v3.1 data*/
/*read in list*/

%let DEATH_KEEP = %str(	
                PATID
                DEATH_DT    
                V_DOD_SW);
  
%let DX_KEEP = %str(
		     PATID             
             ENCOUNTERID 
             ENC_TYPE 
             ADMIT_DATE  
             PROVIDERID
		     ICD_DGNS_CD:
		     THRU_DT);

%let PX_KEEP = %str(
		     PATID             
             ENCOUNTERID 
             ENC_TYPE 
             ADMIT_DATE  
             PROVIDERID
             ICD_PRCDR_CD:  
             PRCDR_DT:  
             THRU_DT);

/*final list*/
%let DENOM_ORDER = %str(
                 PATID,
                 BIRTH_DATE LENGTH = 8
                            INFORMAT=date9.   
                            FORMAT=date9.,  
                 BIRTH_TIME,
                 SEX,
	             SEXUAL_ORIENTATION,
                 GENDER_IDENTITY,
                 HISPANIC,
                 RACE,
                 BIOBANK_FLAG,
                 RAW_SEX,
	             RAW_SEXUAL_ORIENTATION,
                 RAW_GENDER_IDENTITY,
                 RAW_HISPANIC,
                 RAW_RACE);

 %let DEATH_ORDER = %str(
                  PATID,
                  DEATH_DATE LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
                  DEATH_DATE_IMPUTE,
                  DEATH_SOURCE,
                  DEATH_MATCH_CONFIDENCE);

  %let DISPENSING_ORDER = %str(
                        DISPENSINGID,
                        a.PATID,
                        PRESCRIBINGID,
                        DISPENSE_DATE LENGTH = 8
                                      INFORMAT=date9.   
                                      FORMAT=date9.,
                        NDC,
                        DISPENSE_SUP, 
                        DISPENSE_AMT,
                        RAW_NDC);


%let ENCOUNTER_ORDER = %str(
		             ENCOUNTERID,
                     a.PATID,
                     ADMIT_DATE LENGTH = 8
                                INFORMAT=date9.   
                                FORMAT=date9.,
                     ADMIT_TIME,
                     DISCHARGE_DATE LENGTH = 8
                                    INFORMAT=date9.   
                                    FORMAT=date9.,
                     DISCHARGE_TIME,
                     PROVIDERID,
                     FACILITY_LOCATION,
                     ENC_TYPE,
                     FACILITYID,
                     DISCHARGE_DISPOSITION,
                     DISCHARGE_STATUS,
                     DRG,
                     DRG_TYPE, 
                     ADMITTING_SOURCE,
                     RAW_SITEID,
                     RAW_ENC_TYPE,
                     RAW_DISCHARGE_DISPOSITION,
                     RAW_DISCHARGE_STATUS,
                     RAW_DRG_TYPE,
                     RAW_ADMITTING_SOURCE);

   %let DIAGNOSIS_ORDER = %str(
		                DIAGNOSISID,
                        a.PATID,
                        ENCOUNTERID,
                        ENC_TYPE,
                        ADMIT_DATE LENGTH = 8
                                   INFORMAT=date9.   
                                   FORMAT=date9.,
                        PROVIDERID,
                        DX,       
                        DX_TYPE,
                        DX_SOURCE,
		                DX_ORIGIN,
                        PDX,
                        RAW_DX,
                        RAW_DX_TYPE,
                        RAW_DX_SOURCE,
                        RAW_PDX);

	%let PROCEDURE_ORDER = %str(
                         PROCEDURESID,
                         a.PATID,
                         ENCOUNTERID,
                         ENC_TYPE,
                         ADMIT_DATE LENGTH = 8
                                    INFORMAT=date9.   
                                    FORMAT=date9.,
                         PROVIDERID,
                         PX_DATE    LENGTH = 8
                                    INFORMAT=date9.   
                                    FORMAT=date9.,
                         PX, 
                         PX_TYPE,
                         PX_SOURCE,
                         RAW_PX,
                         RAW_PX_TYPE);

%macro ASSIGN_KEEP_RNM;
  %global denom_keep rn_list enroll_keep enroll_rn ip_keep ip_rn iprev_keep iprev_rn op_keep op_rn oprev_keep oprev_rn 
          carr_keep carr_rn cline_keep cline_rn pde_keep pde_rn fac_keep fac_rn at_keep at_rn prf_keep prf_rn srcadm
          pstus scd ftype psrvc;

   proc sql;
     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :denom_keep separated by ' ', 
                   :rn_list separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :denom_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(strip(name_grp)) = "denom"; 

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
      where lowcase(strip(name_grp)) = "enroll";  
  quit;

  proc sql;
     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :ip_keep separated by ' ', 
                   :ip_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :ip_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "ip";     

	  %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :iprev_keep separated by ' ', 
                   :iprev_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :iprev_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "iprev";     
 
     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :op_keep separated by ' ', 
                   :op_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :op_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "op";     

	 %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :oprev_keep separated by ' ', 
                   :oprev_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :oprev_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "oprev";     
  
     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :carr_keep separated by ' ', 
                   :carr_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :carr_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "carr";     

	 %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :cline_keep separated by ' ', 
                   :cline_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :cline_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "cline";     
  quit;

  proc sql;
     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :pde_keep separated by ' ', 
                   :pde_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :pde_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "pde";     
  quit;

  proc sql;
     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :fac_keep separated by ' ', 
                   :fac_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :fac_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(shname) = "provider";   

      %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :at_keep separated by ' ', 
                   :at_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :at_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(shname) = "at_npi";  

     %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :prf_keep separated by ' ', 
                   :prf_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :prf_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(shname) = "prf_npi";  

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :srcadm separated by ' '
	  from ref.colist
      where lowcase(shname) = "src_adms"; 

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :pstus separated by ' '
	  from ref.colist
      where lowcase(shname) = "ptntstus";

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :scd separated by ' '
	  from ref.colist
      where lowcase(shname) = "stus_cd";

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :ftype separated by ' '
	  from ref.colist
      where lowcase(shname) = "fac_type";

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :psrvc separated by ' '
	  from ref.colist
      where lowcase(shname) = "plcsrvc";
     
  quit;

%mend;

%assign_keep_rnm;

%macro add_newvars_denom;
  length PATID                    $15
         BIRTH_DATE               8
         BIRTH_TIME               8
         SEX                      $2
         SEXUAL_ORIENTATION       $2
		 GENDER_IDENTITY          $2
         HISPANIC                 $2 
         RACE                     $2 
         BIOBANK_FLAG             $1 
         RAW_SEX                  $5
		 RAW_SEXUAL_ORIENTATION   $5
         RAW_GENDER_IDENTITY      $5
         RAW_HISPANIC             $5
         RAW_RACE                 $5;

  call missing (BIRTH_TIME,
	            SEXUAL_ORIENTATION,
                GENDER_IDENTITY,
                BIOBANK_FLAG,
                RAW_SEX,
	            RAW_SEXUAL_ORIENTATION,
                RAW_GENDER_IDENTITY,
                RAW_HISPANIC,
                RAW_RACE)
%mend;

%macro add_newvars_death;
  length  PATID                  $15
          DEATH_DATE             8
          DEATH_DATE_IMPUTE      $2
          DEATH_SOURCE           $2
          DEATH_MATCH_CONFIDENCE $2;

  call missing (DEATH_DATE_IMPUTE,
                DEATH_SOURCE,
                DEATH_MATCH_CONFIDENCE)
%mend;

%macro add_newvars_encounter;
  length ENCOUNTERID               $12     
         PATID                     $15      
         ADMIT_DATE                8            
         ADMIT_TIME                8                                                   
         DISCHARGE_DATE            8
         DISCHARGE_TIME            8                                                  
         PROVIDERID                $10
         FACILITY_LOCATION         $3
         ENC_TYPE                  $2     
         FACILITYID                $6
         DISCHARGE_DISPOSITION     $2
         DISCHARGE_STATUS          $2 
         DRG                       $3  
         DRG_TYPE                  $2      
         ADMITTING_SOURCE          $2
         RAW_SITEID                $5 
         RAW_ENC_TYPE              $5
         RAW_DISCHARGE_DISPOSITION $5  
         RAW_DISCHARGE_STATUS      $5
         RAW_DRG_TYPE              $5
         RAW_ADMITTING_SOURCE      $5;

   call missing (ADMIT_TIME,
                 DISCHARGE_TIME,
                 FACILITY_LOCATION,
                 ENC_TYPE,
                 FACILITYID,
                 DISCHARGE_DISPOSITION,
                 DISCHARGE_STATUS,
                 DRG,
                 DRG_TYPE, 
                 ADMITTING_SOURCE,
                 RAW_SITEID,
                 RAW_ENC_TYPE,
                 RAW_DISCHARGE_DISPOSITION,
                 RAW_DISCHARGE_STATUS,
                 RAW_DRG_TYPE,
                 RAW_ADMITTING_SOURCE) 
%mend;

%macro add_newvars_diagnosis;
  length DX                 $18      
         DX_TYPE            $2
         DX_SOURCE          $2
		 DX_ORIGIN          $2
         PDX                $2
         RAW_DX             $7
         RAW_DX_TYPE        $2
         RAW_DX_SOURCE      $2
         RAW_PDX            $2;

  call missing (DX_SOURCE,
		        DX_ORIGIN,
                PDX,
                RAW_DX,
                RAW_DX_TYPE,
                RAW_DX_SOURCE,
                RAW_PDX)
%mend;

%macro add_newvars_procedure;
  length PX_DATE            8 
         PX                 $11 
         PX_TYPE            $2 
         PX_SOURCE          $2  
         RAW_PX             $7 
         RAW_PX_TYPE        $2;

  call missing (PX_SOURCE,RAW_PX,RAW_PX_TYPE)
%mend;

%macro add_newvars_pde;
  length DISPENSINGID       $12
         PATID              $15
         PRESCRIBINGID      $15
         DISPENSE_DATE      8
         NDC                $11
         DISPENSE_SUP       8 
         DISPENSE_AMT       8
         RAW_NDC            $11;

  call missing (DISPENSINGID, RAW_NDC)
%mend;
