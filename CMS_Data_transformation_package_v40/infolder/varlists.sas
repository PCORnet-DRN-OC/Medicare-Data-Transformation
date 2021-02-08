/*USE Pcornet CDM v5.1 data*/
/*read in list*/

%let DEATH_KEEP = %str(	
                PATID
                DEATH_DT    
                V_DOD_SW);

%let ADDRESS_KEEP = %str(	
                PATID   
                STATE_CD
                ZIP_CD
                RFRNC_YR);
  
%let DX_KEEP = %str(
		     PATID             
             ENCOUNTERID 
             ENC_TYPE 
             ADMIT_DATE  
             PROVIDERID
		     ICD_DGNS_CD:
		     THRU_DT);

%let DX_MP_KEEP = %str(
		     PATID             
             ENCOUNTERID 
             ENC_TYPE 
             ADMIT_DATE  
             PROVIDERID
		     DGNSCD:
		     DGNS_VRSN_CD:
             POA_DGNS:);

%let PX_KEEP = %str(
		     PATID             
             ENCOUNTERID 
             ENC_TYPE 
             ADMIT_DATE  
             PROVIDERID
             ICD_PRCDR_CD:  
             PRCDR_DT:  
             THRU_DT);

%let PX_MP_KEEP = %str(
		     PATID             
             ENCOUNTERID 
             ENC_TYPE 
             ADMIT_DATE  
             PROVIDERID
             PRCDRCD:  
             PRCDRDT:  
             SRGCL_PRCDR_VRSN_CD:);

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
				 PAT_PREF_LANGUAGE_SPOKEN,
                 RAW_SEX,
	             RAW_SEXUAL_ORIENTATION,
                 RAW_GENDER_IDENTITY,
                 RAW_HISPANIC,
                 RAW_RACE,
                 RAW_PAT_PREF_LANGUAGE_SPOKEN);

 %let DEATH_ORDER = %str(
                  PATID,
                  DEATH_DATE LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
                  DEATH_DATE_IMPUTE,
                  DEATH_SOURCE,
                  DEATH_MATCH_CONFIDENCE);

%let ADDRESS_ORDER = %str(
				  ADDRESSID,
                  PATID,
                  ADDRESS_USE,
                  ADDRESS_TYPE,
                  ADDRESS_PREFERRED,
                  ADDRESS_CITY,
                  ADDRESS_STATE,
                  ADDRESS_ZIP5,
                  ADDRESS_ZIP9,
                  ADDRESS_PERIOD_START LENGTH = 8
                                       INFORMAT=date9.   
                                       FORMAT=date9.,
                  ADDRESS_PERIOD_END LENGTH = 8
                                     INFORMAT=date9.   
                                     FORMAT=date9.);

  %let DISPENSING_ORDER = %str(
                        DISPENSINGID,
                        a.PATID,
                        PRESCRIBINGID,
                        DISPENSE_DATE LENGTH = 8
                                      INFORMAT=date9.   
                                      FORMAT=date9.,
                        NDC,
						DISPENSE_SOURCE,
                        DISPENSE_SUP, 
                        DISPENSE_AMT,
						DISPENSE_DOSE_DISP,
						DISPENSE_DOSE_DISP_UNIT,
						DISPENSE_ROUTE,
                        RAW_NDC,
                        RAW_DISPENSE_DOSE_DISP,
                        RAW_DISPENSE_DOSE_DISP_UNIT,
						RAW_DISPENSE_ROUTE);

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
					 PAYER_TYPE_PRIMARY,
		             PAYER_TYPE_SECONDARY,
					 FACILITY_TYPE,
                     RAW_SITEID,
                     RAW_ENC_TYPE,
                     RAW_DISCHARGE_DISPOSITION,
                     RAW_DISCHARGE_STATUS,
                     RAW_DRG_TYPE,
                     RAW_ADMITTING_SOURCE,
					 RAW_FACILITY_TYPE,
                     RAW_PAYER_TYPE_PRIMARY,
				     RAW_PAYER_NAME_PRIMARY,
				     RAW_PAYER_ID_PRIMARY,
                     RAW_PAYER_TYPE_SECONDARY,
                     RAW_PAYER_NAME_SECONDARY,
                     RAW_PAYER_ID_SECONDARY);

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
						DX_DATE LENGTH = 8
                                INFORMAT=date9.   
                                FORMAT=date9.,
                        DX_SOURCE,
		                DX_ORIGIN,
                        PDX,
						DX_POA,
                        RAW_DX,
                        RAW_DX_TYPE,
                        RAW_DX_SOURCE,
                        RAW_PDX,
                        RAW_DX_POA);

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
						 PPX,
                         RAW_PX,
                         RAW_PX_TYPE,
                         RAW_PPX);

	%let PROVIDER_ORDER = %str(
                         PROVIDERID,
                         PROVIDER_SEX,
                         PROVIDER_SPECIALTY_PRIMARY,
                         PROVIDER_NPI,
                         PROVIDER_NPI_FLAG,
		                 RAW_PROVIDER_SPECIALTY_PRIMARY);

%macro ASSIGN_KEEP_RNM;
  %global denom_keep rn_list enroll_keep enroll_rn ip_keep ip_rn snf_keep snf_rn mp_keep mp_rn iprev_keep iprev_rn op_keep op_rn oprev_keep oprev_rn 
          carr_keep carr_rn cline_keep cline_rn pde_keep pde_rn fac_keep fac_mp_keep fac_rn fac_mp_rn at_keep at_rn prf_keep prf_rn srcadm srcadmp
          pstus pstusmp scd scdmp ftype psrvc;

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
              into :snf_keep separated by ' ', 
                   :snf_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :snf_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "ip"
      and lowcase(shname) ^? ('clm_poa_ind_sw');  

      %if &nametype eq L %then %do;
      select distinct loname,cats(loname,"=",shname) 
              into :mp_keep separated by ' ', 
                   :mp_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :mp_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(name_grp) = "medpar";    

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
              into :fac_mp_keep separated by ' ', 
                   :fac_mp_rn separated by ' '
     %end;
	 %else %do;
	   select distinct shname
              into :fac_mp_keep separated by ' '
	 %end;
      from ref.colist
      where lowcase(shname) = "prvdrnum";   

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
      where lowcase(shname) = "prf_npi" or lowcase(name_grp) = 'cline_prov';  

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :srcadm separated by ' '
	  from ref.colist
      where lowcase(shname) = "src_adms"
      and lowcase(name_grp) = "ip"; 

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :srcadmp separated by ' '
	  from ref.colist
      where lowcase(shname) = "src_adms"
      and lowcase(name_grp) = "medpar"; 

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :pstus separated by ' '
	  from ref.colist
      where lowcase(shname) = "ptntstus"
      and lowcase(name_grp) = "ip";

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :pstusmp separated by ' '
	  from ref.colist
      where lowcase(shname) = "dschrgcd"
      and lowcase(name_grp) = "medpar";

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :scdmp separated by ' '
	  from ref.colist
      where lowcase(shname) = "dstntncd"
      and lowcase(name_grp) = "medpar";

	  select distinct 
      %if &nametype eq L %then %do; loname %end;
	  %else %do; shname %end;
      into :scd separated by ' '
	  from ref.colist
      where lowcase(shname) = "stus_cd"
      and lowcase(name_grp) = "ip";

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
  length PATID                        $15
         BIRTH_DATE                   8
         BIRTH_TIME                   8
         SEX                          $2
         SEXUAL_ORIENTATION           $2
		 GENDER_IDENTITY              $2
         HISPANIC                     $2 
         RACE                         $2 
         BIOBANK_FLAG                 $1
         PAT_PREF_LANGUAGE_SPOKEN     $3 
         RAW_SEX                      $5
		 RAW_SEXUAL_ORIENTATION       $5
         RAW_GENDER_IDENTITY          $5
         RAW_HISPANIC                 $5
         RAW_RACE                     $5
		 RAW_PAT_PREF_LANGUAGE_SPOKEN $5;

  call missing (BIRTH_TIME,
	            SEXUAL_ORIENTATION,
                GENDER_IDENTITY,
                BIOBANK_FLAG,
				PAT_PREF_LANGUAGE_SPOKEN,
                RAW_SEX,
	            RAW_SEXUAL_ORIENTATION,
                RAW_GENDER_IDENTITY,
                RAW_HISPANIC,
                RAW_RACE,
                RAW_PAT_PREF_LANGUAGE_SPOKEN)
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

%macro add_newvars_address;
  length  ADDRESSID	            $16
          PATID	                $15
          ADDRESS_USE	        $2
          ADDRESS_TYPE	        $2
          ADDRESS_PREFERRED	    $2
          ADDRESS_CITY	        $20
          ADDRESS_STATE  	    $2
          ADDRESS_ZIP5	        $5
          ADDRESS_ZIP9	        $9
          ADDRESS_PERIOD_START	8
          ADDRESS_PERIOD_END	8;

  call missing (ADDRESS_CITY,
                ADDRESS_STATE,
                ADDRESS_ZIP5,
                ADDRESS_ZIP9,
                ADDRESS_PERIOD_END)

%mend;

%macro add_newvars_encounter;
  length ENCOUNTERID               $12     
         PATID                     $15      
         ADMIT_DATE                8            
         ADMIT_TIME                8                                                   
         DISCHARGE_DATE            8
         DISCHARGE_TIME            8                                                  
         PROVIDERID                $10
         FACILITY_LOCATION         $5
         ENC_TYPE                  $2     
         FACILITYID                $10
         DISCHARGE_DISPOSITION     $2
         DISCHARGE_STATUS          $2 
         DRG                       $3  
         DRG_TYPE                  $2      
         ADMITTING_SOURCE          $2
		 PAYER_TYPE_PRIMARY        $5
		 PAYER_TYPE_SECONDARY      $5
		 FACILITY_TYPE             $60
         RAW_SITEID                $5 
         RAW_ENC_TYPE              $5
         RAW_DISCHARGE_DISPOSITION $5  
         RAW_DISCHARGE_STATUS      $5
         RAW_DRG_TYPE              $5
         RAW_ADMITTING_SOURCE      $5
		 RAW_FACILITY_TYPE         $60
		 RAW_PAYER_TYPE_PRIMARY    $5
		 RAW_PAYER_NAME_PRIMARY    $5
		 RAW_PAYER_ID_PRIMARY      $5
         RAW_PAYER_TYPE_SECONDARY  $5
         RAW_PAYER_NAME_SECONDARY  $5
         RAW_PAYER_ID_SECONDARY    $5;

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
                 RAW_ADMITTING_SOURCE,
				 RAW_FACILITY_TYPE,
                 RAW_PAYER_TYPE_PRIMARY,
				 RAW_PAYER_NAME_PRIMARY,
				 RAW_PAYER_ID_PRIMARY,
                 RAW_PAYER_TYPE_SECONDARY,
                 RAW_PAYER_NAME_SECONDARY,
                 RAW_PAYER_ID_SECONDARY) 
%mend;

%macro add_newvars_diagnosis;
  length DX                 $18      
         DX_TYPE            $2
		 DX_DATE            8
         DX_SOURCE          $2
		 DX_ORIGIN          $2
         PDX                $2
		 DX_POA             $2
         RAW_DX             $7
         RAW_DX_TYPE        $2
         RAW_DX_SOURCE      $2
         RAW_PDX            $2
         RAW_DX_POA         $2;

  call missing (DX_DATE,
                DX_SOURCE,
		        DX_ORIGIN,
                PDX,
				DX_POA,
                RAW_DX,
                RAW_DX_TYPE,
                RAW_DX_SOURCE,
                RAW_PDX,
                RAW_DX_POA)
%mend;

%macro add_newvars_procedure;
  length PX_DATE            8 
         PX                 $11 
         PX_TYPE            $2 
         PX_SOURCE          $2 
         PPX                $2  
         RAW_PX             $7 
         RAW_PX_TYPE        $2
         RAW_PPX            $2;

  call missing (PX_SOURCE,PPX,RAW_PX,RAW_PX_TYPE,RAW_PPX)
%mend;


%macro add_newvars_pde;
  length DISPENSINGID                $12
         PATID                       $15
         PRESCRIBINGID               $15
         DISPENSE_DATE               8
         NDC                         $11
		 DISPENSE_SOURCE             $2
         DISPENSE_SUP                8 
         DISPENSE_AMT                8
		 DISPENSE_DOSE_DISP          8
		 DISPENSE_DOSE_DISP_UNIT     $5
		 DISPENSE_ROUTE              $5
         RAW_NDC                     $11
         RAW_DISPENSE_DOSE_DISP      $5
         RAW_DISPENSE_DOSE_DISP_UNIT $5 
		 RAW_DISPENSE_ROUTE          $5; 

  call missing (DISPENSINGID, 
                DISPENSE_DOSE_DISP, 
                DISPENSE_DOSE_DISP_UNIT, 
                DISPENSE_ROUTE, 
                RAW_NDC, 
                RAW_DISPENSE_DOSE_DISP, 
                RAW_DISPENSE_DOSE_DISP_UNIT,
                RAW_DISPENSE_ROUTE)
%mend;

%macro add_newvars_prov;
  length PROVIDERID                      $10
         PROVIDER_SEX                    $2
         PROVIDER_SPECIALTY_PRIMARY      $15
         PROVIDER_NPI                    8
         PROVIDER_NPI_FLAG               $1
		 RAW_PROVIDER_SPECIALTY_PRIMARY  $4
         ;

  call missing (PROVIDER_SEX)
%mend;

%macro mapping_enc_share(var);
  
  SELECT (&var); 
    WHEN ('C','M','N', 'Z', '') DO; PAYER_TYPE_PRIMARY  = '121'; PAYER_TYPE_SECONDARY  = 'NI'; END; 
    WHEN ('I') DO; PAYER_TYPE_PRIMARY  = '32'; PAYER_TYPE_SECONDARY  = '121'; END; 
    WHEN ('H') DO; PAYER_TYPE_PRIMARY  = '35'; PAYER_TYPE_SECONDARY  = '121'; END; 
    WHEN ('G') DO; PAYER_TYPE_PRIMARY  = '37'; PAYER_TYPE_SECONDARY  = '121'; END; 
    WHEN ('F') DO; PAYER_TYPE_PRIMARY  = '38'; PAYER_TYPE_SECONDARY  = '121'; END; 
    WHEN ('A','B') DO; PAYER_TYPE_PRIMARY  = '5'; PAYER_TYPE_SECONDARY  = '121'; END; 
    WHEN ('E', 'W') DO; PAYER_TYPE_PRIMARY  = '95'; PAYER_TYPE_SECONDARY  = '121'; END; 
    WHEN ('D') DO; PAYER_TYPE_PRIMARY  = '96'; PAYER_TYPE_SECONDARY  = '121'; END; 
    WHEN ('L') DO; PAYER_TYPE_PRIMARY  = '97'; PAYER_TYPE_SECONDARY  = '121'; END; 
  END; 

%mend;

%macro make_mapping_stus_ipsnfmp(srcvar, dispvar, stusvar);
  SELECT (&srcvar); 
    WHEN ('2','E') ADMITTING_SOURCE  = 'AV';
    WHEN ('7') ADMITTING_SOURCE  = 'ED'; 
    WHEN ('B','C') ADMITTING_SOURCE  = 'HH'; 
    WHEN ('1') ADMITTING_SOURCE  = 'HO';
    WHEN ('F') ADMITTING_SOURCE  = 'HS'; 
	WHEN ('D') ADMITTING_SOURCE  = 'IH';
    WHEN ('4') ADMITTING_SOURCE  = 'IP'; 
    WHEN ('3','6','8','A') ADMITTING_SOURCE  = 'OT';
    WHEN ('5') ADMITTING_SOURCE  = 'SN'; 
    WHEN ('0','9') ADMITTING_SOURCE  = 'UN'; 
    WHEN ('') ADMITTING_SOURCE   = 'NI'; 
  END;

  SELECT (&dispvar); 
    WHEN ('A') DISCHARGE_DISPOSITION  = 'A'; 
    WHEN ('B') DISCHARGE_DISPOSITION  = 'E'; 
    WHEN ('C') DISCHARGE_DISPOSITION  = 'OT';
    WHEN ('') DISCHARGE_DISPOSITION  = 'NI';
  END; 

  SELECT (&stusvar); 
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


/*Creates empty CDM table shells for unavailable tables*/ 
%macro create_cdm_tbl;

PROC SQL;
   create table pcordata.VITAL   
   (
	 VITALID            CHAR(2),
     PATID              CHAR(15),
	 ENCOUNTERID        CHAR(12),
	 MEASURE_DATE       NUM LENGTH = 8
                            INFORMAT=date9.   
                            FORMAT=date9.,
	 MEASURE_TIME       NUM LENGTH = 8
                            INFORMAT=TIME8.   
                            FORMAT=TIME8.,
     VITAL_SOURCE       CHAR(2),
     HT                 NUM,
     WT                 NUM,
     DIASTOLIC          NUM length = 8,
     SYSTOLIC           NUM length = 8,  
     ORIGINAL_BMI       NUM, 
     BP_POSITION        CHAR(2),
     SMOKING            CHAR(2),
     TOBACCO            CHAR(2),
     TOBACCO_TYPE       CHAR(2),
     RAW_DIASTOLIC      CHAR(3),
     RAW_SYSTOLIC       CHAR(3),
     RAW_BP_POSITION    CHAR(3),
	 RAW_SMOKING        CHAR(100),
     RAW_TOBACCO        CHAR(100),
     RAW_TOBACCO_TYPE   CHAR(100)
    );                                                               


   create table pcordata.LAB_RESULT_CM         
   (
     LAB_RESULT_CM_ID    CHAR(15),
     PATID               CHAR(15),
	 ENCOUNTERID         CHAR(12),
     SPECIMEN_SOURCE     CHAR(100),
     LAB_LOINC           CHAR(10),
	 LAB_RESULT_SOURCE   CHAR(2),
	 LAB_LOINC_SOURCE    CHAR(2),
     PRIORITY            CHAR(2),
     RESULT_LOC          CHAR(2),
     LAB_PX              CHAR(11),        
     LAB_PX_TYPE         CHAR(2),
     LAB_ORDER_DATE      NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     SPECIMEN_DATE       NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     SPECIMEN_TIME       NUM LENGTH = 8
                             INFORMAT=TIME8.   
                             FORMAT=TIME8.,
     RESULT_DATE         NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     RESULT_TIME         NUM LENGTH = 8
                             INFORMAT=TIME8.   
                             FORMAT=TIME8.,
     RESULT_QUAL         CHAR(12),
     RESULT_SNOMED       CHAR(100),
     RESULT_NUM          NUM, 
     RESULT_MODIFIER     CHAR(2),
     RESULT_UNIT         CHAR(11),
     NORM_RANGE_LOW      CHAR(10),
     NORM_MODIFIER_LOW   CHAR(2),
     NORM_RANGE_HIGH     CHAR(10),  
     NORM_MODIFIER_HIGH  CHAR(2),
     ABN_IND             CHAR(2),
     RAW_LAB_NAME        CHAR(40),
     RAW_LAB_CODE        CHAR(10),
     RAW_PANEL           CHAR(10),
     RAW_RESULT          CHAR(20),
     RAW_UNIT            CHAR(10),
     RAW_ORDER_DEPT      CHAR(10),
     RAW_FACILITY_CODE   CHAR(10)
    );

   create table pcordata.CONDITION  
    (
	 CONDITIONID         CHAR(15),
     PATID               CHAR(15),
	 ENCOUNTERID         CHAR(12),
     REPORT_DATE         NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     RESOLVE_DATE        NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     ONSET_DATE          NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     CONDITION_STATUS    CHAR(2),
     CONDITION           CHAR(18),
     CONDITION_TYPE      CHAR(2),
     CONDITION_SOURCE    CHAR(2),   
     RAW_CONDITION_STATUS CHAR(2),
     RAW_CONDITION        CHAR(18),
     RAW_CONDITION_TYPE   CHAR(2),
     RAW_CONDITION_SOURCE CHAR(2)
    ); 

   create table pcordata.PRO_CM  
    (
	 PRO_CM_ID           CHAR(15),
     PATID               CHAR(15),
	 ENCOUNTERID         CHAR(12),
     PRO_DATE            NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     PRO_TIME            NUM LENGTH = 8
                             INFORMAT=TIME8.   
                             FORMAT=TIME8.,
	 PRO_TYPE            CHAR(2),
     PRO_ITEM_NAME       CHAR(200),
     PRO_ITEM_LOINC      CHAR(10),
	 PRO_SOURCE          CHAR(2),
     PRO_RESPONSE_TEXT   CHAR(200),
     PRO_RESPONSE_NUM    NUM,
     PRO_METHOD          CHAR(2),
     PRO_MODE            CHAR(2),
     PRO_CAT             CHAR(2),
     PRO_ITEM_VERSION    CHAR(200),
     PRO_MEASURE_NAME    CHAR(200),
     PRO_MEASURE_SEQ     CHAR(200),
     PRO_MEASURE_SCORE   NUM,
     PRO_MEASURE_THETA   NUM,
     PRO_MEASURE_SCALED_TSCORE  NUM,
     PRO_MEASURE_STANDARD_ERROR NUM,
     PRO_MEASURE_COUNT_SCORED   NUM,
     PRO_MEASURE_LOINC   CHAR(10),
     PRO_MEASURE_VERSION CHAR(200),
     PRO_ITEM_FULLNAME   CHAR(200),
     PRO_ITEM_TEXT       CHAR(200),
     PRO_MEASURE_FULLNAME CHAR(200)
    ); 

	create table pcordata.PRESCRIBING
     (
	  PRESCRIBINGID      CHAR(15),
      PATID              CHAR(15),
	  ENCOUNTERID        CHAR(12),
      RX_PROVIDERID      CHAR(10),
      RX_ORDER_DATE      NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     RX_ORDER_TIME       NUM LENGTH = 8
                             INFORMAT=TIME8.   
                             FORMAT=TIME8.,
     RX_START_DATE       NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     RX_END_DATE         NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     RX_DOSE_ORDERED     NUM,
     RX_DOSE_ORDERED_UNIT CHAR(100),
     RX_QUANTITY         NUM,    
     RX_DOSE_FORM        CHAR(100),
     RX_REFILLS          NUM,
     RX_DAYS_SUPPLY      NUM,
     RX_FREQUENCY        CHAR(2),
     RX_PRN_FLAG         CHAR(1),
     RX_ROUTE            CHAR(100),
     RX_BASIS            CHAR(2),
     RXNORM_CUI          CHAR(8),
     RX_SOURCE           CHAR(2),
     RX_DISPENSE_AS_WRITTEN CHAR(2),
     RAW_RX_MED_NAME     CHAR(40),   
     RAW_RX_FREQUENCY    CHAR(20), 
     RAW_RXNORM_CUI      CHAR(15),
     RAW_RX_QUANTITY     CHAR(40),  
     RAW_RX_NDC          CHAR(100),
     RAW_RX_DOSE_ORDERED CHAR(40),
     RAW_RX_DOSE_ORDERED_UNIT CHAR(2),
     RAW_RX_ROUTE        CHAR(2),
     RAW_RX_REFILLS      CHAR(12)
    ); 
 
  create table pcordata.PCORNET_TRIAL
    (
	 PATID               CHAR(15),
     TRIALID             CHAR(20),
     PARTICIPANTID       CHAR(20),
     TRIAL_SITEID        CHAR(20),
     TRIAL_ENROLL_DATE   NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     TRIAL_END_DATE      NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     TRIAL_WITHDRAW_DATE NUM LENGTH = 8
                             INFORMAT=date9.   
                             FORMAT=date9.,
     TRIAL_INVITE_CODE   CHAR(20)
    ); 
  
  create table pcordata.DEATH_CAUSE
   ( 
    PATID                  CHAR(15),
    DEATH_CAUSE            CHAR(8),
    DEATH_CAUSE_CODE       CHAR(2),
    DEATH_CAUSE_TYPE       CHAR(2),
    DEATH_CAUSE_SOURCE     CHAR(2),
    DEATH_CAUSE_CONFIDENCE CHAR(2)
   ); 

  create table pcordata.MED_ADMIN 
   (
    MEDADMINID             CHAR(15),
    PATID                  CHAR(15),
	ENCOUNTERID            CHAR(12),
    PRESCRIBINGID          CHAR(15),
    MEDADMIN_PROVIDERID    CHAR(10),
    MEDADMIN_START_DATE    NUM LENGTH = 8
                               INFORMAT=date9.   
                               FORMAT=date9.,
    MEDADMIN_START_TIME    NUM LENGTH = 8
                               INFORMAT=TIME8.   
                               FORMAT=TIME8.,
    MEDADMIN_STOP_DATE     NUM LENGTH = 8
                               INFORMAT=date9.   
                               FORMAT=date9.,
    MEDADMIN_STOP_TIME     NUM LENGTH = 8
                               INFORMAT=TIME8.   
                               FORMAT=TIME8.,
    MEDADMIN_TYPE          CHAR(2),
    MEDADMIN_CODE          CHAR(100),
    MEDADMIN_DOSE_ADMIN    NUM,
    MEDADMIN_DOSE_ADMIN_UNIT CHAR(100),
    MEDADMIN_ROUTE         CHAR(100),
    MEDADMIN_SOURCE        CHAR(2),  
    RAW_MEDADMIN_MED_NAME  CHAR(100),
    RAW_MEDADMIN_CODE      CHAR(100),
    RAW_MEDADMIN_DOSE_ADMIN CHAR(100),
    RAW_MEDADMIN_DOSE_ADMIN_UNIT CHAR(100),
    RAW_MEDADMIN_ROUTE    CHAR(100)
   ); 

  create table pcordata.OBS_CLIN 
   (
    OBSCLINID              CHAR(15),
    PATID                  CHAR(15),
	ENCOUNTERID            CHAR(12),
    OBSCLIN_PROVIDERID     CHAR(10),
    OBSCLIN_DATE           NUM LENGTH = 8
                               INFORMAT=date9.   
                               FORMAT=date9.,
    OBSCLIN_TIME           NUM LENGTH = 8
                               INFORMAT=TIME8.   
                               FORMAT=TIME8.,
    OBSCLIN_TYPE           CHAR(2),
    OBSCLIN_CODE           CHAR(100),
	OBSCLIN_SOURCE         CHAR(2),
    OBSCLIN_RESULT_QUAL    CHAR(100),
    OBSCLIN_RESULT_TEXT    CHAR(100),
    OBSCLIN_RESULT_SNOMED  CHAR(100),
    OBSCLIN_RESULT_NUM     NUM,
    OBSCLIN_RESULT_MODIFIER CHAR(2),
    OBSCLIN_RESULT_UNIT    CHAR(100),
    RAW_OBSCLIN_NAME       CHAR(100),
    RAW_OBSCLIN_CODE       CHAR(100),
    RAW_OBSCLIN_TYPE       CHAR(2),
    RAW_OBSCLIN_RESULT     CHAR(100),
    RAW_OBSCLIN_MODIFIER   CHAR(100),
    RAW_OBSCLIN_UNIT       CHAR(100)
   ); 

  create table pcordata.OBS_GEN
   (
    OBSGENID               CHAR(15),
    PATID                  CHAR(15),
	ENCOUNTERID            CHAR(12),
    OBSGEN_PROVIDERID      CHAR(10),
    OBSGEN_DATE            NUM LENGTH = 8
                               INFORMAT=date9.   
                               FORMAT=date9.,
    OBSGEN_TIME            NUM LENGTH = 8
                               INFORMAT=TIME8.   
                               FORMAT=TIME8.,
    OBSGEN_TYPE            CHAR(30),
    OBSGEN_CODE            CHAR(100),  
    OBSGEN_SOURCE          CHAR(2),
    OBSGEN_RESULT_QUAL     CHAR(100),
    OBSGEN_RESULT_TEXT     CHAR(100),
    OBSGEN_RESULT_NUM      NUM,
    OBSGEN_RESULT_MODIFIER CHAR(2),
    OBSGEN_RESULT_UNIT     CHAR(100),
    OBSGEN_TABLE_MODIFIED  CHAR(3),
    OBSGEN_ID_MODIFIED     CHAR(100),
    RAW_OBSGEN_NAME        CHAR(100), 
    RAW_OBSGEN_CODE        CHAR(100),
    RAW_OBSGEN_TYPE        CHAR(100),
    RAW_OBSGEN_RESULT      CHAR(100),
    RAW_OBSGEN_UNIT        CHAR(100)
   ); 

   create table pcordata.IMMUNIZATION
   (
    IMMUNIZATIONID	 CHAR(15),
    PATID	         CHAR(15),
    ENCOUNTERID	     CHAR(12),
    PROCEDURESID	 CHAR(12),
    VX_PROVIDERID	 CHAR(10),
    VX_RECORD_DATE	 NUM,
    VX_ADMIN_DATE	 NUM,
    VX_CODE_TYPE	 CHAR(2),
    VX_CODE	         CHAR(10),
    VX_STATUS	     CHAR(2),
    VX_STATUS_REASON CHAR(2),
    VX_SOURCE        CHAR(2),
    VX_DOSE	         NUM,
    VX_DOSE_UNIT	 CHAR(10),
    VX_ROUTE	     CHAR(10),
    VX_BODY_SITE	 CHAR(10),
    VX_MANUFACTURER	 CHAR(10),
    VX_LOT_NUM	     CHAR(10),
    VX_EXP_DATE      NUM,
    RAW_VX_NAME	     CHAR(10),
    RAW_VX_CODE	     CHAR(10),
    RAW_VX_CODE_TYPE CHAR(10),
    RAW_VX_DOSE	     CHAR(10),
    RAW_VX_DOSE_UNIT CHAR(10),
    RAW_VX_ROUTE	 CHAR(10),
    RAW_VX_BODY_SITE CHAR(10),
    RAW_VX_STATUS	 CHAR(10),
    RAW_VX_STATUS_REASON CHAR(10),
    RAW_VX_MANUFACTURER	 CHAR(10)
   ); 

   create table pcordata.HASH_TOKEN	
    (
      PATID	   CHAR(15),
      TOKEN_01 CHAR(15),
      TOKEN_02 CHAR(15),
      TOKEN_03 CHAR(15),
      TOKEN_04 CHAR(15),
      TOKEN_05 CHAR(15),
      TOKEN_16 CHAR(15)
     );
QUIT;
 
%mend;


