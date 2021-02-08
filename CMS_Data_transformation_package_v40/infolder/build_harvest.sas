
proc sql;
  create table pcordata.harvest as
    select distinct 
     "&NETID" as NETWORKID length =10 informat = $10. format = $10.,	
     "&NETNM" as NETWORK_NAME length =20 informat = $20. format = $20.,	
     "&DMID" as DATAMARTID length =10 informat = $10. format = $10.,		
     "&DMNM" as DATAMART_NAME length =20 informat = $20. format = $20.,		
     "&DMPLFM" as DATAMART_PLATFORM	length =2 informat = $2. format = $2.,	
     4.1 as CDM_VERSION length =8 informat = 8.1 format = 8.1,
     "02" as DATAMART_CLAIMS length =2 informat = $2. format = $2.,
     "01" as DATAMART_EHR length =2 informat = $2. format = $2.,
     "01" as BIRTH_DATE_MGMT length =2 informat = $2. format = $2.,
     "01" as ENR_START_DATE_MGMT length =2 informat = $2. format = $2.,
     "01" as ENR_END_DATE_MGMT length =2 informat = $2. format = $2.,
     "01" as ADMIT_DATE_MGMT length =2 informat = $2. format = $2.,
     "01" as DISCHARGE_DATE_MGMT length =2 informat = $2. format = $2.,
	 "01" as DX_DATE_MGMT length =2 informat = $2. format = $2.,
     "01" as PX_DATE_MGMT length =2 informat = $2. format = $2.,
     "" as RX_ORDER_DATE_MGMT	length =2 informat = $2. format = $2.,
     "" as RX_START_DATE_MGMT	length =2 informat = $2. format = $2.,
     "" as RX_END_DATE_MGMT	length =2 informat = $2. format = $2.,
     "01" as DISPENSE_DATE_MGMT	length =2 informat = $2. format = $2.,
     "" as LAB_ORDER_DATE_MGMT length =2 informat = $2. format = $2.,
     "" as SPECIMEN_DATE_MGMT	length =2 informat = $2. format = $2.,
     "" as RESULT_DATE_MGMT	length =2 informat = $2. format = $2.,
     "" as MEASURE_DATE_MGMT	length =2 informat = $2. format = $2.,
     "" as ONSET_DATE_MGMT	length =2 informat = $2. format = $2.,
     "" as REPORT_DATE_MGMT	length =2 informat = $2. format = $2.,
	 "" as RESOLVE_DATE_MGMT length =2 informat = $2. format = $2.,
     "" as PRO_DATE_MGMT	length =2 informat = $2. format = $2.,
	 "" as DEATH_DATE_MGMT length =2 informat = $2. format = $2.,
	 "" as MEDADMIN_START_DATE_MGMT length =2 informat = $2. format = $2.,
	 "" as MEDADMIN_STOP_DATE_MGMT length =2 informat = $2. format = $2.,
	 "" as OBSCLIN_DATE_MGMT length =2 informat = $2. format = $2.,
	 "" as OBSGEN_DATE_MGMT length =2 informat = $2. format = $2.,
	 "02" as ADDRESS_PERIOD_START_MGMT length =2 informat = $2. format = $2.,
     "02" as ADDRESS_PERIOD_END_MGMT length =2 informat = $2. format = $2.,
     "" as VX_RECORD_DATE_MGMT length =2 informat = $2. format = $2.,
     "" as VX_ADMIN_DATE_MGMT length =2 informat = $2. format = $2.,
     "" as VX_EXP_DATE_MGMT length =2 informat = $2. format = $2.,
     . as REFRESH_DEMOGRAPHIC_DATE length =8 informat = date9. format = date9.,	                                                        
     . as REFRESH_ENROLLMENT_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_ENCOUNTER_DATE length =8 informat = date9. format = date9.,
     . as REFRESH_DIAGNOSIS_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_PROCEDURES_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_VITAL_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_DISPENSING_DATE length =8 informat = date9. format = date9.,
     . as REFRESH_LAB_RESULT_CM_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_CONDITION_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_PRO_CM_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_PRESCRIBING_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_PCORNET_TRIAL_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_DEATH_DATE length =8 informat = date9. format = date9.,	
     . as REFRESH_DEATH_CAUSE_DATE length =8 informat = date9. format = date9.,
	 . as REFRESH_MED_ADMIN_DATE length =8 informat = date9. format = date9.,
	 . as REFRESH_OBS_CLIN_DATE length =8 informat = date9. format = date9.,
	 . as REFRESH_PROVIDER_DATE length =8 informat = date9. format = date9.,
	 . as REFRESH_OBS_GEN_DATE length =8 informat = date9. format = date9.,
	 . as REFRESH_HASH_TOKEN_DATE length =8 informat = date9. format = date9.,
     . as REFRESH_LDS_ADDRESS_HX_DATE length =8 informat = date9. format = date9.,
     . as REFRESH_IMMUNIZATION_DATE length =8 informat = date9. format = date9.
	 from lib_den.&MBSFTBL.&startyr;	
quit;

    