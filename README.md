# Medicare to PCORnet Data Transformation Package
### *Version 4.0*

### Purpose

The purpose of the Data Transformation Program package is to transform Medicare Research Identifiable files (RIF) to the PCORnet Common Data Model (CDM) v5.1 to facilitate and standardize the use of Medicare data in conducting PCORnet studies. Documentation about the CDM is available at http://pcornet.org/pcornet-common-data-model/. 

The focus of the current Data Transformation Programs is to transform the following Medicare RIF files: Master Beneficiary Summary File - Base (A/B/D); Inpatient Institutional Claims; Outpatient Institutional Claims; Carrier (i.e., Physician/Supplier) Claims; Part D Prescription Drug Events, Skilled Nursing Facility (SNF), and Medicare Provider Analysis and Review (MedPar). The MedPAR file is an alternate source of information about inpatient hospital and skilled nursing facility (SNF) care, users can transform either the MedPAR file or Inpatient Claims and Skilled Nursing Facility Claims, but not both, to the PCORnet Common Data Model. Other claim type files exist (e.g., DME, Home Health) and may be added to this package in the future. The Medicare data are able to populate the following PCORnet CDM tables: Demographic, Enrollment, Encounter, Diagnosis, Procedures, Dispensing, Death, Provider, and LDS_Address_History. 

### System Requirements

This code is designed to run in SAS versions 9.3 or higher.

### Acknowledgments

This code package was developed by members of the PCORnet Coordinating Center team at Duke Clinical Research Institute and tested by the following PCORnet network partners: Kansas University Medical Center, New York City, and Vanderbilt.  

