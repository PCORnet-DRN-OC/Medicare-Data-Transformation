
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                      
* Program Name:  pcncdm_formats.sas                          
*         Date:  08/04/2017                                               
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to store output templates 
*           and formats of variables.
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;

ODS PATH RESET;                              
ODS PATH (PREPEND) WORK.Templat(UPDATE);  
ODS NOPROCTITLE;
PROC TEMPLATE;                               
  EDIT Base.Freq.OneWayList;                 
    EDIT Frequency;                          
      FORMAT = COMMA12.;                      
    END;                                     
    EDIT CumFrequency;                       
      FORMAT = COMMA12.;                      
    END;                                     
    EDIT Percent;                            
      FORMAT = 5.1;                          
    END;                                     
    EDIT CumPercent;                         
      FORMAT = 5.1;                          
    END;                                     
  END;                                       
    
  DEFINE STYLE  styles.PCORNET_CDMTL;
         parent=styles.printer;
           replace fonts /
        'TitleFont'           = ("Times",12pt)
        'TitleFont2'          = ("Times",10pt)        
        'StrongFont'          = ("Times",10pt)
        'EmphasisFont'        = ("Times",10pt,Bold)
        'FixedEmphasisFont'   = ("Courier New,Courier",9pt)
        'FixedStrongFont'     = ("Courier New,Courier",9pt)
        'FixedHeadingFont'    = ("Courier New,Courier",9pt)
        'BatchFixedFont'      = ("SAS Monospace,Courier New,Courier",9pt)
        'FixedFont'           = ("Courier New,Courier",9pt)
        'headingEmphasisFont' = ("Times",10pt)
        'headingFont'         = ("Times",10pt)
        'docFont'             = ("Times",9pt)
        'FootnoteFont'        = ("Times",9pt);

		replace color_list                                                      
            "Colors used in the default style" /                                 
            'link' = blue                                                        
            'bgH' = _undef_ 
            'fg' = dark black                                                        
            'bg' = _undef_ ;
   END;
RUN;

PROC FORMAT;
  value msck
        . = "MISSING"
        other = "NON-MISSING";


   value $msck
         " " = "MISSING"
         other = "NON-MISSING";
RUN;


