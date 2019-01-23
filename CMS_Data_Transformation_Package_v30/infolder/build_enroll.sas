/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *                                                                     
* Program Name:  build_enroll.sas                          
*         Date:  12/15/2017                                                
*        Study:  PCORnet CMS Linkage 
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Purpose:  The purpose of the program is to store macros used in ETL enrollment program                
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */;

%macro partdelig(inds,idvar, yr);
    
  data den;
	set &inds;

	length elig $ 13;

	elig = "-------------";

	do i = 1 to 12;
	   substr(elig, i, 1) = (substr(lowcase(plnind),i,1) in 
                              %if %cmpres(&yr)>= 2010 %then %do; ("s","e","r","x") %end;
                             %else %do;
							  ("s","e","r")
                             %end;);
	end;
  run;

  /* Create in/out of eligibility for analysis */;
  data allinout;
	set den;

	eligible = .;
	first_dt = .;
	last_dt = .;
	
	do i = 1 to 13;
		_m = mod(i, 12);
		_d = 1;
		_y = int(i/12) + &yr;

		if _m = 0 then do;
			_m = 12;
			_y = _y - 1;
		end;

		this_dt = mdy(_m, _d, _y);

		currelig = substr(elig, i, 1);

		if currelig eq "-" then do;
			if not missing(first_dt) and eligible = 1 then do;
				last_dt = this_dt - 1;
				output;
				first_dt = .;
				last_dt = .;
				eligible = .;
			end;
		end;
		else do; /* currelig ne "-" */; 
			if currelig ne eligible then do;
				if missing(first_dt) and currelig = 1 then do;
					first_dt = mdy(_m, _d, _y);
					eligible = currelig;
				end;
				else do;
					last_dt = this_dt - 1;
					output;
					first_dt = this_dt;
					last_dt = .;
					eligible = currelig;
				end;
			end;
		end;
    end;

    keep &IDVAR first_dt last_dt eligible;
    format first_dt last_dt date9.;
  run;

  data &outds;
	set allinout;
	where eligible = 1;
  run;

%mend;

%macro ptdelig(inds,outds,yr);
 
  proc sort data=&inds out = denom_oneyr noduprecs;
    by _all_ ;
  run;
    
  data denom_oneyr;
    length plnind $12; 
    set denom_oneyr;
    array cntrctid {12} $5 CNTRCT01-CNTRCT12;
    plnind = 'NNNNNNNNNNNN';
    do i = 1 to 12;
      if cntrctid{i} ne '' then do;
        substr(plnind, i, 1) = substr(cntrctid{i},1,1);
      end;
    end;
  run;
     
  %partdelig(denom_oneyr, bene_id, &yr);
 
%mend;

%macro ptdenomelig_append(baseds, newds, fullds, idvar);

  /* Concat datasets and set flag indicating new data */;
  data allelig;
	set
		&baseds
		&newds(in = innew keep=&idvar first_dt last_dt);
		
	if innew then 
		new = 1;
	else 
		new = 0;
  run;

  /*******************************************************************************
  * Sort by &idvar and date and sort into EASY and DIFFICULT records. "Difficult" 
  * are the last-old and first-new records from pts with both old and new data;
  ********************************************************************************/
  proc sort data=allelig;
    by &idvar new;
  run;

  data 
	easy (drop=new)
	difficult (drop=new);
	set allelig;
	by &idvar new;

	if first.&idvar and last.&idvar then do;
		if new then
			firstelig = 1;
		output easy;
	end;
	else if not new and not last.new then
		output easy;
	else if new and not first.new then do;
		firstelig = 0;
		output easy;
	end;
	else if not new and last.&idvar then
		output easy;
	else if new and first.&idvar then do;
		firstelig = 1;	
		output easy;
	end;
	else
		output difficult;
  run;

  /* Merge the difficult records if necessary */;
  data difficult;
	set difficult;
	by &idvar;
	
	retain first_dt_1 last_dt_1 firstelig_1;
	
	if first.&idvar then do;
		first_dt_1  = first_dt;
		last_dt_1   = last_dt;
		firstelig_1 = firstelig;
	end;

	if last.&idvar then do;
		first_dt_2  = first_dt;
		last_dt_2   = last_dt;
		firstelig_2 = 0;
		
		diff = intck("dtday", last_dt_1, first_dt_2);
	
		if diff > 1 then do;
			first_dt  = first_dt_1;
			last_dt   = last_dt_1;
			firstelig = firstelig_1;
			output;

			first_dt  = first_dt_2;
			last_dt   = last_dt_2;
			firstelig = firstelig_2;
			output;
		end;
		else do;
                     
			first_dt  = first_dt_1;
			last_dt   = last_dt_2;
			firstelig = firstelig_1;
			output;

                end;
	end;

	keep &idvar first_dt last_dt firstelig;
  run;

  /* Merge final records back together */;
  data denomelig;
	set easy
		difficult;
  run;

  /* Sort by &IDVAR and date and save */;
  proc sort data=denomelig nodups out=&fullds;
	by &idvar first_dt;
  run;
%mend;

%macro denomelig(inds, outds, idvar, yr);
  data den;
	set &inds;

	length elig $ 13;

	elig = "-------------";

	do i = 1 to 12;
		substr(elig, i, 1) = (
			substr(BUYIN, i, 1) in ("3", "C") and
			substr(HMOIND, i, 1) = "0"
		);
	end;
  run;

  /* Create in/out of eligibility for analysis */;
  data allinout;
	set den;

	eligible = .;
	first_dt = .;
	last_dt = .;
	
	do i = 1 to 13;
		_m = mod(i, 12);
		_d = 1;
		_y = int(i/12) + &yr;

		if _m = 0 then do;
		   _m = 12;
		   _y = _y - 1;
		end;

		this_dt = mdy(_m, _d, _y);

		currelig = substr(elig, i, 1);

		if currelig eq "-" then do;
			if not missing(first_dt) and eligible = 1 then do;
				last_dt = this_dt - 1;
				output;
				first_dt = .;
				last_dt = .;
				eligible = .;
			end;
		end;
		else do; /* currelig ne "-" */; 
			if currelig ne eligible then do;
				if missing(first_dt) and currelig = 1 then do;
					first_dt = mdy(_m, _d, _y);
					eligible = currelig;
				end;
				else do;
					last_dt = this_dt - 1;
					output;
					first_dt = this_dt;
					last_dt = .;
					eligible = currelig;
				end;
			end;
		end;
	end;

	keep &IDVAR first_dt last_dt eligible;
	format first_dt last_dt date9.;
  run;

  data &outds;
	set allinout;
	where eligible = 1;
  run;
%mend;

%macro denomelig_append(baseds,newds, fullds, idvar);

  /* Concat datasets and set flag indicating new data */;
  data allelig;
	set
		&baseds
		&newds(in = innew keep=&idvar first_dt last_dt);
		
	if innew then 
		new = 1;
	else 
		new = 0;
  run;

  /*******************************************************************************
  * Sort by &idvar and date and sort into EASY and DIFFICULT records. "Difficult" 
  * are the last-old and first-new records from pts with both old and new data;
  ********************************************************************************/

  proc sort data=allelig;
	by &idvar new;
  run;

  data 
	easy (drop=new)
	difficult (drop=new);
	set allelig;
	by &idvar new;

	if first.&idvar and last.&idvar then do;
		if new then
			firstelig = 1;
		output easy;
	end;
	else if not new and not last.new then
		output easy;
	else if new and not first.new then do;
		firstelig = 0;
		output easy;
	end;
	else if not new and last.&idvar then
		output easy;
	else if new and first.&idvar then do;
		firstelig = 1;	
		output easy;
	end;
	else
		output difficult;
  run;

  /* Merge the difficult records if necessary */;
  proc sort data=difficult;
	by &idvar first_dt;
  run;

  data difficult;
	set difficult;
	by &idvar;
	
	retain first_dt_1 last_dt_1 firstelig_1;
	
	if first.&idvar then do;
		first_dt_1  = first_dt;
		last_dt_1   = last_dt;
		firstelig_1 = firstelig;
	end;

	if last.&idvar then do;
		first_dt_2  = first_dt;
		last_dt_2   = last_dt;
		firstelig_2 = 0;
		
		diff = intck("dtday", last_dt_1, first_dt_2);
	
		if diff > 1 then do;
			first_dt  = first_dt_1;
			last_dt   = last_dt_1;
			firstelig = firstelig_1;
			output;

			first_dt  = first_dt_2;
			last_dt   = last_dt_2;
			firstelig = firstelig_2;
			output;
		end;
		else do;
                     
			first_dt  = first_dt_1;
			last_dt   = last_dt_2;
			firstelig = firstelig_1;
			output;

        end;
	end;

	keep &idvar first_dt last_dt firstelig;
  run;

  /* Merge final records back together */;
  data denomelig;
	set
		easy
		difficult;
  run;

  /* Sort by &IDVAR and date and save */;
  proc sort data=denomelig out=&fullds;
	by &idvar first_dt;
  run;
%mend;



