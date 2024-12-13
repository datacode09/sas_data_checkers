%macro detect_duplicates(libname, export_path);

    /* Step 1: Get all dataset names in the specified library */
    proc sql noprint;
        select memname into :dataset_list separated by ' '
        from dictionary.tables
        where libname = upcase("&libname.");
        %let ds_count = &sqlobs; /* Count of datasets */
    quit;

    /* Check if there are any datasets in the library */
    %if &ds_count = 0 %then %do;
        %put ERROR: No datasets found in the library "&libname.";
        %return;
    %end;

    /* Step 2: Generate a timestamp for the filename */
    %let timestamp = %sysfunc(datetime(), datetime20.);
    %let timestamp = %sysfunc(compress(&timestamp, ' :-', 'kd')); /* Remove special characters */

    /* Step 3: Initialize output dataset for storing duplicate statistics */
    data duplicate_stats;
        length Dataset $32 Record_Count 8 Duplicate_Count 8 Timestamp $20;
        format Timestamp $20.;
    run;

    /* Step 4: Loop through each dataset and calculate duplicates */
    %do i = 1 %to &ds_count;
        %let dataset = %scan(&dataset_list, &i);

        /* Calculate total record count */
        proc sql noprint;
            select count(*) into :record_count
            from &libname..&dataset;
        quit;

        /* Sort dataset and identify duplicates */
        proc sort data=&libname..&dataset out=_sorted nodupkey dupout=_dupes;
            by _all_;
        run;

        /* Check if _dupes exists and calculate duplicate count */
        %if %sysfunc(exist(_dupes)) %then %do;
            proc sql noprint;
                select count(*) into :dup_count
                from _dupes;
            quit;
        %end;
        %else %do;
            %let dup_count = 0;
        %end;

        /* Append statistics to the summary dataset */
        data duplicate_stats;
            set duplicate_stats;
            Dataset = "&dataset";
            Record_Count = &record_count;
            Duplicate_Count = &dup_count;
            Timestamp = %sysfunc(datetime(), datetime20.);
            output;
        run;

        /* Cleanup temporary datasets */
        proc datasets lib=work nolist;
            delete _sorted _dupes;
        quit;

    %end;

    /* Step 5: Save the report to the specified path with timestamp in the filename */
    proc export data=duplicate_stats
        outfile="&export_path./duplicate_stats_&timestamp..csv"
        dbms=csv replace;
    run;

    /* Print the report */
    proc print data=duplicate_stats noobs;
        title "Duplicate Statistics Report";
    run;

%mend detect_duplicates;
