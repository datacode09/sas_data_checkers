%macro detect_duplicates_recursive(libname, report_path);

    /* Step 1: Get all datasets from the specified library and subfolders */
    proc sql noprint;
        select catx('.', libname, memname) into :dataset_list separated by ' '
        from dictionary.tables
        where libname = upcase("&libname.");
    quit;

    /* Step 2: Initialize output dataset for storing statistics */
    data duplicate_stats;
        length Dataset $128 Record_Count 8 Duplicate_Count 8;
    run;

    /* Step 3: Loop through each dataset and calculate statistics */
    %let ds_count = %sysfunc(countw(&dataset_list));
    %do i = 1 %to &ds_count;
        %let dataset = %scan(&dataset_list, &i);

        /* Get total record count */
        proc sql noprint;
            select count(*) into :record_count
            from &dataset;
        quit;

        /* Sort dataset and identify duplicates */
        proc sort data=&dataset out=_sorted nodupkey dupout=_dupes;
            by _all_;
        run;

        /* Get duplicate count */
        proc sql noprint;
            select count(*) into :dup_count
            from _dupes;
        quit;

        /* Save statistics into a summary dataset */
        data duplicate_stats;
            set duplicate_stats;
            Dataset = "&dataset";
            Record_Count = &record_count;
            Duplicate_Count = &dup_count;
            output;
        run;

        /* Cleanup temporary datasets */
        proc datasets lib=work nolist;
            delete _sorted _dupes;
        quit;

    %end;

    /* Step 4: Create final report including all datasets */
    proc sql;
        create table duplicate_stats as
        select distinct Dataset, 
               coalesce(Record_Count, 0) as Record_Count,
               coalesce(Duplicate_Count, 0) as Duplicate_Count
        from duplicate_stats;
    quit;

    /* Step 5: Save the report in the specified path */
    proc export data=duplicate_stats
        outfile="&report_path./duplicate_stats_recursive.csv"
        dbms=csv replace;
    run;

    /* Print the report */
    proc print data=duplicate_stats noobs;
        title "Record and Duplicate Statistics Report (Recursive)";
    run;

%mend detect_duplicates_recursive;
