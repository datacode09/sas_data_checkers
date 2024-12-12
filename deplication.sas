%macro detect_duplicates(libname);
    /* Step 1: Get all dataset names in the specified library */
    proc sql noprint;
        select memname into :dataset_list separated by ' ' 
        from dictionary.tables
        where libname = upcase("&libname.");
    quit;

    /* Step 2: Initialize output dataset for storing duplicate statistics */
    data duplicate_stats;
        length Dataset $32 Duplicate_Count 8;
    run;

    /* Step 3: Loop through each dataset and calculate duplicates */
    %let ds_count = %sysfunc(countw(&dataset_list));
    %do i = 1 %to &ds_count;
        %let dataset = %scan(&dataset_list, &i);

        /* Sort dataset and identify duplicates */
        proc sort data=&libname..&dataset out=_sorted nodupkey dupout=_dupes;
            by _all_;
        run;

        /* Calculate the number of duplicate rows */
        proc sql noprint;
            select count(*) into :dup_count
            from _dupes;
        quit;

        /* Save statistics into a summary dataset */
        data duplicate_stats;
            set duplicate_stats;
            Dataset = "&dataset";
            Duplicate_Count = &dup_count;
            output;
        run;

        /* Cleanup temporary datasets */
        proc datasets lib=work nolist;
            delete _sorted _dupes;
        quit;

    %end;

    /* Step 4: Save the report in the specified library */
    proc export data=duplicate_stats
        outfile="&libname.\duplicate_stats.csv"
        dbms=csv replace;
    run;

    /* Print the report */
    proc print data=duplicate_stats noobs;
        title "Duplicate Statistics Report";
    run;

%mend detect_duplicates;

