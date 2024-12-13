%macro detect_duplicates(libname, export_path);

    /* Step 1: Get all dataset names in the specified library */
    %put NOTE: Retrieving all dataset names from library "&libname.";
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
    %put NOTE: Found &ds_count datasets in library "&libname.";

    /* Step 2: Generate a timestamp for the filename */
    %let timestamp = %sysfunc(putn(%sysfunc(datetime()), datetime20.), yymmddn8.)%sysfunc(putn(%sysfunc(datetime()), time8.), hhmmss6.);
    %put NOTE: Generated timestamp for report: &timestamp.;

    /* Step 3: Initialize output dataset for storing duplicate statistics */
    %put NOTE: Initializing the duplicate statistics output dataset.;
    data duplicate_stats;
        length Dataset $32 Record_Count 8 Duplicate_Count 8;
    run;

    /* Step 4: Loop through each dataset and calculate duplicates */
    %do i = 1 %to &ds_count;
        %let dataset = %scan(&dataset_list, &i);
        %put NOTE: Processing dataset &i of &ds_count: &dataset.;

        /* Calculate total record count */
        %put NOTE: Calculating record count for dataset "&dataset.";
        proc sql noprint;
            select count(*) into :record_count
            from &libname..&dataset;
        quit;
        %put NOTE: Record count for dataset "&dataset.": &record_count.;

        /* Sort dataset and identify duplicates */
        %put NOTE: Sorting dataset "&dataset." and identifying duplicates.;
        proc sort data=&libname..&dataset out=_sorted nodupkey dupout=_dupes;
            by _all_;
        run;

        /* Check if _dupes exists and calculate duplicate count */
        %if %sysfunc(exist(_dupes)) %then %do;
            proc sql noprint;
                select count(*) into :dup_count
                from _dupes;
            quit;
            %put NOTE: Duplicate count for dataset "&dataset.": &dup_count.;
        %end;
        %else %do;
            %let dup_count = 0;
            %put NOTE: No duplicates found for dataset "&dataset.";
        %end;

        /* Append statistics to the summary dataset */
        %put NOTE: Appending statistics for dataset "&dataset." to the output dataset.;
        data duplicate_stats;
            set duplicate_stats;
            Dataset = "&dataset";
            Record_Count = &record_count;
            Duplicate_Count = &dup_count;
            output;
        run;

        /* Cleanup temporary datasets */
        %put NOTE: Cleaning up temporary datasets for "&dataset.";
        proc datasets lib=work nolist;
            delete _sorted _dupes;
        quit;

    %end;

    /* Step 5: Save the report to the specified path with timestamp in the filename */
    %put NOTE: Saving the duplicate statistics report to "&export_path./duplicate_stats_&timestamp..csv";
    proc export data=duplicate_stats
        outfile="&export_path./duplicate_stats_&timestamp..csv"
        dbms=csv replace;
    run;

    /* Print the report */
    %put NOTE: Printing the duplicate statistics report.;
    proc print data=duplicate_stats noobs;
        title "Duplicate Statistics Report";
    run;

    %put NOTE: Macro execution completed successfully. Report generated: &export_path./duplicate_stats_&timestamp..csv.;

%mend detect_duplicates;
