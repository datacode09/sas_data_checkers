%macro detect_duplicates_recursive(libpath, report_path);

    /* Step 1: Set up logging */
    filename logfile "&report_path./duplication_check.log";
    proc printto log=logfile new;
    run;

    %put %sysfunc(datetime(), datetime.) === Starting Duplication Detection Workflow ===;
    %put %sysfunc(datetime(), datetime.) Library Path: &libpath.;
    %put %sysfunc(datetime(), datetime.) Report Path: &report_path.;

    /* Step 2: Detect all subfolders */
    filename dirlist pipe "dir &libpath. /s /b /ad"; /* List subfolders only */
    data subfolders;
        length folder $256;
        infile dirlist truncover;
        input folder $256.;
        subfolder_id = _n_; /* Unique ID for each subfolder */
    run;
    filename dirlist clear;

    %put %sysfunc(datetime(), datetime.) Detected subfolders:;
    proc print data=subfolders; run;

    /* Step 3: Loop through each subfolder */
    data _null_;
        set subfolders;
        call execute(cats('%nrstr(%process_folder)(', quote(trim(folder)), ',', subfolder_id, ',', quote(trim("&report_path")), ');'));
    run;

    %put %sysfunc(datetime(), datetime.) === Completed Duplication Detection Workflow ===;

    /* Reset the log destination */
    proc printto;
    run;

%mend detect_duplicates_recursive;

/* Sub-Macro: Process Each Folder */
%macro process_folder(folder, subfolder_id, report_path);

    %put %sysfunc(datetime(), datetime.) Processing Folder: &folder.;

    /* Assign a dynamic libname for the subfolder */
    %let libname = sublib&subfolder_id;
    libname &libname "&folder.";

    /* Get all datasets in the subfolder */
    proc sql noprint;
        select catx('.', "&libname", memname) into :dataset_list separated by ' '
        from dictionary.tables
        where libname = upcase("&libname.");
    quit;

    %put %sysfunc(datetime(), datetime.) Datasets in &folder.: &dataset_list.;

    /* Initialize output dataset for storing statistics (first folder only) */
    %if %sysfunc(exist(duplicate_stats)) = 0 %then %do;
        data duplicate_stats;
            length Dataset $128 Record_Count 8 Duplicate_Count 8;
        run;
        %put %sysfunc(datetime(), datetime.) Created duplicate_stats dataset.;
    %end;

    /* Process each dataset */
    %let ds_count = %sysfunc(countw(&dataset_list));
    %do i = 1 %to &ds_count;
        %let dataset = %scan(&dataset_list, &i);
        %put %sysfunc(datetime(), datetime.) Processing Dataset: &dataset.;

        /* Get total record count */
        proc sql noprint;
            select count(*) into :record_count
            from &dataset;
        quit;
        %put %sysfunc(datetime(), datetime.) Record Count for &dataset.: &record_count.;

        /* Sort dataset and identify duplicates */
        proc sort data=&dataset out=_sorted nodupkey dupout=_dupes;
            by _all_;
        run;

        /* Get duplicate count */
        proc sql noprint;
            select count(*) into :dup_count
            from _dupes;
        quit;
        %put %sysfunc(datetime(), datetime.) Duplicate Count for &dataset.: &dup_count.;

        /* Append to summary dataset */
        data duplicate_stats;
            set duplicate_stats;
            Dataset = "&dataset";
            Record_Count = &record_count;
            Duplicate_Count = &dup_count;
            output;
        run;

        /* Cleanup */
        proc datasets lib=work nolist;
            delete _sorted _dupes;
        quit;
    %end;

    %put %sysfunc(datetime(), datetime.) Finished processing folder: &folder.;

    /* Clear libname */
    libname &libname clear;

%mend process_folder;
