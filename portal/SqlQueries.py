
def allqueries(startdate, enddate, selected_provinces, pepfar_support):
    parameters = (startdate, enddate)
    
            
    SQLqueries = [
        {
            'query_name': 'DistrictISTResults',
            'sql': """
                    SELECT  
                    District_,
                            SUM(CAST([vl_plasma_sam] AS int) ) [vl_plasma_sam] ,
                            SUM(CAST([vl_dbs_sam] AS int)) [vl_dbs_sam],
                            SUM(CAST([eid_sam] AS int)) [eid_sam] ,
                            SUM(CAST([sputum_sam] AS int)) [sputum_sam],
                            SUM(CAST([urine_sam] AS int)) [urine_sam],
                            SUM(CAST([Other_samples_Cleaned_] AS int)) [Other_samples_Cleaned_], 
                            SUM(CAST([vl_plasma_sam] AS int) )+
                            SUM(CAST([vl_dbs_sam] AS int))+
                            SUM(CAST([eid_sam] AS int))  +
                            SUM(CAST([sputum_sam] AS int))+
                            SUM(CAST([urine_sam] AS int)) +
                            SUM(CAST([Other_samples_Cleaned_] AS int))   Overal_Transported,

                            SUM(CAST([vl_plasma_res] AS int) ) [vl_plasma_res] ,
                            SUM(CAST([vl_dbs_res] AS int)) [vl_dbs_res],
                            SUM(CAST([eid_res] AS int)) [eid_res] ,
                            SUM(CAST([sputum_res] AS int)) [sputum_res],
                            SUM(CAST([urine_res] AS int)) [urine_res],
                            SUM(CAST([Other_Results_cleaned_] AS int)) [Other_Results_cleaned_],    
                            
                            SUM(CAST([vl_plasma_res] AS int) )  +
                            SUM(CAST([vl_dbs_res] AS int)) +
                            SUM(CAST([eid_res] AS int))  +
                            SUM(CAST([sputum_res] AS int)) +
                            SUM(CAST([urine_res] AS int)) +
                            SUM(CAST([Other_Results_cleaned_] AS int))  Overal_results
                        FROM [LSS].[dbo].[IST_National]
                        WHERE [Date] >= %s AND [Date] <= %s
                        

                """,
                       'parameters': (startdate, enddate)
            },
        
        {
            'query_name': 'OveralSampleTotals',
            'sql': """
                        SELECT  
                            SUM(CAST([vl_plasma_sam] AS int) ) [vl_plasma_sam] ,
                            SUM(CAST([vl_dbs_sam] AS int)) [vl_dbs_sam],
                            SUM(CAST([eid_sam] AS int)) [eid_sam] ,
                            SUM(CAST([sputum_sam] AS int)) [sputum_sam],
                            SUM(CAST([urine_sam] AS int)) +
                            SUM(CAST([Other_samples_Cleaned_] AS int)) OtherSam,
        
                            SUM(CAST([vl_plasma_sam] AS int) )+
                            SUM(CAST([vl_dbs_sam] AS int))+
                            SUM(CAST([eid_sam] AS int))  +
                            SUM(CAST([sputum_sam] AS int))+
                            SUM(CAST([urine_sam] AS int)) +
                            SUM(CAST([Other_samples_Cleaned_] AS int))   Overal_Transported,

                            SUM(CAST([vl_plasma_res] AS int) ) [vl_plasma_res] ,
                            SUM(CAST([vl_dbs_res] AS int)) [vl_dbs_res],
                            SUM(CAST([eid_res] AS int)) [eid_res] ,
                            SUM(CAST([sputum_res] AS int)) [sputum_res],
                            SUM(CAST([urine_res] AS int)) +
                            SUM(CAST([Other_Results_cleaned_] AS int)) OtherRes,    
                            
                            SUM(CAST([vl_plasma_res] AS int) )  +
                            SUM(CAST([vl_dbs_res] AS int)) +
                            SUM(CAST([eid_res] AS int))  +
                            SUM(CAST([sputum_res] AS int)) +
                            SUM(CAST([urine_res] AS int)) +
                            SUM(CAST([Other_Results_cleaned_] AS int))  Overal_results
                        FROM [LSS].[dbo].[IST_National]
                        WHERE [Date] >= %s AND [Date] <= %s
                         """ ,
                         'parameters': (startdate, enddate)
            },
        
        
        
        {
            'query_name': 'TrendSpecimenResults',
            'sql': """
                      SELECT  
                            substring([date],1,10) AS [month],
                            SUM(CAST([vl_plasma_sam] AS int) ) AS [vl_plasma_sam] ,
                            SUM(CAST([vl_dbs_sam] AS int))  AS [vl_dbs_sam],
                            SUM(CAST([eid_sam] AS int)) AS [eid_sam] ,
                            SUM(CAST([sputum_sam] AS int)) AS [sputum_sam],
                            SUM(CAST([urine_sam] AS int))+SUM(CAST([Other_samples_Cleaned_] AS int)) AS OtherSam,
        
                            SUM(CAST([vl_plasma_sam] AS int) )+
                            SUM(CAST([vl_dbs_sam] AS int))+
                            SUM(CAST([eid_sam] AS int))  +
                            SUM(CAST([sputum_sam] AS int))+
                            SUM(CAST([urine_sam] AS int)) +
                            SUM(CAST([Other_samples_Cleaned_] AS int))  AS  Overal_Transported,

                            SUM(CAST([vl_plasma_res] AS int) ) AS  [vl_plasma_res] ,
                            SUM(CAST([vl_dbs_res] AS int)) AS [vl_dbs_res],
                            SUM(CAST([eid_res] AS int))  AS [eid_res] ,
                            SUM(CAST([sputum_res] AS int)) AS [sputum_res],
                            SUM(CAST([urine_res] AS int)) + SUM(CAST([Other_Results_cleaned_] AS int)) AS OtherRes,     
                            
                            SUM(CAST([vl_plasma_res] AS int) )  +
                            SUM(CAST([vl_dbs_res] AS int)) +
                            SUM(CAST([eid_res] AS int))  +
                            SUM(CAST([sputum_res] AS int)) +
                            SUM(CAST([urine_res] AS int)) +
                            SUM(CAST([Other_Results_cleaned_] AS int)) AS Overal_results
                            FROM [LSS].[dbo].[IST_National]
                            WHERE [Date] >= %s AND [Date] <= %s
                                                
                         """ ,
                       'parameters': (startdate, enddate)

            },

            {
            'query_name': 'CLIanalysis',
            'sql': """

                SELECT DISTINCT
                    SUM([UNRESOLVED_Pending_testing]) [UNRESOLVED_Pending_testing]
                    ,SUM([UNRESOLVED_Referred_awaiting_results]) [UNRESOLVED_Referred_awaiting_results]
                    ,SUM([UNRESOLVED_Investigation_in_progress]) [UNRESOLVED_Investigation_in_progress]

                    ,SUM([RESOLVED_Specimens_rejected]) [RESOLVED_Specimens_rejected]
                    ,SUM([RESOLVED_LIMS_Interface_Failed]) LIMS_Interface_Failed
                    ,SUM([RESOLVED_Results_not_documented_at_facility]) not_documented
                    ,SUM([RESOLVED_Result_pending_publishing]) pending_publishing
                    ,SUM([RESOLVED_Results_not_yet_dispatched_printed]) not_yet_dispatched_printed
                    ,SUM([RESOLVED_Results_sent_to_wrong_facility] ) Results_wrong_facility
                    ,SUM([RESOLVED_Specimens_not_received_rebleed_sent]) Specimens_not_received

                    ,SUM([RESOLVED_Specimens_rejected])+
                    SUM([RESOLVED_LIMS_Interface_Failed])+
                    SUM([RESOLVED_Results_not_documented_at_facility])+
                    SUM([RESOLVED_Result_pending_publishing])+
                    SUM([RESOLVED_Results_not_yet_dispatched_printed])+
                    SUM([RESOLVED_Results_sent_to_wrong_facility] )+
                    SUM([RESOLVED_Specimens_not_received_rebleed_sent]) RESOLVED

                FROM [LSS].[dbo].[Dash_CLI] c, Dashboard_Indicators l
                WHERE c.[SourceFile]=l.[SourceFile]
                and c.Update_date=l.Update_Date   
                and 
                   CAST(l.[date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                   AND CAST(l.[date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                                     
                         """ ,
                       'parameters': ()
            },
            
            {
            'query_name': 'CLI_Unresolved_Resolved',
            'sql': """
                SELECT cast(l.[Date] as date) AS date, 
                             SUM( [UNRESOLVED_Pending_testing]+
                                [UNRESOLVED_Referred_awaiting_results]+
                                [UNRESOLVED_Investigation_in_progress] ) AS unresolved        
                        ,sum([RESOLVED_Missing_results_outcomes_found_Shared_with_facilities]) resolved
                FROM [LSS].[dbo].[Dash_CLI] c, Dashboard_Indicators l
                        Where c.[SourceFile]=l.[SourceFile]
                        and c.Update_date=l.Update_Date
                group by cast(l.[Date] as date)
                order by cast(l.[Date] as date)
                """,
                       'parameters': ()
            },



           
            
            
            
            
            
           

                #  {
                # 'query_name': 'RTCQM_percentage_Score_facilities',
                # 'sql': """
                    
                #     SELECT CAST([Date_of_Audit] AS DATE)[Date_of_Audit]
                #         ,[Testing_Facility_Name]
                #         ,[_Score]
                #         ,[Level_Score] ,
                #         High_Impact
                #     FROM [LSS].[dbo].[RTCQM_Data]
                #     where [Date_of_Audit] !=''
                #     AND CAST([Date_of_Audit] AS DATE) between %s and %s
                

                #     """
                # },
                
                
                {
                'query_name': 'weekly_Dashboard',
                'sql': """
                   select -- [LAB] 
                LTRIM(RTRIM([Name_of_Lab])) LAB
                FROM --[LSS].[dbo].[Dashboard_Indicators]
                     [LSS].[dbo].[Dash_This_week_Rec_Samples]
                                    WHERE
                                            CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                            AND CAST([date] AS DATE) <= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)           

                    """,
                       'parameters': ()
                },
                
                
                {
                'query_name': 'ist_submission',
                'sql': """
                    select DISTINCT 
                    CASE 
                            WHEN Province_ IN ('Masonaland West', 'Mashonaland West', 'MASHWEST') THEN 'Mash West'
                            WHEN Province_ = 'Matebeleland North' THEN 'Mat North'
                            WHEN Province_ = 'Mashonaland Central' THEN 'Mash Central'
                            WHEN Province_ = 'Matebeleland South' THEN 'Mat South'
                            WHEN Province_ = 'Mashonaland East' THEN 'Mash East'
                            ELSE Province_
                        END as Province
                    from IST_National
                    where    CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                    AND CAST([date] AS DATE) <= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                    and status='rider'
                    """,
                       'parameters': ()
                },
                
                
                
                
                
                            {
                'query_name': 'weekly_Dashboard_sampleRUN',
                'sql': """                
                                
                      WITH achievement AS (
                    select name_of_lab    ,
                    sum(  isnull(cast(RECEIVED_TOTAL_Sample_RUN as int),0) ) Tests_Done

                    ,sum(  isnull(cast(RECEIVED_REPEATS_RUN as int),0) ) Repeats

                    ,sum(  isnull(cast(RECEIVED_TOTAL_Sample_RUN as int),0) )-
                    sum( isnull(cast( [RECEIVED_FAILED_bt_Elig_REPEAT] as int),0)+ 
                    isnull(cast([RECEIVED_FAILED_bt_NOT_Elig_REPEAT] as int),0) )      total_patients_run

                    from [Dash_Sample_Run] t
                    where CAST(t.[date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                    AND CAST(t.[date] AS DATE) <= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0) 
                    and [Test_Type]='VL'
                    and status='Lab'
                    and  [Platform_Roche_Abbott_Hologic_BMX]!='GeneXpert'
                    group by  name_of_lab
                    )

                    select 
                    sum(Tests_Done) Tests_Done,sum(Repeats) Repeats, sum(total_patients_run) total_patients_run,sum(w.weekly_targets) weekly_targets, round (cast(sum(total_patients_run) as float)/sum(w.weekly_targets),2)*100 perc
                    from achievement a, Dash_weekly_targets w
                    where a.Name_of_Lab=w.Lab              


                    """,
                    'parameters': ()
                },
                
        
                
         {
            'query_name': 'Stockout_alert',
            'sql': """
                  SELECT  [Date]
                        ,[Name_of_Lab]
                        ,[Test_Type]
                        ,[Platform_Roche_Abbott_Hologic_BMX]
                        ,[Reagent_kits_RECEIVED_from_OTHER_Labs]
                        ,[Reagent_tests_kits_Stock_on_hand]
                        ,[Date_Received_at_Lab]
                        ,[Reagent_kits_to_OTHER_Labs]
                        ,[Lab_Name_Loaned_to]   
                        ,[Lab_Name_Received_from]
                        ,[Reagent_tests_kits_available]
                        ,[Reagent_stockout_days]
                        ,ISNULL(cast ([Reagent_tests_kits_Stock_on_hand] as float), 0)-ISNULL(cast ([Reagent_kits_RECEIVED_from_OTHER_Labs] as float), 0) Stocks 
                    FROM [LSS].[dbo].[Dash_Testing_Capacity]
                    where   (cast([date] as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                    AND cast([date] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                    AND ISNULL(cast ([Reagent_tests_kits_Stock_on_hand] as float), 0)-ISNULL(cast ([Reagent_kits_RECEIVED_from_OTHER_Labs] as float), 0) <=7
                        
                         """ ,
                       'parameters': ()

            },
         
         
            
            
          
           
               {
            'query_name': 'Dash_LIST',
            'sql': """
          
                 SELECT DISTINCT
                [Date], [Name_of_Lab] LAB   ,[unique_key]    ,[SourceFile],[Update_Date] 
                FROM --[LSS].[dbo].[Dashboard_Indicators]
                     [LSS].[dbo].[Dash_This_week_Rec_Samples]
                    

                        WHERE cast([date] as date) >= %s AND cast([date] as date) <= %s
                        and status='Lab'
                
                """,
                      'parameters': (startdate, enddate)
            },
               
               

               {
            'query_name': 'IST_Dash_LIST',
            'sql': """
          
            SELECT DISTINCT
             cast([date] as date) [Date], SourceFile LAB   ,[unique_key]    ,[SourceFile],[Update_Date] 
             FROM 
               [LSS].[dbo].[IST_National]
             WHERE cast([date] as date) >= %s AND cast([date] as date) <= %s
             order by [Update_Date] 
              
                
                """,
                      'parameters': (startdate, enddate)
            },
               
               
                  



          {
            'query_name': 'Homepage_received',
            'sql': """

            WITH sample_received_total AS (
                                SELECT
                        Last_Sunday,
                        Name_of_Lab,
                        ISNULL([DBS], 0) AS [DBS],
                        ISNULL([Plasma], 0) AS [Plasma],
                        ISNULL([TB Specimen], 0) AS [TB Specimen],
                        w.weekly_targets
                    FROM
                        (
                            SELECT
                               [date] Last_Sunday,
                               -- MAX(CASE WHEN DATEPART(WEEKDAY, CAST([date] AS DATE)) = 1 THEN CAST([date] AS DATE) END) AS Last_Sunday,
                                [Name_of_Lab],
                                [Sample_Type],
                               sum ( cast (Total_samples_received as int) ) Total_samples_received
                        
                            FROM
                                [LSS].[dbo].[Dash_This_week_Rec_Samples]
                            WHERE
                                --CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                --AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                                 [Test_Type]='VL'
                                and CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                AND CAST([date] AS DATE) <= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0) 
                                and status='Lab'
                            GROUP BY
                                [Name_of_Lab],
                                [Sample_Type],
                                [Total_samples_received],
                                [date]
                        ) AS DataSource
                        PIVOT
                        (
                            SUM(Total_samples_received)
                            FOR [Sample_Type] IN ([DBS], [Plasma],[TB Specimen])
                        ) AS PivotTable
                    JOIN Dash_weekly_targets w ON PivotTable.[Name_of_Lab] = w.lab

                    )

                    select sum(plasma)+sum(DBS) total_received, sum(weekly_targets) target,ROUND (cast((sum(plasma)+sum(DBS)) as float)/sum(weekly_targets),2)*100 perce_achievement
                    from sample_received_total
                """,
                       'parameters': ()
            },
          
          
          
           {
            'query_name': 'failure_rate_monthly',
            'sql': """
                                        -- Step 1: Declare variables
                    DECLARE @dates NVARCHAR(MAX) = ''
                    DECLARE @query NVARCHAR(MAX)
                    DECLARE @count INT

                    -- Get the count of distinct dates
                    SELECT @count = COUNT(DISTINCT CAST(date as DATE))
                    FROM [LSS].[dbo].[Dash_Sample_Run]

                    -- Step 2: Create a comma-separated list of dates
                    SELECT @dates = @dates + '[' + CAST([date] AS NVARCHAR(10)) + '],'
                    FROM (
                        SELECT DISTINCT TOP (@count) CAST(date as DATE) [Date] 
                        FROM [LSS].[dbo].[Dash_Sample_Run]
                        where cast([date] as date) >='2023-06-01'
                        ORDER BY CAST(date as DATE)  -- Order by date in ascending order
                    ) AS DistinctDates

                    -- Remove the trailing comma
                    SET @dates = LEFT(@dates, LEN(@dates) - 1)

                    -- Step 3: Create the dynamic SQL query
                    SET @query = '
                    WITH PlatformMapping AS (
                        SELECT 
                            CAST(date as DATE) as [date], 
                        [Platform],
                       ISNULL (FailureRate,0) FailureRate
                        FROM [LSS].[dbo].[FailureRate_view] r
                        where cast([date] as date) >=''2023-06-01''
                    
                    )

                    SELECT [Platform], ' + @dates + '
                    FROM (
                        SELECT [date], [Platform], [FailureRate]
                        FROM PlatformMapping
                    ) AS SourceData
                    PIVOT(
                        SUM(FailureRate)
                        FOR [date] IN (' + @dates + ')
                    ) AS PivotData
                    ORDER BY [Platform];'

                    -- Step 4: Execute the dynamic SQL query
                    EXEC sp_executesql @query;
                    
                                    
                """,
                      'parameters': ()
            },
            
            
               {
            'query_name': 'lab_unresolved',
            'sql': """
                    DECLARE @Dates NVARCHAR(MAX) = ''
                    DECLARE @SQL NVARCHAR(MAX)

                    -- Get distinct dates ordered by descending and convert them to a comma-separated string
                    SELECT @Dates += '[' + CONVERT(VARCHAR, [Date], 23) + '],'
                    FROM (
                        SELECT DISTINCT TOP 10000 CAST([Date] AS DATE) AS [Date] 
                        FROM Dashboard_Indicators 
                        ORDER BY [Date] DESC
                    ) t

                    -- Remove trailing comma
                    SET @Dates = LEFT(@Dates, LEN(@Dates) - 1)

                    -- Construct dynamic SQL
                    SET @SQL = '
                    WITH UnresolvedCounts AS (
                        SELECT 
                            CAST(l.[Date] AS DATE) AS date, 
                            l.lab,
                            SUM(  [UNRESOLVED_Pending_testing]+
                                [UNRESOLVED_Referred_awaiting_results]+
                                [UNRESOLVED_Investigation_in_progress]     ) AS unresolved                       
                        FROM 
                            [LSS].[dbo].[Dash_CLI] c 
                        JOIN 
                            Dashboard_Indicators l ON c.[SourceFile] = l.[SourceFile] AND c.Update_date = l.Update_Date
                        GROUP BY 
                            CAST(l.[Date] AS DATE), l.lab
                    )

                    SELECT 
                        lab, ' + @Dates + '
                    FROM 
                        (SELECT date, lab, unresolved FROM UnresolvedCounts) AS SourceData
                    PIVOT 
                    (
                        SUM(unresolved)
                        FOR date IN (' + @Dates + ')
                    ) AS PivotData
                    ORDER BY 
                        lab;
                    '

                    -- Execute the dynamic SQL
                    EXEC sp_executesql @SQL

                
            
               """,
                      'parameters': ()
            },   
               
               
               
                   {
            'query_name': 'count_dash',
            'sql': """
                       
                    WITH CTE AS (
                        SELECT *,
                            ROW_NUMBER() OVER(PARTITION BY
                            [Date]      
                        ,[LAB]
                        ,[Number_of_carryover_samples]
                        ,[Backlog]
                        ,[Age_of_oldest_Plasma_sample]
                        ,[Age_of_oldest_DBS_sample]
                        ,[Age_of_oldest_EID_sample]
                        ,[Number_of_samples_received_this_week]
                        ,[Total_received_samples_with_Age_Sex_Disaggregation]
                        ,[Number_of_samples_that_are_rejected]
                        ,[Total_with_reasons_for_rejection]
                        ,[Number_of_samples_entered_into_LIMS_on_day_of_arrival]
                        ,[Total_number_of_referred_samples_received]
                        ,[Total_referred_with_reason_for_referral]
                        ,[Total_number_of_samples_run]
                        ,[Calculated_carryover_at_start_of_week_Plasma_VL]
                        ,[Calculated_carryover_at_start_of_week_DBS_VL]
                        ,[Calculated_carryover_at_start_of_week_DBS_EID]
                        ,[Total_number_failed_eligible_for_repeat]
                        ,[Total_number_failed_not_eligible_for_repeat]
                        ,[Number_with_reason_for_failure]
                        ,[Number_of_results_printed_from_LIMS_by_VL_Lab]
                        ,[Number_of_results_dispatched_by_lab]
                        ,[Reagent_test_kits_available]
                        ,[Total_machine_downtime_hours]
                        ,[LIMS_Hours_of_functionality]
                        ,[Number_of_hours_with_no_electricity]
                        ,[Number_of_hours_generator_was_on]
                        ,[SourceFile]
                        ORDER BY LAB) AS rn   
                    FROM [LSS].[dbo].dashboard_indicators

                    )

                    select lab,count(*) dups
                    FROM CTE WHERE rn > 1
                    group by lab


                
                """,
                      'parameters': ()
            },
                   
                 {
            'query_name': 'count_dups',
            'sql': """
                       
                    WITH CTE AS (
                        SELECT *,
                            ROW_NUMBER() OVER(PARTITION BY
                            [Date]      
                        ,[LAB]
                        ,[Number_of_carryover_samples]
                        ,[Backlog]
                        ,[Age_of_oldest_Plasma_sample]
                        ,[Age_of_oldest_DBS_sample]
                        ,[Age_of_oldest_EID_sample]
                        ,[Number_of_samples_received_this_week]
                        ,[Total_received_samples_with_Age_Sex_Disaggregation]
                        ,[Number_of_samples_that_are_rejected]
                        ,[Total_with_reasons_for_rejection]
                        ,[Number_of_samples_entered_into_LIMS_on_day_of_arrival]
                        ,[Total_number_of_referred_samples_received]
                        ,[Total_referred_with_reason_for_referral]
                        ,[Total_number_of_samples_run]
                        ,[Calculated_carryover_at_start_of_week_Plasma_VL]
                        ,[Calculated_carryover_at_start_of_week_DBS_VL]
                        ,[Calculated_carryover_at_start_of_week_DBS_EID]
                        ,[Total_number_failed_eligible_for_repeat]
                        ,[Total_number_failed_not_eligible_for_repeat]
                        ,[Number_with_reason_for_failure]
                        ,[Number_of_results_printed_from_LIMS_by_VL_Lab]
                        ,[Number_of_results_dispatched_by_lab]
                        ,[Reagent_test_kits_available]
                        ,[Total_machine_downtime_hours]
                        ,[LIMS_Hours_of_functionality]
                        ,[Number_of_hours_with_no_electricity]
                        ,[Number_of_hours_generator_was_on]
                        ,[SourceFile]
                        ORDER BY LAB) AS rn   
                    FROM [LSS].[dbo].dashboard_indicators

                    )

                    select count(*) dups
                    FROM CTE WHERE rn > 1
                   


                
                """,
                      'parameters': ()
            },
               
          
              {
            'query_name': 'Dash_IST_LIST',
            'sql': """         


                WITH riders AS (
                select max(unique_key) unique_key, count(*) riders
                from  [LSS].[dbo].[IST_NATIONAL]
                where  CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                and status='rider'
                group by unique_key
                
                ), 

                reliefs AS (  
                select  max(unique_key) unique_key, count(*) relief
                from  [LSS].[dbo].[IST_NATIONAL]
                where  CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                and status='relief rider'
                group by unique_key
                )
                , Drivers AS (  
                select  max(unique_key) unique_key, count(*) driver
                from  [LSS].[dbo].[IST_NATIONAL]
                where  CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                and status='Driver'
                group by unique_key
                )
                
                SELECT   Max(n.[unique_key]) [unique_key],
                                Max(cast ([Date] as date)) [Date], max(n.Province_) Province      , Max([SourceFile]) [SourceFile], MAx([Update_Date]) [Update_Date] 
                                ,count(*) entries,max(r.riders)  riders, max(l.relief) relief, max(d.driver) driver
                                FROM 
                                [LSS].[dbo].[IST_NATIONAL] n
                                LEFT JOIN riders r on r.unique_key=n.unique_key
                                LEFT JOIN reliefs l on l.unique_key=n.unique_key
                                LEFT JOIN Drivers d on d.unique_key=n.unique_key
                                
                    
                                where  CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                                group by n.unique_key 

              
              
              
                """,
                      'parameters': ()
            },
                   
               
               
                 {
            'query_name': 'reagents_stock_expiaries',
            'sql': """
                 With EXpiary_dates AS (
                        select  d.lab Lab, Platform_roche_Abbott_hologic_BMX,Test_Type, 
                        CASE 
                            WHEN Reagent_tests_kits_available_Expiry_Date IS NULL THEN NULL
                            WHEN TRY_CAST(Reagent_tests_kits_available_Expiry_Date AS DATE) IS NOT NULL 
                            THEN CAST(Reagent_tests_kits_available_Expiry_Date AS DATE)
                            ELSE NULL
                        END     Reagent_tests_kits_available_Expiry_Date
                        FROM DATIM_Facility_names d

                        Left join [LSS].[dbo].Dash_Testing_Capacity r on r.Name_of_Lab=d.facility
                        WHERE    r.status='Lab' 
                        and   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                        AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)   


                    ),


                    figures AS (
                            SELECT 
                            d.lab Lab, Platform_roche_Abbott_hologic_BMX,Test_Type
                            ,sum ( cast(Reagent_tests_kits_Stock_on_hand as int)) Reagent_tests_kits_Stock_on_hand,
                            -- max (cast(Reagent_tests_kits_available_Expiry_Date as date)) Reagent_tests_kits_available_Expiry_Date,
                            sum(cast(stock_of_control_available as int)) stock_of_control_available
                            -- ,max (cast (Expiry_Date_of_Controls as date)) Expiry_Date_of_Controls

                            ,CASE 
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Roche C8800' THEN 960
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'COBAS 6800' THEN 384          
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Abbott' THEN 300
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Abbott - cold chain' THEN 300
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Roche C5800' THEN 216     
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Roche C6800' THEN 384
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Abbott - room temperature' THEN 300
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Hologic Panther' THEN 320
                            END capacity8hrs,

                            CASE 
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Roche C8800' THEN 96
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Hologic Panther' THEN 100
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Roche C5800' THEN 192 
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'COBAS 6800' THEN 96
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Abbott - cold chain' THEN 93
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Abbott - room temperature' THEN 93
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Roche C6800' THEN 96                  
                            WHEN Platform_Roche_Abbott_Hologic_BMX = 'Abbott' THEN 93               
                            END tests

                            FROM DATIM_Facility_names d
                            Left join [LSS].[dbo].Dash_Testing_Capacity r on r.Name_of_Lab=d.facility
                            WHERE    r.status='Lab'  
                            and   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                            AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)            

                            Group by  d.lab ,Platform_roche_Abbott_hologic_BMX,Test_Type

                    )

                    ,Nearly_all AS (
                            select f.lab,f.Platform_Roche_Abbott_Hologic_BMX,f.Test_Type,f.capacity8hrs,f.Reagent_tests_kits_Stock_on_hand,
                            CASE 
                            WHEN isnull(capacity8hrs, 0) = 0 THEN NULL
                            ELSE cast(Reagent_tests_kits_Stock_on_hand*tests as float) / isnull(capacity8hrs, 0)
                            END as days_of_stock

                            ,CASE 
                            WHEN isnull(tests, 0) = 0 THEN NULL
                            ELSE cast(Reagent_tests_kits_Stock_on_hand*tests as float) 
                            END as stock_tests


                            ,x.Reagent_tests_kits_available_Expiry_Date,
                            CASE 
                            WHEN Reagent_tests_kits_available_Expiry_Date='1900-01-01' THEN NULL
                            ELSE DATEDIFF(day, GETDATE(), CAST(Reagent_tests_kits_available_Expiry_Date AS DATE))
                        END AS DaysUntilExpiry

                            from EXpiary_dates x,figures f
                            where x.Lab=f.Lab
                            and x.Platform_Roche_Abbott_Hologic_BMX=f.Platform_Roche_Abbott_Hologic_BMX
                            and x.Test_Type=f.Test_Type

                    ) 

                    select lab,
                        Platform_Roche_Abbott_Hologic_BMX, 
                        Test_Type,
                            stock_tests,
                        (days_of_stock*capacity8hrs)-(DaysUntilExpiry*capacity8hrs) unused_reagents,
                        DaysUntilExpiry,        
                        (DaysUntilExpiry*capacity8hrs) reagents_tobe_consumed, 
                        days_of_stock,
                        Reagent_tests_kits_available_Expiry_Date,
                        Reagent_tests_kits_Stock_on_hand, 
                        capacity8hrs    

                    from Nearly_all
                    order by (days_of_stock*capacity8hrs)-(DaysUntilExpiry*capacity8hrs) DESC

                                  
                
                """,
                      'parameters': ()
            },
                            
                            
                
    ]

    if selected_provinces:
        # Create a string of comma-separated placeholders (%s) for the IN clause
        placeholders = ', '.join('%s' for _ in selected_provinces)
        for i in [0,1,2]:
            SQLqueries[i]['sql'] += f" AND [Province_] IN ({placeholders})"
            SQLqueries[i]['parameters'] += tuple(selected_provinces)

    if pepfar_support:
        # Create a string of comma-separated placeholders (%s) for the IN clause
        placeholders = ', '.join('%s' for _ in pepfar_support)
        for i in  [0,1,2]:
            SQLqueries[i]['sql'] += f" AND Type_of_PEPFAR_Support IN ({placeholders})"
            SQLqueries[i]['parameters'] += tuple(pepfar_support)
            
    SQLqueries[0]['sql'] += " group by District_"
    SQLqueries[2]['sql'] += " group by [Date]"
    # SQLqueries[3]['sql'] += "GROUP BY FORMAT(CAST([month] AS date), 'MM-yyyy'), [month],[Type_of_PEPFAR_Support]"
    # SQLqueries[4]['sql'] += "GROUP BY [Province_]"
    # SQLqueries[6]['sql'] += "group by District_"
    
    return SQLqueries



def istqueries(startdate, enddate, selected_provinces, pepfar_support):
    parameters = (startdate, enddate)

    queries = [
        {
            'query_name': 'Query 1',
            'sql': """
                SELECT 
                    [Province_]
                    ,[District_]
                    ,[Type_of_PEPFAR_Support]
                    ,sum(cast([vl_plasma_sam] as int)) Blood_Plasma
                    ,sum(cast([vl_dbs_sam] as int)) DBS_Specimen
                    ,sum(cast([eid_sam] as int)) EID_Specimen
                    ,sum(cast([sputum_sam] as int)) [sputum_sam]
                    ,sum(cast([urine_sam] as int)) [urine_sam]
                    ,sum(cast([Other_samples_Cleaned_] as int)) Other_samples
                FROM IST_National
                WHERE [date] >= %s AND [date] <= %s
            """ ,'parameters': (startdate, enddate)
            
        },
        
        {
                'query_name': 'Query 2',
                'sql': """
                        SELECT 
                        [Province_]
                        ,[Type_of_PEPFAR_Support]
                        ,sum(cast([vl_plasma_sam] as int)) Blood_Plasma
                        ,sum(cast([vl_dbs_sam] as int)) DBS_Specimen
                        ,sum(cast([eid_sam] as int)) EID_Specimen
                        ,sum(cast([sputum_sam] as int)) [sputum_sam]
                        ,sum(cast([urine_sam] as int)) [urine_sam]
                        ,sum(cast([Other_samples_Cleaned_] as int)) Other_samples
                    from  IST_National
                    WHERE [date] >= %s AND [date] <= %s                    
                    
                """,'parameters': (startdate, enddate)
            },
        
         {
                'query_name': 'Query 3',
                'sql': """
                        SELECT
                            [District_],
                            [Type_of_PEPFAR_Support],
                            SUM(CAST([Number_of__Scheduled_Visits_to_Clinic_per__Week] AS INT)) AS Scheduled,
                            SUM(CAST([Number_of_Visits_to_Clinic_per_week] AS INT)) AS Actual,
                            SUM(CAST([Number_of_Visits_to_Clinic_per_week] AS INT)) * 100 / NULLIF(SUM(CAST([Number_of__Scheduled_Visits_to_Clinic_per__Week] AS INT)), 0) AS percentag
                        FROM  IST_National
                         WHERE [date] >= %s AND [date] <= %s     

                """,'parameters': (startdate, enddate)
            },
         
         
           {
                'query_name': 'Query 4',
                'sql': """
                        SELECT FORMAT(CAST([date] AS date), 'MM-yyyy') AS formatted_month,[Type_of_PEPFAR_Support],
                            SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([sputum_sam] AS int) + CAST([urine_sam] AS int) + CAST([Other_samples_Cleaned_] AS int)) AS total_transported
                        FROM [LSS].[dbo].[IST_National]
                         WHERE [date] >= %s AND [date] <= %s        

                """,'parameters': (startdate, enddate)
            },
           
            {
                'query_name': 'Query 5',
                'sql': """

                    SELECT [Province_],
                            SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([sputum_sam] AS int) + CAST([urine_sam] AS int) + CAST([Other_samples_Cleaned_] AS int)) AS total_transported,
                            SUM (cast([vl_plasma_res] as int)+ cast([vl_dbs_res] as int) + cast([eid_res] as int)+ cast([sputum_res] as int)+cast([urine_res] as int)+ cast([Other_Results_cleaned_] as int)) AS Total_results

                        FROM [LSS].[dbo].[IST_National]
                        WHERE [date] >= %s AND [date] <= %s   

                """,'parameters': (startdate, enddate)
            },
            
             {
            'query_name': 'Query 6',
            'sql': """
                        SELECT  
                            SUM(CAST([vl_plasma_sam] AS int) ) [vl_plasma_sam] ,
                            SUM(CAST([vl_dbs_sam] AS int)) [vl_dbs_sam],
                            SUM(CAST([eid_sam] AS int)) [eid_sam] ,
                            SUM(CAST([sputum_sam] AS int)) [sputum_sam],
                            SUM(CAST([urine_sam] AS int)) [urine_sam],
                            SUM(CAST([Other_samples_Cleaned_] AS int)) [Other_samples_Cleaned_], 
                            SUM(CAST([vl_plasma_sam] AS int) )+
                            SUM(CAST([vl_dbs_sam] AS int))+
                            SUM(CAST([eid_sam] AS int))  +
                            SUM(CAST([sputum_sam] AS int))+
                            SUM(CAST([urine_sam] AS int)) +
                            SUM(CAST([Other_samples_Cleaned_] AS int))   Overal_Transported,

                            SUM(CAST([vl_plasma_res] AS int) ) [vl_plasma_res] ,
                            SUM(CAST([vl_dbs_res] AS int)) [vl_dbs_res],
                            SUM(CAST([eid_res] AS int)) [eid_res] ,
                            SUM(CAST([sputum_res] AS int)) [sputum_res],
                            SUM(CAST([urine_res] AS int)) [urine_res],
                            SUM(CAST([Other_Results_cleaned_] AS int)) [Other_Results_cleaned_],    
                            
                            SUM(CAST([vl_plasma_res] AS int) )  +
                            SUM(CAST([vl_dbs_res] AS int)) +
                            SUM(CAST([eid_res] AS int))  +
                            SUM(CAST([sputum_res] AS int)) +
                            SUM(CAST([urine_res] AS int)) +
                            SUM(CAST([Other_Results_cleaned_] AS int))  Overal_results
                        FROM [LSS].[dbo].[IST_National]
                        WHERE [date] >= %s AND [date] <= %s 

                """,'parameters': (startdate, enddate)
            },
             
             
              {
            'query_name': 'Query 7',
            'sql': """
                    SELECT  
                    District_,
                            SUM(CAST([vl_plasma_sam] AS int) ) [vl_plasma_sam] ,
                            SUM(CAST([vl_dbs_sam] AS int)) [vl_dbs_sam],
                            SUM(CAST([eid_sam] AS int)) [eid_sam] ,
                            SUM(CAST([sputum_sam] AS int)) [sputum_sam],
                            SUM(CAST([urine_sam] AS int)) [urine_sam],
                            SUM(CAST([Other_samples_Cleaned_] AS int)) [Other_samples_Cleaned_], 
                            SUM(CAST([vl_plasma_sam] AS int) )+
                            SUM(CAST([vl_dbs_sam] AS int))+
                            SUM(CAST([eid_sam] AS int))  +
                            SUM(CAST([sputum_sam] AS int))+
                            SUM(CAST([urine_sam] AS int)) +
                            SUM(CAST([Other_samples_Cleaned_] AS int))   Overal_Transported,

                            SUM(CAST([vl_plasma_res] AS int) ) [vl_plasma_res] ,
                            SUM(CAST([vl_dbs_res] AS int)) [vl_dbs_res],
                            SUM(CAST([eid_res] AS int)) [eid_res] ,
                            SUM(CAST([sputum_res] AS int)) [sputum_res],
                            SUM(CAST([urine_res] AS int)) [urine_res],
                            SUM(CAST([Other_Results_cleaned_] AS int)) [Other_Results_cleaned_],    
                            
                            SUM(CAST([vl_plasma_res] AS int) )  +
                            SUM(CAST([vl_dbs_res] AS int)) +
                            SUM(CAST([eid_res] AS int))  +
                            SUM(CAST([sputum_res] AS int)) +
                            SUM(CAST([urine_res] AS int)) +
                            SUM(CAST([Other_Results_cleaned_] AS int))  Overal_results
                        FROM [LSS].[dbo].[IST_National]
                        WHERE [date] >= %s AND [date] <= %s  

                            """,'parameters': (startdate, enddate)
                        },
                        
             
             
             
    ]


    if selected_provinces:
        # Create a string of comma-separated placeholders (%s) for the IN clause
        placeholders = ', '.join('%s' for _ in selected_provinces)
        for i in [0,1,2,3, 4, 5, 6]:
            queries[i]['sql'] += f" AND [Province_] IN ({placeholders})"
            queries[i]['parameters'] += tuple(selected_provinces)

    if pepfar_support:
        # Create a string of comma-separated placeholders (%s) for the IN clause
        placeholders = ', '.join('%s' for _ in pepfar_support)
        for i in range(7):
            queries[i]['sql'] += f" AND Type_of_PEPFAR_Support IN ({placeholders})"
            queries[i]['parameters'] += tuple(pepfar_support)
            
    queries[0]['sql'] += " GROUP BY [Province_], [District_], [Type_of_PEPFAR_Support]"
    queries[1]['sql'] += "group by [Province_],[Type_of_PEPFAR_Support]"
    queries[2]['sql'] += "GROUP BY    [District_],  [Type_of_PEPFAR_Support]"
    queries[3]['sql'] += "GROUP BY FORMAT(CAST([date] AS date), 'MM-yyyy'), [date],[Type_of_PEPFAR_Support]"
    queries[4]['sql'] += "GROUP BY [Province_]"
    queries[6]['sql'] += "group by District_"


    return queries


def TATSQLQueries(startdate,enddate):
    parameters = (startdate,enddate)

    SQLqueries = [
       {
            'query_name': 'ist_sample_tat_sql',
            'sql': """
                EXEC [dbo].[IST_TAT_SAMPLEONLY] @StartDate = %s, @EndDate=%s
            """ ,  # wrapping formatted_startdate with a tuple
            'parameters': (startdate, enddate)
       },
       
        {
            'query_name': 'IST_TAT_FACILITY_SAMPLE_TYPE_weekly',
            'sql': """
                EXEC [dbo].[IST_TAT_FACILITY_SAMPLE_TYPE_weekly] @StartDate = %s, @EndDate=%s
            """ ,  # wrapping formatted_startdate with a tuple
            'parameters': (startdate, enddate)
       },
        
         {
            'query_name': 'IST_TAT_FACILITY_SAMPLE_TYPE_monthly',
            'sql': """
                EXEC [dbo].[IST_TAT_FACILITY_SAMPLE_TYPE_monthly] @StartDate = %s, @EndDate=%s
            """ ,  # wrapping formatted_startdate with a tuple
            'parameters': (startdate, enddate)
       },
         
          {
            'query_name': 'IST_TAT_FACILITY_SAMPLE_TYPE',
            'sql': """
                EXEC [dbo].[IST_TAT_FACILITY_SAMPLE_TYPE] @StartDate = %s, @EndDate=%s
            """ ,  # wrapping formatted_startdate with a tuple
            'parameters': (startdate, enddate)
       },
          {
            'query_name': 'IST_TAT_FACILITY_SAMPLE_TYPE_province',
            'sql': """
                EXEC [IST_TAT_FACILITY_SAMPLE_TYPE_province] @StartDate = %s, @EndDate=%s
            """ ,  # wrapping formatted_startdate with a tuple
            'parameters': (startdate, enddate)
       },
       
    ]
    return SQLqueries

def ISTSQLQueries(datelist,startdate,enddate):
    parameters = (datelist,startdate,enddate)

    SQLqueries = [
       {
            'query_name': 'ist_getspecimen_results',
            'sql': """
                EXEC [dbo].[GetSpecimens_and_Results] @StartDate = %s,  @EndDate = %s  
            """ ,  
            'parameters': (startdate,enddate)
       },  
         {
            'query_name': 'DISTRICT_GetSpecimens_and_Results',
            'sql': """
                EXEC [dbo].[DISTRICT_GetSpecimens_and_Results] @StartDate = %s,  @EndDate = %s  
            """ ,  
            'parameters': (startdate,enddate)
       },  
       
        {
            'query_name': 'ist_GFgetspecimen_results',
            'sql': """
                EXEC [dbo].[GFGetSpecimens_and_Results] @StartDate = %s,  @EndDate = %s  
            """ ,  
            'parameters': (startdate,enddate)
       },  
        
         {
            'query_name': 'ist_PEPFARgetspecimen_results',
            'sql': """
                EXEC [dbo].[PEPFAR_GetSpecimens_and_Results] @StartDate = %s,  @EndDate = %s  
            """ ,  
            'parameters': (startdate,enddate)
       },  
       
       {
            'query_name': 'ist_getbike_functionality',
            'sql': """
                   EXEC [dbo].[GetBike_Functionality]
                    @StartDate = %s,  @EndDate = %s                                      
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
       
        {
            'query_name': 'ist_getRxns_nonfunctional',
            'sql': """
                   EXEC [dbo].[GetReasons_BikeNonFunctional]
                    @StartDate = %s,  @EndDate = %s                                      
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
        
          {
            'query_name': 'ist_getRxns_Summing_all',
            'sql': """
                   EXEC [dbo].[GetReasons_Summing]
                    @StartDate = %s,  @EndDate = %s                                      
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
           {
            'query_name': 'GetDistricts_with_reasons',
            'sql': """
                   EXEC [dbo].[GetDistricts_with_reasons]
                    @StartDate = %s,  @EndDate = %s                                      
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
       
       
        {
            'query_name': 'ist_getTA_DSD_relief',
            'sql': """
                SELECT
                    case Type_of_PEPFAR_Support
                    when 'TA-DSI' then 'Global_Fund'
                    when 'DSD' then 'Pepfar'
                    when 'TA-SDI' then 'Global_Fund'
                    else Type_of_PEPFAR_Support
                    end as Type_of_PEPFAR_Support
                    , count (LOWER(REPLACE(LTRIM(RTRIM(Bike_Registration_Number)), ' ', '')) ) relief_riders 
                FROM ist_national
                WHERE CAST([date] AS date) between %s and %s
                AND status = 'relief rider'
                group by 
                    case Type_of_PEPFAR_Support
                    when 'TA-DSI' then 'Global_Fund'
                    when 'DSD' then 'Pepfar'
                    when 'TA-SDI' then 'Global_Fund'
                    else Type_of_PEPFAR_Support
                    end                                      
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
         {
            'query_name': 'ist_reliefSamples_transported',
            'sql': """
                  SELECT 

                    CASE 
                        WHEN Province_ IN ('Masonaland West', 'Mashonaland West', 'MASHWEST') THEN 'Mash West'
                        WHEN Province_ = 'Matebeleland North' THEN 'Mat North'
                        WHEN Province_ = 'Mashonaland Central' THEN 'Mash Central'
                        WHEN Province_ = 'Matebeleland South' THEN 'Mat South'
                        WHEN Province_ = 'Mashonaland East' THEN 'Mash East'
                        ELSE Province_
                    END as Province_,

                    -- Summing up samples and results for various tests
                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
                    
                    -- Sputum tests
                    SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
                    SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
                    
                    -- Individual sums for certain tests
                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
                    SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
                    SUM(CAST([HPV] AS int)) AS HPV,
                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
                    SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
                    SUM(CAST([HPV_res] AS int)) AS HPV_res,

                    -- total all
                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) +SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int))+  SUM(CAST([HPV] AS int)) AS total_sam,
                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) + SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int))+SUM(CAST([HPV_res] AS int))  AS total_res
                    

                FROM 
                    [LSS].[dbo].[IST_National]
                WHERE 
                    status = 'relief rider'
                    AND CAST([date] AS date) between %s and %s
                GROUP BY  
                
                    CASE 
                        WHEN Province_ IN ('Masonaland West', 'Mashonaland West', 'MASHWEST') THEN 'Mash West'
                        WHEN Province_ = 'Matebeleland North' THEN 'Mat North'
                        WHEN Province_ = 'Mashonaland Central' THEN 'Mash Central'
                        WHEN Province_ = 'Matebeleland South' THEN 'Mat South'
                        WHEN Province_ = 'Mashonaland East' THEN 'Mash East'
                        ELSE Province_
                    END                                     
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
         
           {
            'query_name': 'ist_other_samples_and_results',
            'sql': """            
            Select  other_sam,other_res
            FROM 
                [LSS].[dbo].[IST_National]
            WHERE 
            
            status in ( 'relief rider' ,'rider','driver')
                AND CAST([date] AS date) between %s and %s    
               -- and ([Type_of_PEPFAR_Support]='TA-SDI' or  [Type_of_PEPFAR_Support]='TA-DSI')            
                and (Driver_Sample_Status ='Samples transported for the first time' or Driver_Sample_Status ='' or Driver_Sample_Status is null)                                     
                                
            """ ,  
            'parameters': (startdate,enddate)
       },  
       
       
         {
            'query_name': 'ist_relief_riders_per_province',
            'sql': """            
                              
                    WITH datasets AS (      
                            
                            SELECT 

                                    CASE 
                                        WHEN Province_ IN ('Masonaland West', 'Mashonaland West', 'MASHWEST') THEN 'Mash West'
                                        WHEN Province_ = 'Matebeleland North' THEN 'Mat North'
                                        WHEN Province_ = 'Mashonaland Central' THEN 'Mash Central'
                                        WHEN Province_ = 'Matebeleland South' THEN 'Mat South'
                                        WHEN Province_ = 'Mashonaland East' THEN 'Mash East'
                                        ELSE Province_
                                    END as Province_,

                                    count(*) relief_riders

                                FROM 
                                    [LSS].[dbo].[IST_National]
                                WHERE 
                                    status = 'relief rider'
                                    AND CAST([date] AS date) between %s and %s
                                    --and CAST([date] AS date) between '2023-10-23' and '2023-10-29'
                                GROUP BY  
                                
                                    CASE 
                                        WHEN Province_ IN ('Masonaland West', 'Mashonaland West', 'MASHWEST') THEN 'Mash West'
                                        WHEN Province_ = 'Matebeleland North' THEN 'Mat North'
                                        WHEN Province_ = 'Mashonaland Central' THEN 'Mash Central'
                                        WHEN Province_ = 'Matebeleland South' THEN 'Mat South'
                                        WHEN Province_ = 'Mashonaland East' THEN 'Mash East'
                                        ELSE Province_
                                    END  
                                    )

                                    SELECT  
                    ISNULL([Mash West], 0) AS [Mash West],
                    ISNULL([Mat North], 0) AS [Mat North],
                    ISNULL([Mash Central], 0) AS [Mash Central],
                    ISNULL([Mat South], 0) AS [Mat South],
                    ISNULL([Mash East], 0) AS [Mash East],
                    ISNULL([Midlands], 0) AS [Midlands],
                    ISNULL([Manicaland], 0) AS [Manicaland],
                    ISNULL([Bulawayo], 0) AS [Bulawayo],
                    ISNULL([Harare], 0) AS [Harare],
                    ISNULL([Masvingo], 0) AS [Masvingo]

                    
                FROM datasets
                PIVOT (
                    SUM(relief_riders)
                    FOR Province_ IN ([Mash West], [Mat North], [Mash Central], [Mat South], [Mash East] ,[Masvingo],[Harare],[Bulawayo],[Manicaland],[Midlands])
                ) AS pivoted;
                                                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
       
       
           {
            'query_name': 'ist_TAT_DSD_relief',
            'sql': """
                    SELECT 
                        FORMAT(CAST([date] AS date), 'MM-yyyy') [date],
                                    case Type_of_PEPFAR_Support
                                        when 'TA-DSI' then 'Global_Fund'
                                        when 'DSD' then 'Pepfar'
                                        when 'TA-SDI' then 'Global_Fund'
                                        else Type_of_PEPFAR_Support
                                        end     Type_of_PEPFAR_Support,
                        -- Summing up samples and results for various tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
                        
                        -- Sputum tests
                        SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
                        SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
                        
                        -- Individual sums for certain tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
                        SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
                        SUM(CAST([HPV] AS int)) AS HPV,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
                        SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
                        SUM(CAST([HPV_res] AS int)) AS HPV_res,

                        -- total all
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) +SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int))+  SUM(CAST([HPV] AS int)) AS total_sam,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) + SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int))+SUM(CAST([HPV_res] AS int))  AS total_res
                        

                    FROM 
                        [LSS].[dbo].[IST_National]
                    WHERE 
                        status = 'relief rider'
                        --AND CAST([date] AS date) between '2023-07-01' and '2023-10-22'
                        AND CAST([date] AS date) between %s and %s
                    GROUP BY  
                        FORMAT(CAST([date] AS date), 'MM-yyyy'), 
                        case Type_of_PEPFAR_Support
                                        when 'TA-DSI' then 'Global_Fund'
                                        when 'DSD' then 'Pepfar'
                                        when 'TA-SDI' then 'Global_Fund'
                                        else Type_of_PEPFAR_Support
                                        end  
                    order by FORMAT(CAST([date] AS date), 'MM-yyyy')                  
                                                    
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
           
           
           
            {
            'query_name': 'ist_TAT_DSD_relief_by_week',
            'sql': """
                    SELECT 
                        CAST([date] AS date) [date],
                                    case Type_of_PEPFAR_Support
                                        when 'TA-DSI' then 'Global_Fund'
                                        when 'DSD' then 'Pepfar'
                                        when 'TA-SDI' then 'Global_Fund'
                                        else Type_of_PEPFAR_Support
                                        end     Type_of_PEPFAR_Support,
                        -- Summing up samples and results for various tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
                        
                        -- Sputum tests
                        SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
                        SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
                        
                        -- Individual sums for certain tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
                        SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
                        SUM(CAST([HPV] AS int)) AS HPV,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
                        SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
                        SUM(CAST([HPV_res] AS int)) AS HPV_res,

                        -- total all
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) +SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int))+  SUM(CAST([HPV] AS int)) AS total_sam,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) + SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int))+SUM(CAST([HPV_res] AS int))  AS total_res
                        

                    FROM 
                        [LSS].[dbo].[IST_National]
                    WHERE 
                        status = 'relief rider'
                        --AND CAST([date] AS date) between '2023-07-01' and '2023-10-22'
                        AND CAST([date] AS date) between %s and %s
                    GROUP BY  
                        CAST([date] AS date), 
                        case Type_of_PEPFAR_Support
                                        when 'TA-DSI' then 'Global_Fund'
                                        when 'DSD' then 'Pepfar'
                                        when 'TA-SDI' then 'Global_Fund'
                                        else Type_of_PEPFAR_Support
                                        end  
                    order by CAST([date] AS date)                 
                                                    
                    
            """ ,  
            'parameters': (startdate,enddate)
       },  
       
       
        {
            'query_name': 'weekly_ist_transported',
            'sql': """
                
                    WITH Rider_and_Relief  AS (
                    SELECT 
                    CAST([date] AS date) AS [month],            

                        -- Summing up samples and results for various tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
                        
                        -- Sputum tests
                        SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
                        SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
                        
                        -- Individual sums for certain tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
                        SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
                        SUM(CAST([HPV] AS int)) AS HPV,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
                        SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
                        SUM(CAST([HPV_res] AS int)) AS HPV_res
                    FROM 
                        [LSS].[dbo].[IST_National]
                    WHERE 
                        status != 'Driver'
                        AND CAST([date] AS date)  between %s and %s
                    GROUP BY  
                        CAST([date] AS date)                 

                    )

                    ,Driver_1st_Time AS (

                    SELECT 
                        CAST([date] AS date)   AS [month],                   

                        -- Summing up samples and results for various tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
                        
                        -- Sputum tests
                        SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
                        SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
                        
                        -- Individual sums for certain tests
                        SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
                        SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
                        SUM(CAST([HPV] AS int)) AS HPV,
                        SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
                        SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
                        SUM(CAST([HPV_res] AS int)) AS HPV_res
                    FROM 
                        [LSS].[dbo].[IST_National]
                    WHERE 
                        driver_sample_status != 'Samples relayed (not carried for the first time)'
                        and status = 'Driver'
                        AND CAST([date] AS date)  between %s and %s                   
                    GROUP BY  
                     CAST([date] AS date)  
                    
                        )


                    SELECT 
        
                    r.[month],
                    ISNULL(r.total_vl_eid_sam, 0) + ISNULL(d.total_vl_eid_sam, 0) AS total_vl_eid_sam,
                    ISNULL(r.total_vl_eid_res, 0) + ISNULL(d.total_vl_eid_res, 0) AS total_vl_eid_res,
                    ISNULL(r.[Sputum_Culture_DR_NTBRL], 0) + ISNULL(d.[Sputum_Culture_DR_NTBRL], 0) AS Sputum_Culture_DR_NTBRL,
                    ISNULL(r.[Sputum_Culture_DR_NTBRL_res], 0) + ISNULL(d.[Sputum_Culture_DR_NTBRL_res], 0) AS Sputum_Culture_DR_NTBRL_res,
                    ISNULL(r.VL_Plasma_DBS, 0) + ISNULL(d.VL_Plasma_DBS, 0) AS VL_Plasma_DBS,
                    ISNULL(r.EID_Plasma_DBS, 0) + ISNULL(d.EID_Plasma_DBS, 0) AS EID_Plasma_DBS,
                    ISNULL(r.HPV, 0) + ISNULL(d.HPV, 0) AS HPV,
                    ISNULL(r.HPV_res, 0) + ISNULL(d.HPV_res, 0) AS HPV_res,
                    ISNULL(r.VL_Plasma_DBS_res, 0) + ISNULL(d.VL_Plasma_DBS_res, 0) AS VL_Plasma_DBS_res,
                    ISNULL(r.EID_Plasma_DBS_res, 0) + ISNULL(d.EID_Plasma_DBS_res, 0) AS EID_Plasma_DBS_res
                    FROM 
                    Rider_and_Relief r
                    LEFT JOIN Driver_1st_Time d ON r.[month] = d.[month]                
                
                
            """ ,  
            'parameters': (startdate,enddate,startdate,enddate)
       }, 
       
        
         
        {
            'query_name': 'monthly_ist_transported',
            'sql': """
                
                WITH Rider_and_Relief  AS (
                SELECT 
                FORMAT(CAST([date] AS date), 'MM-yyyy') AS [month],                 

                    -- Summing up samples and results for various tests
                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
                    
                    -- Sputum tests
                    SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
                    SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
                    
                    -- Individual sums for certain tests
                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
                    SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
                    SUM(CAST([HPV] AS int)) AS HPV,
                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
                    SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
                    SUM(CAST([HPV_res] AS int)) AS HPV_res
                FROM 
                    [LSS].[dbo].[IST_National]
                WHERE 
                    status != 'Driver'
                    AND CAST([date] AS date)  between %s and %s

                -- AND FORMAT(CAST([date] AS date), 'MM-yyyy') IN ('07-2023', '08-2023', '09-2023')
                GROUP BY  
                    FORMAT(CAST([date] AS date), 'MM-yyyy')
          
                )

                ,Driver_1st_Time AS (

                SELECT 
                    FORMAT(CAST([date] AS date), 'MM-yyyy') AS [month],
              
                    -- Summing up samples and results for various tests
                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int) + CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS total_vl_eid_sam,
                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int) + CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS total_vl_eid_res,
                    
                    -- Sputum tests
                    SUM(CAST([Sputum_Culture_DR_NTBRL] AS int) + CAST([sputum_sam] AS int)) AS [Sputum_Culture_DR_NTBRL],
                    SUM(CAST([Sputum_Culture_DR_NTBRL_res] AS int) + CAST([sputum_res] AS int)) AS [Sputum_Culture_DR_NTBRL_res],
                    
                    -- Individual sums for certain tests
                    SUM(CAST([vl_plasma_sam] AS int) + CAST([vl_dbs_sam] AS int)) AS VL_Plasma_DBS,
                    SUM(CAST([eid_sam] AS int) + CAST([eid_dbs] AS int)) AS EID_Plasma_DBS,
                    SUM(CAST([HPV] AS int)) AS HPV,
                    SUM(CAST([vl_plasma_res] AS int) + CAST([vl_dbs_res] AS int)) AS VL_Plasma_DBS_res,
                    SUM(CAST([eid_res] AS int) + CAST([eid_dbs_res] AS int)) AS EID_Plasma_DBS_res,
                    SUM(CAST([HPV_res] AS int)) AS HPV_res
                FROM 
                    [LSS].[dbo].[IST_National]
                WHERE 
                    driver_sample_status != 'Samples relayed (not carried for the first time)'
                    and status = 'Driver'
                    AND CAST([date] AS date)  between %s and %s
      
                GROUP BY  
                    FORMAT(CAST([date] AS date), 'MM-yyyy')
                
                    )


                SELECT 
                --r.Province_,District_,
                r.[month],
                ISNULL(r.total_vl_eid_sam, 0) + ISNULL(d.total_vl_eid_sam, 0) AS total_vl_eid_sam,
                ISNULL(r.total_vl_eid_res, 0) + ISNULL(d.total_vl_eid_res, 0) AS total_vl_eid_res,
                ISNULL(r.[Sputum_Culture_DR_NTBRL], 0) + ISNULL(d.[Sputum_Culture_DR_NTBRL], 0) AS Sputum_Culture_DR_NTBRL,
                ISNULL(r.[Sputum_Culture_DR_NTBRL_res], 0) + ISNULL(d.[Sputum_Culture_DR_NTBRL_res], 0) AS Sputum_Culture_DR_NTBRL_res,
                ISNULL(r.VL_Plasma_DBS, 0) + ISNULL(d.VL_Plasma_DBS, 0) AS VL_Plasma_DBS,
                ISNULL(r.EID_Plasma_DBS, 0) + ISNULL(d.EID_Plasma_DBS, 0) AS EID_Plasma_DBS,
                ISNULL(r.HPV, 0) + ISNULL(d.HPV, 0) AS HPV,
                ISNULL(r.HPV_res, 0) + ISNULL(d.HPV_res, 0) AS HPV_res,
                ISNULL(r.VL_Plasma_DBS_res, 0) + ISNULL(d.VL_Plasma_DBS_res, 0) AS VL_Plasma_DBS_res,
                ISNULL(r.EID_Plasma_DBS_res, 0) + ISNULL(d.EID_Plasma_DBS_res, 0) AS EID_Plasma_DBS_res
                FROM 
                Rider_and_Relief r
                LEFT JOIN Driver_1st_Time d ON r.[month] = d.[month]

            """ ,  
            'parameters': (startdate,enddate,startdate,enddate)
       }, 
       
 
    ]
    return SQLqueries





def weekly_narrative_queries(startdate,enddate):
    parameters = (startdate,enddate)

    SQLqueries = [
      
         {
            'query_name': 'Sample_Received',
            'sql': """
                                SELECT
                        Last_Sunday,
                        Name_of_Lab,
                        ISNULL([DBS], 0) AS [DBS],
                        ISNULL([Plasma], 0) AS [Plasma],
                        w.weekly_targets
                    FROM
                        (
                            SELECT
                               [date] Last_Sunday,
                               -- MAX(CASE WHEN DATEPART(WEEKDAY, CAST([date] AS DATE)) = 1 THEN CAST([date] AS DATE) END) AS Last_Sunday,
                                [Name_of_Lab],
                                [Sample_Type],
                                sum([Total_samples_received]) [Total_samples_received]  
                        
                            FROM
                                [LSS].[dbo].[Dash_This_week_Rec_Samples]
                            WHERE
                                --CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                --AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                                 [Test_Type]!='EID'
                                and  cast([date] as date) >= %s AND cast([date] as date) <= %s
                            GROUP BY
                                [Name_of_Lab],
                                [Sample_Type],
                                [Total_samples_received],
                                [date]
                        ) AS DataSource
                        PIVOT
                        (
                            SUM(Total_samples_received)
                            FOR [Sample_Type] IN ([DBS], [Plasma])
                        ) AS PivotTable
                    JOIN Dash_weekly_targets w ON PivotTable.[Name_of_Lab] = w.lab

                """,
                         'parameters': (startdate, enddate)
            },
            
            
                {
            'query_name': 'percentage_achievement',
            'sql': """
                   WITH base AS  (

                        SELECT  [Date]
                            ,[Name_of_Lab]
                            ,SUM ( isnull(cast([CARRYOVER_Samples_RUN] as int),0) + isnull(cast([RECEIVED_TOTAL_Sample_RUN] as int),0) + isnull(cast([REFERRED_Samples_RUN] as int),0)) Test_Done
                            
                            ,SUM ( isnull(cast([CARRYOVER_FAILED_samples_elig_repeat] as int),0) + isnull(cast([CARRYOVER_FAILED_samples_Ntelig_repeat] as int),0) +
                                    isnull(cast([RECEIVED_FAILED_bt_Elig_REPEAT] as int),0) + isnull(cast([RECEIVED_FAILED_bt_NOT_Elig_REPEAT] as int),0) +
                                    isnull(cast([REFERRED_FAILED_bt_Elig_REPEAT] as int),0)+  isnull(cast([REFERRED_FAILED_bt_NOT_Elig_REPEAT] as int),0) )  failed_elig,
                                    
                            SUM( isnull(cast(CARRYOVER_repeats_RUN as int),0) + isnull(cast([RECEIVED_REPEATS_RUN] as int),0)+ isnull(cast([REFERRED_REPEATS_RUN]as int),0) )total_repeats,
                            
                            SUM ( (    isnull(cast([CARRYOVER_Samples_RUN] as int),0) +  isnull(cast([RECEIVED_TOTAL_Sample_RUN] as int),0) + isnull(cast([REFERRED_Samples_RUN] as int),0))-
                            (isnull(cast([CARRYOVER_FAILED_samples_elig_repeat] as int),0)+ isnull(cast([CARRYOVER_FAILED_samples_Ntelig_repeat] as int),0) +
                            isnull(cast([RECEIVED_FAILED_bt_Elig_REPEAT] as int),0) + isnull(cast([RECEIVED_FAILED_bt_NOT_Elig_REPEAT] as int),0) +
                            isnull(cast([REFERRED_FAILED_bt_Elig_REPEAT] as int),0) + isnull(cast([REFERRED_FAILED_bt_NOT_Elig_REPEAT] as int),0) ) +                            
                            (isnull(cast(CARRYOVER_repeats_RUN as int),0) + isnull(cast([RECEIVED_REPEATS_RUN] as int),0) + isnull(cast([REFERRED_REPEATS_RUN]as int),0) ) ) Total_Patients_Run 
                            
                        FROM [LSS].[dbo].[Dash_Sample_Run]  
                        where Test_type!='EID' 
                        and Platform_Roche_Abbott_Hologic_BMX!='GeneXpert'
                        and Status='Lab'

                        group by  [Date] ,[Name_of_Lab]
                        ) 

                        select 
                         [Date],
                         CASE 
                                WHEN name_of_lab = 'Beitbridge - 100052 - District Hospital' THEN 'Beitbridge'
                                WHEN name_of_lab = 'Bindura - 100070 - Provincial Hospital' THEN 'Bindura'
                                WHEN name_of_lab = 'Beatrice Infectious - 100050 - Hospital' THEN 'BRIDH'
                                WHEN name_of_lab = 'Chinhoyi - 100235 - Provincial Hospital' THEN 'Chinhoyi'
                                WHEN name_of_lab = 'Gwanda - 100561 - Provincial Hospital' THEN 'Gwanda'
                                WHEN name_of_lab = 'Gweru - 100572 - Provincial Hospital' THEN 'Gweru'
                                WHEN name_of_lab = 'Kadoma - 100681 - District Hosp' THEN 'Kadoma'
                                WHEN name_of_lab = 'Marondera - 100903 - Provincial Hospital' THEN 'Marondera'
                                WHEN name_of_lab = 'Masvingo - 100937 - General Hospital' THEN 'Masvingo'
                                WHEN name_of_lab = 'Mpilo - 101041 - Central Hospital' THEN 'Mpilo'
                                WHEN name_of_lab = 'Mutare - 101165 - Provincial Hospital' THEN 'Mutare'
                                WHEN name_of_lab = 'National Reference Laboratory - 101206 - Laboratory' THEN 'NMRL'
                                WHEN name_of_lab = 'St. Lukes - 101645 - Mission Hospital' THEN 'St Lukes'
                                WHEN name_of_lab = 'Victoria Falls - 101739 - District Hospital' THEN 'Vic Falls'
                                ELSE name_of_lab
                            END AS name_of_lab, Test_Done, failed_elig, total_repeats, Total_Patients_Run,

                            [weekly_targets], 
                        CASE
                            WHEN t.weekly_targets IS NOT NULL AND t.weekly_targets != 0 THEN ROUND((CAST(b.Total_Patients_Run AS FLOAT) / t.weekly_targets) * 100, 2)
                            ELSE NULL
                        END AS [Target_Achievement], 100 percent_target
                        from  Base b,[LSS].[dbo].Dash_weekly_targets t
                        where t.[Lab]=b.Name_of_Lab
                         --and  (cast([date] as date) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                         --    AND cast([date] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                        and  cast([date] as date) >= %s AND cast([date] as date) <= %s
                        order by [Date]

    
                        
                                """,
                        'parameters': (startdate, enddate)
            },


            
                
            {
                'query_name': 'EID_on_GeneXpert',
                'sql': """
                    
                    SELECT  
                        [Name_of_Lab]
                        ,SUM (cast([CARRYOVER_Samples_RUN] as int))[CARRYOVER_Samples_RUN]
                        ,SUM (cast([RECEIVED_TOTAL_Sample_RUN] as int))[RECEIVED_TOTAL_Sample_RUN]
                        ,SUM (cast ([REFERRED_Samples_RUN] as int))[REFERRED_Samples_RUN]
                        ,SUM(cast([CARRYOVER_Samples_RUN] as int)+cast([RECEIVED_TOTAL_Sample_RUN] as int)+cast ([REFERRED_Samples_RUN] as int)) Total_EID_Run
                    
                    FROM [LSS].[dbo].[Dash_Sample_Run]
                    where  [Test_Type]='EID'
                    and  [Platform_Roche_Abbott_Hologic_BMX]='GeneXpert'
                    --and  (cast([date] as date) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                    --AND cast([date] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                    and  cast([date] as date) >= %s AND cast([date] as date) <= %s
                    Group by  [Name_of_Lab]
                    Having  (SUM(cast([CARRYOVER_Samples_RUN] as int)+cast([RECEIVED_TOTAL_Sample_RUN] as int)+cast ([REFERRED_Samples_RUN] as int)))>0

                    """,
                       'parameters': (startdate, enddate)
                },
            
            {
                'query_name': 'EID_not_tested',
                'sql': """
                     SELECT
                                 CASE 
                                WHEN r.name_of_lab = 'Beitbridge - 100052 - District Hospital' THEN 'Beitbridge'
                                WHEN r.name_of_lab = 'Bindura - 100070 - Provincial Hospital' THEN 'Bindura'
                                WHEN r.name_of_lab = 'Beatrice Infectious - 100050 - Hospital' THEN 'BRIDH'
                                WHEN r.name_of_lab = 'Chinhoyi - 100235 - Provincial Hospital' THEN 'Chinhoyi'
                                WHEN r.name_of_lab = 'Gwanda - 100561 - Provincial Hospital' THEN 'Gwanda'
                                WHEN r.name_of_lab = 'Gweru - 100572 - Provincial Hospital' THEN 'Gweru'
                                WHEN r.name_of_lab = 'Kadoma - 100681 - District Hosp' THEN 'Kadoma'
                                WHEN r.name_of_lab = 'Marondera - 100903 - Provincial Hospital' THEN 'Marondera'
                                WHEN r.name_of_lab = 'Masvingo - 100937 - General Hospital' THEN 'Masvingo'
                                WHEN r.name_of_lab = 'Mpilo - 101041 - Central Hospital' THEN 'Mpilo'
                                WHEN r.name_of_lab = 'Mutare - 101165 - Provincial Hospital' THEN 'Mutare'
                                WHEN r.name_of_lab = 'National Reference Laboratory - 101206 - Laboratory' THEN 'NMRL'
                                WHEN r.name_of_lab = 'St. Lukes - 101645 - Mission Hospital' THEN 'St Lukes'
                                WHEN r.name_of_lab = 'Victoria Falls - 101739 - District Hospital' THEN 'Vic Falls'
                                ELSE r.name_of_lab
                            END name_of_lab,
                                r.[Date],
                                SUM(                                   
                                    CAST(r.[RECEIVED_TOTAL_Sample_RUN] AS int) 
                                   
                                ) AS Eid_Tested
                               

                            FROM [LSS].[dbo].[Dash_Sample_Run] r
                            WHERE r.[Test_Type] = 'EID'
                            AND r.Sample_Type='DBS' 
                            and r.status='lab'
                            and platform_roche_Abbott_hologic_BMX='GeneXpert'    
                             and   CAST(r.[date] AS DATE) >= %s
                              AND CAST(r.[date] AS DATE) < %s
                            GROUP BY                              CASE 
                                WHEN r.name_of_lab = 'Beitbridge - 100052 - District Hospital' THEN 'Beitbridge'
                                WHEN r.name_of_lab = 'Bindura - 100070 - Provincial Hospital' THEN 'Bindura'
                                WHEN r.name_of_lab = 'Beatrice Infectious - 100050 - Hospital' THEN 'BRIDH'
                                WHEN r.name_of_lab = 'Chinhoyi - 100235 - Provincial Hospital' THEN 'Chinhoyi'
                                WHEN r.name_of_lab = 'Gwanda - 100561 - Provincial Hospital' THEN 'Gwanda'
                                WHEN r.name_of_lab = 'Gweru - 100572 - Provincial Hospital' THEN 'Gweru'
                                WHEN r.name_of_lab = 'Kadoma - 100681 - District Hosp' THEN 'Kadoma'
                                WHEN r.name_of_lab = 'Marondera - 100903 - Provincial Hospital' THEN 'Marondera'
                                WHEN r.name_of_lab = 'Masvingo - 100937 - General Hospital' THEN 'Masvingo'
                                WHEN r.name_of_lab = 'Mpilo - 101041 - Central Hospital' THEN 'Mpilo'
                                WHEN r.name_of_lab = 'Mutare - 101165 - Provincial Hospital' THEN 'Mutare'
                                WHEN r.name_of_lab = 'National Reference Laboratory - 101206 - Laboratory' THEN 'NMRL'
                                WHEN r.name_of_lab = 'St. Lukes - 101645 - Mission Hospital' THEN 'St Lukes'
                                WHEN r.name_of_lab = 'Victoria Falls - 101739 - District Hospital' THEN 'Vic Falls'
                                ELSE r.name_of_lab
                            END, r.[Date]
                                      
                            

                    """,
                      'parameters': (startdate, enddate)
                },



                  
         {
            'query_name': 'failed_Samples',
            'sql': """
                            DECLARE @Platforms NVARCHAR(MAX), 
                            @SQL NVARCHAR(MAX), 
                            @StartDate DATE, 
                            @EndDate DATE;

                    -- Set your desired dates here
                    --SET @StartDate = '2023-07-31';
                    --SET @EndDate = '2023-08-06';

                    -- Get unique platforms
                    SELECT @Platforms = STRING_AGG('[' + Platform_Roche_Abbott_Hologic_BMX + ']', ',')
                    FROM (SELECT DISTINCT [Platform_Roche_Abbott_Hologic_BMX] 
                        FROM [LSS].[dbo].[Dash_Sample_Run]) AS Platforms;

                    -- Create the dynamic SQL
                    SET @SQL = N'
                    WITH FailedSamplesCTE AS (
                        SELECT  [Date]
                            ,[Name_of_Lab]
                            ,[Platform_Roche_Abbott_Hologic_BMX]
                            ,sum( cast([CARRYOVER_FAILED_samples_elig_repeat] as float)+
                            cast([CARRYOVER_FAILED_samples_Ntelig_repeat] as float)+      
                            cast([RECEIVED_FAILED_bt_Elig_REPEAT]as float)+ 
                            cast([RECEIVED_FAILED_bt_NOT_Elig_REPEAT]as float)+     
                            cast([REFERRED_FAILED_bt_Elig_REPEAT]as float)+ 
                            cast([REFERRED_FAILED_bt_NOT_Elig_REPEAT]as float)) AS failed_samples
                        FROM [LSS].[dbo].[Dash_Sample_Run]
                        WHERE cast(date as date) BETWEEN @StartDate AND @EndDate
                        and test_type!=''EID''
                        --and  [Platform_Roche_Abbott_Hologic_BMX]!=''GeneXpert''
                        GROUP BY [Platform_Roche_Abbott_Hologic_BMX],[Date],[Name_of_Lab]
                    )

                    SELECT  [Name_of_Lab], ' + @Platforms + '
                    FROM (
                        SELECT [Date], [Name_of_Lab], [Platform_Roche_Abbott_Hologic_BMX], failed_samples
                        FROM FailedSamplesCTE
                    ) AS SourceTable
                    PIVOT (
                        SUM(failed_samples)
                        FOR [Platform_Roche_Abbott_Hologic_BMX] IN (' + @Platforms + ')
                    ) AS PivotTable
                    ORDER BY Name_of_Lab;';

                    -- Execute the dynamic SQL, passing the date parameters
                    EXEC sp_executesql @SQL, N'@StartDate DATE, @EndDate DATE', %s, %s;
                                            
                         """ ,
                       'parameters': (startdate, enddate)

            },
         
         
               
         {
            'query_name': 'Age_Oldest_Sample',
            'sql': """
            
                                WITH SampleData AS (
                    SELECT 
                        [Date],
                        [Name_of_Lab],
                        [Sample_Type]+' '+[Test_Type] AS sample_type,
                        CAST([NEVERTESTED_Samples_in_Lab] AS NVARCHAR) + ' (' + CAST([Days_for_OLDEST_CarryoverSample] AS NVARCHAR) + ' days)' AS Sample_with_Days,
                        [comment]
                    FROM [LSS].[dbo].[Dash_Carryover_Sample_inventory]
                    WHERE   cast([date] as date) >= %s AND cast([date] as date) <= %s
                ),
                CommentData AS (
                    SELECT
                        [Name_of_Lab],
                        STRING_AGG([comment], '; ') AS Comments
                    FROM SampleData
                    GROUP BY [Name_of_Lab]
                )

                SELECT 
                    PivotTable.[Date],
                    PivotTable.[Name_of_Lab],
                    [Plasma VL],
                    [DBS VL],
                    [DBS EID],   
                    CommentData.Comments
                FROM 
                (
                    SELECT 
                        [Date],
                        [Name_of_Lab],
                        sample_type,
                        Sample_with_Days
                    FROM SampleData
                ) AS SourceTable

                PIVOT
                (
                    MAX(Sample_with_Days)
                    FOR sample_type IN ( [Plasma VL], [DBS VL],[DBS EID])
                ) AS PivotTable

                JOIN CommentData ON PivotTable.[Name_of_Lab] = CommentData.[Name_of_Lab]
                ORDER BY PivotTable.[Name_of_Lab];

                        
                         """ ,
                         'parameters': (startdate, enddate)

            },
         
            {
            'query_name': 'Stock_update_Table',
            'sql': """
            
            WITH   alldata as 
                (SELECT 
                    cast([Date] as date)  [Date],
                    CASE 
                        WHEN ([Platform_Roche_Abbott_Hologic_BMX] + ' ' + Test_Type) IN ('Roche C6800 VL', 'Roche C8800 VL', 'Roche C5800 VL') THEN 'Roche C6800 VL'
                        ELSE ([Platform_Roche_Abbott_Hologic_BMX] + ' ' + Test_Type)
                    END AS Platform,
                    SUM(cast([Reagent_tests_kits_Stock_on_hand] as float)) AS Total_Stock_on_hand
                FROM 
                    [LSS].[dbo].[Dash_Testing_Capacity]
                WHERE cast([date] as date) >= %s AND cast([date] as date) <= %s
                   and status='Lab'  
                GROUP BY 
                    cast([Date] as date),
                    CASE 
                        WHEN ([Platform_Roche_Abbott_Hologic_BMX] + ' ' + Test_Type) IN ('Roche C6800 VL', 'Roche C8800 VL', 'Roche C5800 VL') THEN 'Roche C6800 VL'
                        ELSE ([Platform_Roche_Abbott_Hologic_BMX] + ' ' + Test_Type)
                    END

                )  
                
                ,Stock_Update AS (
                                        SELECT  [Date]
                                            , [Platform]
                                            ,Total_Stock_on_hand  Number_of_tests_kits_Labs
                                            ,(select top 1 [No_of_test_kits_Central_level]
                                                FROM [LSS].[dbo].[Dash_Stock_Update] u
                                                WHERE u.[platform]=d.[Platform]
                                                ) [No_of_test_kits_Central_level]

                                                ,(select top 1 [Avg_Monthly_Consumption]
                                                FROM [LSS].[dbo].[Dash_Stock_Update] u
                                                WHERE u.[platform]=d.[Platform]
                                                ) [Avg_Monthly_Consumption]

                                                ,CASE
                                                WHEN d.[Platform]='Roche C6800 VL' THEN Total_Stock_on_hand*96
                                                WHEN d.[Platform]='Roche C8800 VL' THEN Total_Stock_on_hand*96
                                                WHEN d.[Platform]='Roche EID' THEN Total_Stock_on_hand*48
                                                WHEN d.[Platform]='Roche VL' THEN Total_Stock_on_hand*48
                                                WHEN d.[Platform]='Hologic Panther VL/EID' THEN Total_Stock_on_hand*100
                                                WHEN d.[Platform]='Abbott - room temperature VL' THEN Total_Stock_on_hand*96
                                                WHEN d.[Platform]='Abbott - cold chain VL' THEN Total_Stock_on_hand*96
                                                END Total_Number_of_tests_lab
                                            
                                        FROM alldata d
                
                                        ),

                        STOCK_LATEST AS (
                                        select *,
                                        CASE
                                                WHEN [Platform]='Roche C6800 VL' THEN (Number_of_tests_kits_Labs+[No_of_test_kits_Central_level])*96
                                                WHEN [Platform]='Roche EID' THEN (Number_of_tests_kits_Labs+[No_of_test_kits_Central_level])*48
                                                WHEN [Platform]='Roche VL' THEN (Number_of_tests_kits_Labs+[No_of_test_kits_Central_level])*48
                                                WHEN [Platform]='Hologic Panther VL/EID' THEN (Number_of_tests_kits_Labs+[No_of_test_kits_Central_level])*100
                                                WHEN [Platform]='Abbott - room temperature VL' THEN (Number_of_tests_kits_Labs+[No_of_test_kits_Central_level])*96
                                                WHEN [Platform]='Abbott - cold chain VL' THEN (Number_of_tests_kits_Labs+[No_of_test_kits_Central_level])*96
                                                END Total_Number_of_tests_Central_level_lab

                                                ,ROUND((cast(Total_Number_of_tests_lab as float)/[Avg_Monthly_Consumption] ),1)    Months_of_Stock_lab

                                                
                                        
                                        from Stock_Update

                                        )

                            SELECT 
                                CASE 
                                    WHEN [Platform]='Roche C6800 VL' THEN 'Roche C6800 /C8800 /C5800 VL'
                                    ELSE [Platform]
                                    END [Platform],
                                    Number_of_tests_kits_Labs,
                                    No_of_test_kits_Central_level,           
                                    [Avg_Monthly_Consumption],
                                    Total_Number_of_tests_lab,
                                    Total_Number_of_tests_Central_level_lab,
                                    Months_of_Stock_lab
                            
                            ,ROUND((cast(Total_Number_of_tests_Central_level_lab as float)/[Avg_Monthly_Consumption] ),1) Months_of_Stock_Central_Level_lab
                            FROM STOCK_LATEST
 
                """,
                      'parameters': (startdate, enddate)
            },
            



            {
            'query_name': 'Error_diff_Carry_and_Received',
            'sql': """
                      
                        -- Samples younger than 7 days(samples_in_lab - samples_in_backlog)<= Samples_this_Week :,
                        --- The diff can not be greater that samples received during the week- Justify why these are backlogs  

                select name_of_lab, sample_type,Test_Type, Nevertested_samples_in_lab, backlog_samples_intraTAT_7mo
                     ,cast(Nevertested_samples_in_lab as int)- cast(backlog_samples_intraTAT_7mo as int) diff_CarryandBack
                     ,(select top 1 Total_samples_received from Dash_This_week_Rec_Samples w
                       where w.name_of_lab=i.name_of_lab
                       and  w.sample_type=i.sample_type
                       and w.Test_Type=i.Test_Type
                       and w.date=i.date) thisWeekReceived

                from Dash_Carryover_Sample_inventory i
                 Where  (cast([date] as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                  AND cast([date] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                  order by name_of_lab
                
                """,
                      'parameters': ()
            },
            
            


           {
            'query_name': 'Hubs_uploaded_checks',
            'sql': """
                  
                        -- Intended for deletion of records with a specified condition
                         DELETE FROM [LSS].[dbo].[Creation_Reports_Hub_Lab]
                         WHERE [Referring_Lab] IS NOT NULL OR [Referring_Lab] != '';

                        WITH FormatDeterminer AS (
                            SELECT
                                SourceFile AS OGSOURCE,
                                CASE
                                        
                                    WHEN COUNT(CASE WHEN CHARINDEX('-', Date_Registered) > 0 AND LEN(Date_Registered) = 10 THEN 1 END) > 0 THEN 'YYYY-MM-DD'
                                    WHEN COUNT(CASE WHEN CHARINDEX('-', Date_Registered) > 0 AND LEN(Date_Registered) = 27 THEN 1 END) > 0 THEN 'YYYY-MM-DD'
                                    WHEN COUNT(CASE WHEN CHARINDEX('-', Date_Registered) = 0 AND CAST(PARSENAME(REPLACE(Date_Registered, '/', '.'), 3) AS INT) > 12 THEN 1 END) > 0 THEN 'DD/MM/YYYY'
                                    WHEN COUNT(CASE WHEN CHARINDEX('-', Date_Registered) = 0 AND CAST(PARSENAME(REPLACE(Date_Registered, '/', '.'), 2) AS INT) > 12 THEN 1 END) > 0 THEN 'MM/DD/YYYY'
                                    WHEN COUNT(CASE WHEN CHARINDEX('-', Date_Registered) > 0 AND LEN(Date_Registered) = 8 THEN 1 END) > 0 THEN 'DD-MM-YY'
                                    WHEN COUNT(CASE WHEN ISNUMERIC(Date_Registered) = 1 THEN 1 END) > 0 THEN 'Excel Integer'
                                    WHEN COUNT(CASE WHEN CHARINDEX('-', Date_Registered) = 0 AND CAST(PARSENAME(REPLACE(Date_Registered, '/', '.'), 3) AS INT) < 12 THEN 1 END) > 0    THEN 'D/M/YYYY'
                                    ELSE 'DD-MM-YY'
                                END AS DateFormat
                            FROM 
                                [LSS].[dbo].[Creation_Reports_Hub_Lab]
                            GROUP BY 
                                SourceFile
                        )

                        ,StandardizedDate AS (
                            SELECT  
                                t.*,
                                CASE 
                                    WHEN CHARINDEX('-', t.Date_Registered) > 0 AND LEN(t.Date_Registered) = 10 THEN t.Date_Registered
                                    WHEN f.DateFormat = 'DD-MM-YY' THEN '20' + RIGHT(t.Date_Registered, 2) + '-' + SUBSTRING(t.Date_Registered, 4, 2) + '-' + LEFT(t.Date_Registered, 2)
                                    WHEN f.DateFormat = 'DD/MM/YYYY' THEN RIGHT(t.Date_Registered, 4) + '-' + SUBSTRING(t.Date_Registered, CHARINDEX('/', t.Date_Registered) + 1, CHARINDEX('/', t.Date_Registered, CHARINDEX('/', t.Date_Registered) + 1) - CHARINDEX('/', t.Date_Registered) - 1) + '-' + LEFT(t.Date_Registered, CHARINDEX('/', t.Date_Registered) - 1)
                                    WHEN f.DateFormat = 'MM/DD/YYYY' THEN RIGHT(t.Date_Registered, 4) + '-' + LEFT(t.Date_Registered, CHARINDEX('/', t.Date_Registered) - 1) + '-' + SUBSTRING(t.Date_Registered, CHARINDEX('/', t.Date_Registered) + 1, CHARINDEX('/', t.Date_Registered, CHARINDEX('/', t.Date_Registered) + 1) - CHARINDEX('/', t.Date_Registered) - 1)
                                    WHEN f.DateFormat = 'Excel Integer' THEN CONVERT(DATE, DATEADD(DAY, CAST(Date_Registered AS INT), '1899-12-30'))
                                    WHEN f.DateFormat='D/M/YYYY' THEN  cast(t.Date_Registered as date)
                                    ELSE t.Date_Registered
                                END AS StandardizedDate
                            FROM 
                                [LSS].[dbo].[Creation_Reports_Hub_Lab] t
                            JOIN 
                                FormatDeterminer f ON t.SourceFile = f.OGSOURCE
                                
                        )
                            SELECT  

                                [SourceFile],
                                [Status],[user],
                                  count(*) Entries
                            FROM 
                                StandardizedDate c
                            Where  (cast(StandardizedDate as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                          AND cast(StandardizedDate as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                            group by    [SourceFile],   [Status],[user]
                            order by [user]                        


                
                """,
                      'parameters': ()
            },
            


             {
            'query_name': 'Natpharm_received',
            'sql': """
                   select name_of_lab,Test_Type,Platform_Roche_Abbott_Hologic_BMX,natpharm_kits_received_inthiswk

                FROM [LSS].[dbo].Dash_Testing_Capacity
                    where  status='Lab'
                    and   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                    AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                """,
                      'parameters': ()
            },
            



         #     {
         #    'query_name': 'LIMS_Utilization',
         #    'sql': """
         #               EXEC LIMS_FUNCTIONALITY @StartDate = %s, @EndDate = %s;
                
         #        """,
         #              'parameters': (startdate, enddate)
         #    },
            



          ]
    return SQLqueries





def odkScripts(startdate,enddate):
    parameters = (startdate,enddate)

    SQLqueries = [
       
            
                {
            'query_name': 'vl_received_count',
            'sql': """
                  EXEC [sp_vl_Samples_Received] @StartDate = %s, @EndDate=%s                    
                                            
                """,
                       'parameters': (startdate, enddate)
            },
    ]
    return SQLqueries






def odkScripts_granular(startdate, enddate, selected_provinces):
    # Converting tuple of provinces to a comma-separated string
    provinces_str = ','.join(selected_provinces)
    parameters = (startdate, enddate, provinces_str)

    SQLqueries = [
        {
            'query_name': 'odk_bike_granular',
            'sql': """
                EXEC [dbo].sp_ODK_RiderGranular @StartDate = %s, @EndDate = %s, @Provinces = %s
            """,
            'parameters': parameters
        },
    ]
    return SQLqueries



def odkScripts_province(startdate,enddate):
    parameters = (startdate,enddate)

    SQLqueries = [
    
       
        {
            'query_name': 'odk_bike_fxn_provincial',
            'sql': """
             EXEC [dbo].sp_ODK_Provincial_riders_per_month  @StartDate = %s, @EndDate=%s
            """ ,  # wrapping formatted_startdate with a tuple
            'parameters': (startdate,enddate)
       },
       
       
    ]
    return SQLqueries


def odkScripts_province_district(startdate,enddate,selected_provinces):
    # Converting tuple of provinces to a comma-separated string
    provinces_str = ','.join(selected_provinces)
    parameters = (startdate, enddate, provinces_str)

    SQLqueries = [
    
       
        {
            'query_name': 'odk_bike_fxn',
            'sql': """
             EXEC [dbo].sp_ODK_riders_per_month  @StartDate = %s, @EndDate=%s, @Provinces = %s
            """ ,  # wrapping formatted_startdate with a tuple
             'parameters': parameters
       },
       
       
    ]
    return SQLqueries
       
def odkScripts_province_totals(startdate, enddate, selected_provinces):
    # Converting tuple of provinces to a comma-separated string
    # Ensure this string is sanitized to prevent SQL injection
    provinces_str = ','.join(f"'{p}'" for p in selected_provinces)  
 
    SQLqueries = [
        {
            'query_name': 'odk_bike_totals',
            'sql': f"""
                SELECT 
                      [prov]

                    ,SUM (cast([res_vlblood] as int)) [res_vlblood]
                    ,SUM (cast ([res_vldbs] as int)) [res_vldbs]
                    ,SUM (cast([res_eid] as int)) [res_eid]
                    ,SUM (cast([res_urinelam] as int))  [res_urinelam]
                    ,SUM (cast ([res_sputumgxtruenat] as int)) [res_sputumgxtruenat]
                    ,SUM (cast([res_sputumzn] as int)) [res_sputumzn]
                    ,SUM (cast([res_sputumtbculdr] as int)) [res_sputumtbculdr]
                    ,SUM (cast([res_urine] as int))  [res_urine]
                    ,SUM (cast ([covid_19_res] as int)) [covid_19_res]
                    ,SUM(cast([num_other_res_1] as int)) [num_other_res_1]

                    ,SUM( cast([spec_vlblood] as int)) [spec_vlblood]
                    ,SUM (cast([spec_vldbs] as int)) [spec_vldbs]
                    ,SUM (cast ([spec_eid] as int)) [spec_eid]
                    ,SUM (cast( [spec_urinelam] as int)) [spec_urinelam]
                    ,SUM (cast([spec_sputumgxtruenat] as int)) [spec_sputumgxtruenat]
                    ,SUM (cast([spec_sputumzn] as int)) [spec_sputumzn]
                    ,SUM (cast([spec_sputumtbculdr] as int)) [spec_sputumtbculdr]
                    ,SUM (cast([spec_urine] as int)) [spec_urine]
                    ,SUM (cast([covid_19] as int))  [covid_19]
                    ,SUM (cast([num_spec_other_1] as int)) [num_spec_other_1]

                    ,SUM (cast([spec_sputum] as int)) [spec_sputum]
                    ,SUM ( cast([res_sputum] as int))[res_sputum]

                    ,SUM (cast([spec_sputumlam] as int)) [spec_sputumlam]
                    ,SUM (cast([spec_sputumgx] as int)) [spec_sputumgx]
                    ,SUM(cast([spec_sputumtbdr] as int)) [spec_sputumtbdr]
                    ,SUM (cast([res_sputumlam] as int)) [res_sputumlam]
                    ,SUM (cast([res_sputumgx] as int)) [res_sputumgx]
                    ,SUM (cast([res_sputumtbdr] as int)) [res_sputumtbdr]      

                FROM [LSS].[dbo].[ODK_Specimen_and_Results]
                WHERE cast(today as date) between %s and  %s
                and prov in  ({provinces_str}) 
                group by prov
            """ ,
            'parameters': (startdate, enddate)
        },
    ]
    return SQLqueries




def bike_fxnality_prov(startdate,enddate):
       
    SQLqueries = [
        {
            'query_name': 'bike_fxn_prov',
            'sql': """
   
             EXEC [dbo].[Get_Province_Bike_functionality]  @StartDate=%s, @EndDate=%s
            """ ,
            'parameters': (startdate,enddate)
        },     
        
      {
            'query_name': 'ist_getRxns_nonfunctional_Bfnaty',
            'sql': """
                   EXEC [dbo].[GetReasons_BikeNonFunctional]
                    @StartDate = %s,  @EndDate = %s                                      
                    
            """ ,  
            'parameters': (startdate,enddate)
       },    
        
        {
            'query_name': 'bike_fxn_District',
            'sql': """
   
             EXEC [dbo].[Get_DISTRICT_Bike_functionality]  @StartDate=%s, @EndDate=%s
            """ ,
            'parameters': (startdate,enddate)
        },  
        
          {
            'query_name': 'bike_fxn_GF',
            'sql': """
   
             EXEC [dbo].[GF_DISTRICT_Bike_functionality]  @StartDate=%s, @EndDate=%s
            """ ,
            'parameters': (startdate,enddate)
        },            
        
    ]
    return SQLqueries


def weeklyDashboardPull(startdate, enddate, lab):
    parameters = (startdate, enddate, lab)

    SQLqueries = [
        {
            'query_name': 'Dashboard_Indicators',
            'sql': """
                SELECT  * FROM  dashboard_indicators                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and LAB=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
        
         {
            'query_name': 'Dash_Carryover_Sample_inventory',
            'sql': """
                SELECT  * FROM  Dash_Carryover_Sample_inventory                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
         
         {
            'query_name': 'Dash_Age_Sex_Disaggregation',
            'sql': """
                SELECT  * FROM  Dash_Age_Sex_Disaggregation                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
         
         
         
         {
            'query_name': 'Dash_Specimen_Transport',
            'sql': """
                    WITH working_lab AS (
                    select unique_key,name_of_lab 
                    from Dash_This_week_Rec_Samples
                        WHERE 
                   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                  AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
                        and status='lab'
                        AND [Name_of_Lab] = %s
                        )                       
 
                   SELECT * 
                FROM Dash_Specimen_Transport
                WHERE 
                   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                  AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)

                   AND [unique_key]  in (select unique_key from  working_lab)
                
            """ ,'parameters': (lab,)
            
        },
         
         
         
          {
            'query_name': 'Dash_CLI_populate',
            'sql': """
              SELECT * 
                FROM Dash_CLI
                WHERE 
                   CAST([date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                  AND CAST([date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)

                    AND [Name_of_Lab] = %s
                    and status='lab'

        
                
            """ ,'parameters': (lab,)
            
        },
          
          {
            'query_name': 'Dash_Lab_Metrics_Waste_Mgt',
            'sql': """
                
                SELECT  
                   *
                FROM [LSS].[dbo].[Dash_Lab_Metrics_Waste_Mgt]                
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
          
           {
            'query_name': 'Dash_LIMS_Functionality',
            'sql': """
                SELECT  * FROM  Dash_LIMS_Functionality                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
           
             {
            'query_name': 'Dash_Power_Outage',
            'sql': """
                SELECT  * FROM  Dash_Power_Outage                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Laboratory]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
             
                 {
            'query_name': 'Dash_QMS',
            'sql': """
                SELECT  * FROM  Dash_QMS                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Lab_name]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
                 
        {
            'query_name': 'Dash_Referred_Samples',
            'sql': """
                SELECT  * FROM  Dash_Referred_Samples                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
        
           {
            'query_name': 'Dash_Sample_Run',
            'sql': """
                SELECT  * FROM  Dash_Sample_Run                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
           
            {
            'query_name': 'Dash_Testing_Capacity',
            'sql': """
                SELECT  * FROM  Dash_Testing_Capacity                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
            
             {
            'query_name': 'Dash_This_week_Rec_Samples',
            'sql': """
                SELECT  * FROM  Dash_This_week_Rec_Samples                   
                WHERE cast(Date as Date) >= %s AND cast(Date as Date)<= %s
                and [Name_of_Lab]=%s
            """ ,'parameters': (startdate, enddate, lab)
            
        },
             
             
    ]
    
    return SQLqueries



def get_update_query_and_params(row, row_idx):
    # Construct the query string
    query_string = """
        UPDATE dashboard_indicators
        SET  Number_of_carryover_samples= %s
            ,Backlog= %s
            ,Age_of_oldest_Plasma_sample= %s
            ,Age_of_oldest_DBS_sample= %s
            ,Age_of_oldest_EID_sample= %s
            ,Number_of_samples_received_this_week= %s
            ,Total_received_samples_with_Age_Sex_Disaggregation= %s
            ,Number_of_samples_that_are_rejected= %s
            ,Total_with_reasons_for_rejection= %s
            ,Number_of_samples_entered_into_LIMS_on_day_of_arrival= %s
            ,Total_number_of_referred_samples_received= %s
            ,Total_referred_with_reason_for_referral= %s
            ,Total_number_of_samples_run= %s
            ,Calculated_carryover_at_start_of_week_Plasma_VL= %s
            ,Calculated_carryover_at_start_of_week_DBS_VL= %s
            ,Calculated_carryover_at_start_of_week_DBS_EID= %s
            ,Total_number_failed_eligible_for_repeat= %s
            ,Total_number_failed_not_eligible_for_repeat= %s
            ,Number_with_reason_for_failure= %s
            ,Number_of_results_printed_from_LIMS_by_VL_Lab= %s
            ,Number_of_results_dispatched_by_lab= %s
            ,Reagent_test_kits_available= %s
            ,Total_machine_downtime_hours= %s
            ,LIMS_Hours_of_functionality= %s
            ,Number_of_hours_with_no_electricity= %s
            ,Number_of_hours_generator_was_on= %s

        WHERE unique_key = %s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB4"],
            row[f"row-{row_idx}-LAB5"],
            row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
            row[f"row-{row_idx}-LAB14"],           
            row[f"row-{row_idx}-LAB15"],
            row[f"row-{row_idx}-LAB16"],
            row[f"row-{row_idx}-LAB17"],
            row[f"row-{row_idx}-LAB18"],
            row[f"row-{row_idx}-LAB19"],
            row[f"row-{row_idx}-LAB20"],
            row[f"row-{row_idx}-LAB21"],
            row[f"row-{row_idx}-LAB22"],
            row[f"row-{row_idx}-LAB23"],
            row[f"row-{row_idx}-LAB24"],
            row[f"row-{row_idx}-LAB25"],
            row[f"row-{row_idx}-LAB26"],
            row[f"row-{row_idx}-LAB27"],
            row[f"row-{row_idx}-LAB28"],
            row[f"row-{row_idx}-LAB29"],        
        row[f"row-{row_idx}-LAB1"]
    )

    return query_string, params




def get_update_carryoversamples_and_params(row, row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_Carryover_Sample_inventory
        SET  
            NEVERTESTED_Samples_in_Lab= %s
            ,FAILED_Samples_in_Lab= %s
            ,BACKLOG_Samples_intraTAT_7mo= %s
            ,CARRYOVER_Samples_urgent= %s
            ,CARRYOVER_Samples_rebleeds= %s
            ,CARRYOVER_Samples_rejected= %s
            ,REJECTED_Quality_issue= %s
            ,REJECTED_Quantity_insuff= %s
            ,REJECTED_Patient_SampleINFO= %s
            ,REJECTED_Missing_requestForm= %s
            ,REJECTED_Sample_Missing= %s
            ,Days_for_OLDEST_CarryoverSample= %s
            ,Days_for_YOUNGEST_CarryoverSample= %s
            ,NUMBER_carryover_sample_TOO_OLD_test= %s
            ,NUMBER_carryover_samples_in_LIMS= %s
            ,Carry_Over_Sample_LIMS_Backlog_tobelogged= %s
            ,comment= %s
           

        WHERE unique_key = %s
        AND  [Sample_Type]= %s
        AND  [Test_Type]= %s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
       row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
            row[f"row-{row_idx}-LAB14"],           
            row[f"row-{row_idx}-LAB15"],
            row[f"row-{row_idx}-LAB16"],
            row[f"row-{row_idx}-LAB17"],
            row[f"row-{row_idx}-LAB18"],
            row[f"row-{row_idx}-LAB19"],
            row[f"row-{row_idx}-LAB20"],
            row[f"row-{row_idx}-LAB21"], 
           row[f"row-{row_idx}-LAB22"],  
            
        row[f"row-{row_idx}-LAB1"],
        row[f"row-{row_idx}-LAB4"],
        row[f"row-{row_idx}-LAB5"]
    )

    return query_string, params




def get_update_thisweek_and_params(row, row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_This_week_Rec_Samples
        SET         
            
            comments=%s,
            Total_samples_received=%s,
            Urgent_Samples_received=%s,
            Num_ReBleed_Samples=%s,
            Carryover_Samples_in_the_lab=%s,
            
            Samples_in_backlog_Intra_lab_TAT_7_days=%s,
            Num_Rejected_Samples=%s,
            REJECTED_too_old_to_test=%s,
            REJECTED_Quality_issue=%s,
            REJECTED_Quantity_insuff=%s,
            REJECTED_Quanti_Quali_intransit_compromised=%s,
            REJECTED_Patient_SampleINFO=%s,
            REJECTED_Missing_requestForm=%s,
            REJECTED_Sample_Missing=%s,
            REJECTED_other_reasons=%s,
            LIMS_hub_logged_prior_to_arrival=%s,
            LIMS_logged_during_week_of_arrival=%s,
            LIMs_Backlog_yetTObeEntered=%s,
            LIMS_Backlog_shipments_to_be_received=%s,
             checked = 'Reviewed on ' + CONVERT(VARCHAR, getdate())
           

        WHERE unique_key = %s
        AND  [Sample_Type]= %s
        AND  [Test_Type]= %s
        AND  [Name_of_Lab]= %s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB28"],
            row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],
            row[f"row-{row_idx}-LAB23"],

            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
            row[f"row-{row_idx}-LAB14"],
            row[f"row-{row_idx}-LAB15"],
            row[f"row-{row_idx}-LAB16"],
            row[f"row-{row_idx}-LAB17"],
            row[f"row-{row_idx}-LAB18"],
            row[f"row-{row_idx}-LAB19"],
            row[f"row-{row_idx}-LAB20"],
            row[f"row-{row_idx}-LAB21"],
            row[f"row-{row_idx}-LAB22"],
                            
             row[f"row-{row_idx}-LAB1"],
             row[f"row-{row_idx}-LAB4"],
             row[f"row-{row_idx}-LAB5"],
             row[f"row-{row_idx}-LAB3"]  
    )

    return query_string, params



def update_savingdashboard_sample_run(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_Sample_Run
        SET 
       
            Comments=%s,  

            RECEIVED_TOTAL_Sample_RUN=%s,
            RECEIVED_URGENT_Sample_RUN=%s,
            Controls_that_failed=%s,
            RECEIVED_FAILED_bt_NOT_Elig_REPEAT=%s,
            RECEIVED_FAILED_bt_Elig_REPEAT=%s,
            RECEIVED_REPEATS_RUN=%s,
            RECEIVED_FAILED_after_FINAL_repeat_testing=%s,

            RECEIVED_FAILED_sample_handling_error_lab=%s,       
            RECEIVED_FAILED_reagent_quality_issues=%s,
            RECEIVED_FAILED_QC_failure=%s,
            RECEIVED_FAILED_power_failure=%s,
            RECEIVED_FAILED_mechanical_failure=%s,
            FAILED_RECEIVED_sample_processing_error=%s,
            RECEIVED_FAILED_quality_quantity_issues=%s,
            RECEIVED_OTHER=%s,
            Sample_with_Valid_test_result=%s,
            Sample_with_FAILED_test_result=%s,
            RECEIVED_Results_dispatched_by_lab=%s
           

        WHERE unique_key = %s
        AND  [Sample_Type]= %s
        AND  [Test_Type]= %s
        AND  [Name_of_Lab]= %s
        and  [Platform_Roche_Abbott_Hologic_BMX]=%s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB25"],

            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],

            row[f"row-{row_idx}-LAB14"],
            row[f"row-{row_idx}-LAB15"],
            row[f"row-{row_idx}-LAB16"],
            row[f"row-{row_idx}-LAB17"],
            row[f"row-{row_idx}-LAB18"],
            row[f"row-{row_idx}-LAB19"],
            row[f"row-{row_idx}-LAB20"],
            row[f"row-{row_idx}-LAB21"],
            row[f"row-{row_idx}-LAB22"],
            row[f"row-{row_idx}-LAB23"],
            row[f"row-{row_idx}-LAB24"],
         
                            
             row[f"row-{row_idx}-LAB1"],
             row[f"row-{row_idx}-LAB4"],
             row[f"row-{row_idx}-LAB5"],
             row[f"row-{row_idx}-LAB3"],
             row[f"row-{row_idx}-LAB6"]    
    )

    return query_string, params





def update_savingdashboard_lims(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_LIMS_Functionality
        SET      

        Comments=%s, 
        Hours_of_Functionality=%s,
        Hours_of_Downtime=%s,
        Downtime_due_to_hardware_problem=%s,
        Downtime_due_to_internet_connection=%s,
        Downtime_due_to_software_problem=%s, 
        Downtime_Other_reasons=%s,
        TimeLIMS_going_down_HELPDESK=%s,
        TimeTechRESPONSE_Resolution=%s
           

        WHERE unique_key = %s
        AND  [Name_of_Lab]= %s
        and  [Day]=%s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
             row[f"row-{row_idx}-LAB28"],
            row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
         
                            
             row[f"row-{row_idx}-LAB1"],
             row[f"row-{row_idx}-LAB3"],
             row[f"row-{row_idx}-LAB2"]
           
    )

    return query_string, params




def update_savingdashboard_specimentransport(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_Specimen_Transport
        SET      

            Comments=%s,
            Number_of_rider_accidents=%s,
            incomplete_bike_transport_trips=%s,
            specimens_transported_by_non_IST_methods=%s,
            specimens_transported_by_ambulance=%s,
            specimens_transported_by_alternative_IP_transport=%s,
            specimens_transported_by_MoHCC_arranged_transport=%s,
            specimens_transported_by_courier=%s,
            specimens_transported_by_other_non_IST_methods=%s
           

        WHERE unique_key = %s
        AND  District= %s
       
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB28"],
            row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
         
                            
             row[f"row-{row_idx}-LAB1"],
             row[f"row-{row_idx}-LAB3"]
         
           
    )

    return query_string, params



def update_savingdashboard_power_outage(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_Power_Outage
        SET      

        Comments=%s, 
        Hours_with_no_electricity=%s,
        Hours_generator_was_on=%s,
        Fuel_ltrs_added_to_generator=%s,
        Hrs_Machine_idle_coz_PowerCuts=%s,
        Total_Tests_done_using_generator=%s
 
           

        WHERE unique_key = %s
     
        and  Laboratory=%s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
             row[f"row-{row_idx}-LAB28"],
            row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
         
                            
             row[f"row-{row_idx}-LAB1"],
         
             row[f"row-{row_idx}-LAB3"]
           
    )

    return query_string, params




def update_savingdashboard_testing_capacity(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_Testing_Capacity
        SET 
       
           Comments_Reagent_Stock_Status=%s,

            NatPharm_Kits_Received_inThisWK=%s,
            Date_Received_at_Lab=%s,
            Reagent_kits_to_OTHER_Labs=%s, 
            Lab_Name_Loaned_to=%s,
            Reagent_kits_RECEIVED_from_OTHER_Labs=%s, 
            Lab_Name_Received_from=%s,
            Reagent_tests_kits_Stock_on_hand=%s, 
            Reagent_tests_kits_available_Expiry_Date=%s,
            Tests_expired_this_month_before_use=%s,
            Control_loaned_to_other_labs=%s,
            Lab_name_receiving_controls=%s, 
            Controls_received_from_other_labs=%s,
            Lab_name_where_controls_from=%s,
            stock_of_control_available=%s,
            Expiry_Date_of_Controls=%s,
            Stocks_of_bottleneck_consumable_available=%s,
            Name_of_bottleneck_consumable=%s,
            Months_of_stock_of_bottleneck_consumable=%s

        WHERE unique_key = %s

        AND  [Test_Type]= %s
        AND  [Name_of_Lab]= %s
        and  [Platform_Roche_Abbott_Hologic_BMX]=%s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB25"],

            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
            row[f"row-{row_idx}-LAB14"],
            row[f"row-{row_idx}-LAB15"],
            row[f"row-{row_idx}-LAB16"],
            row[f"row-{row_idx}-LAB17"],
            row[f"row-{row_idx}-LAB18"],
            row[f"row-{row_idx}-LAB19"],
            row[f"row-{row_idx}-LAB20"],
            row[f"row-{row_idx}-LAB21"],
            row[f"row-{row_idx}-LAB22"],
            row[f"row-{row_idx}-LAB23"],
            row[f"row-{row_idx}-LAB24"],
         
                            
             row[f"row-{row_idx}-LAB1"],
           
             row[f"row-{row_idx}-LAB5"],
             row[f"row-{row_idx}-LAB3"],
             row[f"row-{row_idx}-LAB6"]    
    )

    return query_string, params




def update_savingdashboard_CLI(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_CLI
        SET 
       
            Comments=%s,
            Incidents_nid_Comms_btwn_LAB_CLINICS=%s,
            Number_of_correspondences=%s,
            ReBLEEDs_requests_sent=%s,
            Corrsp_test_requests_clinic_clarificatons=%s,
            Corrsp_clinic_send_spec_to_diff_lab=%s,
            Corrsp_clinic_expect_delay_due_lab_stockouts=%s,
            Corrsp_clinic_halt_spec_collection_due_lab_stockouts=%s,

            LAST_WEEK_Unresolved_missing_results=%s,       
            THIS_WEEK_Missing_results_req_by_Facilities=%s,

            RESOLVED_Missing_results_outcomes_found_Shared_with_facilities=%s,      
            RESOLVED_LIMS_Interface_Failed=%s,
            RESOLVED_Results_not_documented_at_facility=%s,
            RESOLVED_Results_sent_to_wrong_facility=%s,
            RESOLVED_Results_not_yet_dispatched_printed=%s,
            RESOLVED_Specimens_not_received_rebleed_sent=%s,        
            RESOLVED_Specimens_rejected=%s,

            Result_transmission_to_hub_failed=%s,        
            RESOLVED_Result_pending_publishing=%s,       
            UNRESOLVED_Pending_testing=%s,

            UNRESOLVED_Referred_awaiting_results=%s,
            UNRESOLVED_Investigation_in_progress=%s

        WHERE unique_key = %s   
        AND  [Name_of_Lab]= %s
  
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB28"],
            
            row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
            row[f"row-{row_idx}-LAB14"],
            row[f"row-{row_idx}-LAB15"],
            row[f"row-{row_idx}-LAB16"],
            row[f"row-{row_idx}-LAB17"],
            row[f"row-{row_idx}-LAB18"],
            row[f"row-{row_idx}-LAB19"],
            row[f"row-{row_idx}-LAB20"],
            row[f"row-{row_idx}-LAB21"],
            row[f"row-{row_idx}-LAB22"],
            row[f"row-{row_idx}-LAB23"],
            row[f"row-{row_idx}-LAB24"],
            row[f"row-{row_idx}-LAB25"], 
            row[f"row-{row_idx}-LAB26"],
         
                            
             row[f"row-{row_idx}-LAB1"],       
             row[f"row-{row_idx}-LAB3"]
             
    )

    return query_string, params




def update_savingdashboard_operational_matrix(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_Lab_Metrics_Waste_Mgt
        SET 
       
            Comments_on_ErrorCodes_for_Mach_failure=%s,
            Hrs_in_Shift=%s,
            Actual_number_of_days_platform_used=%s,
            Downtime_Power_Outage=%s,
            Downtime_Mechanical_Failure=%s,
            Downtime_Reagent_Stockout_Expiry=%s,
            Downtime_Controls_Stockout_Expiry=%s,
            Downtime_Controls_Failure=%s,
            Downtime_Staff_Unavailability=%s, 
            Downtime_coz_other_reasons=%s

     

        WHERE unique_key = %s

        AND  [Test_Type]= %s
        AND  [Name_of_Lab]= %s
        and  [Platform_Roche_Abbott_Hologic_BMX]=%s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB6"],

            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB8"],    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
            row[f"row-{row_idx}-LAB14"],
            row[f"row-{row_idx}-LAB15"],
             
                            
             row[f"row-{row_idx}-LAB1"],
           
             row[f"row-{row_idx}-LAB4"],
             row[f"row-{row_idx}-LAB3"],
             row[f"row-{row_idx}-LAB5"]    
    )

    return query_string, params


def update_savingdashboard_referred_samples(row,row_idx):
    # Construct the query string
    query_string = """
        UPDATE Dash_Referred_Samples
        SET 

         Comments=%s,
         Samples_reffered_Out=%s, 
         Lab_Samples_referred_to=%s,
         Swift_Consignment_Number=%s,
         
         Samples_Captured_thru_LIMS=%s,

         REFERRED_Reagent_Stockout=%s,
         REFERRED_Instrument_Failure=%s,
         REFERRED_Insuff_HR_Capacity=%s,
         REFERRED_Insuff_Instrument_Capacity=%s,
         REFERRED_Other_rexns=%s,
         Referred_Sample_Received=%s,
         Referred_From=%s,
         CARRYOVER_Referred_Sample_Received=%s,        
         URGENT_Referred_Samples=%s,
         REBLEED_Referred_Samples=%s,
         REJECTED_Referred_Samples=%s
           

        WHERE unique_key = %s
        AND  [Sample_Type]= %s
        AND  [Test_Type]= %s
        AND  [Name_of_Lab]= %s
        and [id]=%s
    """

    # Construct the parameters tuple using the dynamic row_idx
    params = (
            row[f"row-{row_idx}-LAB20"],
            row[f"row-{row_idx}-LAB6"],
            row[f"row-{row_idx}-LAB7"],
            row[f"row-{row_idx}-LAB22"],  
            row[f"row-{row_idx}-LAB8"],

    
            row[f"row-{row_idx}-LAB9"],
            row[f"row-{row_idx}-LAB10"],
            row[f"row-{row_idx}-LAB11"],
            row[f"row-{row_idx}-LAB12"],
            row[f"row-{row_idx}-LAB13"],
            row[f"row-{row_idx}-LAB14"],
            row[f"row-{row_idx}-LAB15"],
            row[f"row-{row_idx}-LAB16"],
            row[f"row-{row_idx}-LAB17"],
            row[f"row-{row_idx}-LAB18"],
            row[f"row-{row_idx}-LAB19"],
         
                            
             row[f"row-{row_idx}-LAB1"],
             row[f"row-{row_idx}-LAB4"],
             row[f"row-{row_idx}-LAB5"],
             row[f"row-{row_idx}-LAB3"] ,
             row[f"row-{row_idx}-LAB100"] 
              
    )

    return query_string, params





def delete_dashboard_entry(unique_key):
    # SQL query to delete the entry with the provided unique key from multiple tables
    query = """
    DECLARE @unique_key varchar(255) = %s;

    DELETE FROM [LSS].[dbo].[Dash_This_week_Rec_Samples] WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_Testing_Capacity WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_Sample_Run WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_Referred_Samples WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_QMS WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_Power_Outage WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_LIMS_Functionality WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_CLI WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_Carryover_Sample_inventory WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].Dash_Age_Sex_Disaggregation WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].[Dashboard_Indicators] WHERE unique_key = @unique_key;
    DELETE FROM [LSS].[dbo].[Dash_Lab_Metrics_Waste_Mgt] WHERE unique_key = @unique_key;
      DELETE FROM [LSS].[dbo].[Dash_Specimen_Transport]  WHERE unique_key = @unique_key;
    """

    params = [unique_key]

    return query, params




def delete_IST_Entry(unique_key):
    # SQL query to delete the entry with the provided unique key from multiple tables
    query = """
    DECLARE @unique_key varchar(255) = %s;
    DELETE FROM [LSS].[dbo].[IST_National] WHERE unique_key = @unique_key;   
    """

    params = [unique_key]

    return query, params


def delete_creation_reports(update_date,sourcefile,user,status):
   
    query = """
           DELETE 
           FROM [LSS].[dbo].[Creation_Reports_Hub_Lab]
           WHERE  [update_date]= CONVERT(VARCHAR, %s, 121)
           and SourceFile =%s
           and [user]=%s 
           and [status]=%s
    """

    params = [update_date,sourcefile,user,status]

    return query, params


def delete_vl_received_reports(update_date,sourcefile,user):
   
    query = """
           DELETE 
           FROM [LSS].[dbo].[fortnight_received]
           WHERE  [update_date]= CONVERT(VARCHAR, %s, 121)
           and SourceFile =%s
           and [user]=%s 
         
    """

    params = [update_date,sourcefile,user]

    return query, params



def files_uploaded_in_database(user):     
    query = """
           SELECT [update_date]
                ,[SourceFile]
                ,[Status]
                ,[user]
                ,count(*) qnty
            FROM [LSS].[dbo].[Creation_Reports_Hub_Lab]
            where [user]=%s
           and cast([update_date] as date) =cast(getdate() as date)
            --and CAST([update_date] AS DATE) >= DATEADD(DAY, -7, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
           -- AND CAST([update_date] AS DATE) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0)
            group by [update_date],[SourceFile],[Status],[user]
            """
    params = [user]  # list
    return query, params


def Dashboard_uploaded_in_database(user):     
    query = """
           SELECT  [LAB]     
                ,[Update_Date]
                ,[SourceFile]
                ,cast([Date] as date) [date]
                ,[unique_key]
            FROM [LSS].[dbo].[Dashboard_Indicators]
            Where REPLACE(LOWER([LAB]), ' ', '') like %s
             and CAST([date] AS DATE) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
            AND CAST([date] AS DATE) <= DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0) 
            """
    user_pattern = "%" + user + "%"
    params = [user_pattern]  # list
    return query, params




def vl_received_uploaded_in_database(user):     
    query = """
         SELECT [SourceFile]
      ,[user]
      ,CAST([update_date] AS DATE) [update_date],count(*) Entries
   FROM [LSS].[dbo].[fortnight_received]
   Where REPLACE(LOWER([user]), ' ', '') like %s
   and CAST([update_date] AS DATE) =cast(getdate() as date)
   group by [SourceFile]  ,[user],CAST([update_date] AS DATE)
   
            """
    user_pattern = "%" + user + "%"
    params = [user_pattern]  # list
    return query, params




def sms_uploaded_in_database(user):     
    query = """
         SELECT [SourceFile]
      ,[user]
      ,CAST([update_date] AS DATE) [update_date],count(*) Entries
   FROM [LSS].[dbo].[SMS_Data_DLR_Customer]
   Where REPLACE(LOWER([user]), ' ', '') like %s
   and CAST([update_date] AS DATE) =cast(getdate() as date)
   group by [SourceFile]  ,[user],CAST([update_date] AS DATE)
   
            """
    user_pattern = "%" + user + "%"
    params = [user_pattern]  # list
    return query, params


def pull_last_day():     
    query = """
        SELECT top 1 [CompletionDate] ,[SubmissionDate] ,[start] ,[end],[today] ,[prov]
            ,[dis] ,[rider_type] ,[rider],[regnum] ,[geopoint]  ,[month-year],[update_date]
        FROM [LSS].[dbo].[ODK_Specimen_and_Results]
        order by cast ([SubmissionDate] as date) desc    
        """

    params = []  # list
    return query, params

def pull_last_day_ist():     
    query = """
        select  top 1 cast([date] as date) submission_date
        from IST_National
        order by cast([date] as date) DESC    
        """

    params = []  # list
    return query, params

    

def ISTSpecimenQueries(startdate,enddate):
    parameters = (startdate,enddate)
    SQLqueries = [
         {
            'query_name': 'specimen_rejection',
            'sql': """
                        SELECT  FORMAT(cast([date] as date), 'MM-yyyy') as [month]   
                ,sum (cast([REJECTED_Missing_requestForm] as int))[REJECTED_Missing_requestForm]
                ,sum(cast([REJECTED_Sample_Missing] as int))[REJECTED_Sample_Missing]

                ,sum( cast([REJECTED_Quality_issue] as int)+
                cast([REJECTED_Quantity_insuff]as int)+
                cast([REJECTED_Patient_SampleINFO]as int)+
                cast([REJECTED_Missing_requestForm]as int)+
                cast( [REJECTED_Sample_Missing]as int)) total_rejected

                , (cast (sum (cast([REJECTED_Missing_requestForm] as int)) as float))/ 
                    
                    NULLIF ((sum( cast([REJECTED_Quality_issue] as int)+
                    cast([REJECTED_Quantity_insuff]as int)+
                    cast([REJECTED_Patient_SampleINFO]as int)+
                    cast([REJECTED_Missing_requestForm]as int)+
                    cast( [REJECTED_Sample_Missing]as int))),0) percent_missingForm

                , (cast (sum (cast([REJECTED_Sample_Missing] as int)) as float))/ 
                    
                    NULLIF ((sum( cast([REJECTED_Quality_issue] as int)+
                    cast([REJECTED_Quantity_insuff]as int)+
                    cast([REJECTED_Patient_SampleINFO]as int)+
                    cast([REJECTED_Missing_requestForm]as int)+
                    cast( [REJECTED_Sample_Missing]as int))),0) percent_sampleMissing

                    ,sum( cast([REJECTED_Quality_issue] as int)) [REJECTED_Quality_issue]
                    ,sum(cast([REJECTED_Quantity_insuff]as int)) [REJECTED_Quantity_insuff]
                    ,sum( cast([REJECTED_Patient_SampleINFO]as int)) [REJECTED_Patient_SampleINFO]         
                
            FROM [LSS].[dbo].[Dash_This_week_Rec_Samples]
            where cast(date as date) between %s and %s
            group by FORMAT(cast([date] as date), 'MM-yyyy') 
            order by FORMAT(cast([date] as date), 'MM-yyyy') desc             
              
                """ ,'parameters': (startdate,enddate)
            
        },
         
         
            {
            'query_name': 'specimen_rejection_weekly',
            'sql': """
                          SELECT  cast([date] as date) as [week]   
                ,sum (cast([REJECTED_Missing_requestForm] as int))[REJECTED_Missing_requestForm]
                ,sum(cast([REJECTED_Sample_Missing] as int))[REJECTED_Sample_Missing]

                ,sum( cast([REJECTED_Quality_issue] as int)+
                cast([REJECTED_Quantity_insuff]as int)+
                cast([REJECTED_Patient_SampleINFO]as int)+
                cast([REJECTED_Missing_requestForm]as int)+
                cast( [REJECTED_Sample_Missing]as int)) total_rejected

                , (cast (sum (cast([REJECTED_Missing_requestForm] as int)) as float))/ 
                    
                    NULLIF ((sum( cast([REJECTED_Quality_issue] as int)+
                    cast([REJECTED_Quantity_insuff]as int)+
                    cast([REJECTED_Patient_SampleINFO]as int)+
                    cast([REJECTED_Missing_requestForm]as int)+
                    cast( [REJECTED_Sample_Missing]as int))),0) percent_missingForm

                , (cast (sum (cast([REJECTED_Sample_Missing] as int)) as float))/ 
                    
                    NULLIF ((sum( cast([REJECTED_Quality_issue] as int)+
                    cast([REJECTED_Quantity_insuff]as int)+
                    cast([REJECTED_Patient_SampleINFO]as int)+
                    cast([REJECTED_Missing_requestForm]as int)+
                    cast( [REJECTED_Sample_Missing]as int))),0) percent_sampleMissing

                    ,sum( cast([REJECTED_Quality_issue] as int)) [REJECTED_Quality_issue]
                    ,sum(cast([REJECTED_Quantity_insuff]as int)) [REJECTED_Quantity_insuff]
                    ,sum( cast([REJECTED_Patient_SampleINFO]as int)) [REJECTED_Patient_SampleINFO]         
                
            FROM [LSS].[dbo].[Dash_This_week_Rec_Samples]
            where cast(date as date) between %s and %s
            
            group by cast([date] as date)
            order by cast([date] as date) desc          
              
                """ ,'parameters': (startdate,enddate)
            
        },


         {
            'query_name': 'specimen_rejection_bylab',
            'sql': """
                             SELECT  Name_of_lab as lab   
                ,sum (cast([REJECTED_Missing_requestForm] as int))[REJECTED_Missing_requestForm]
                ,sum(cast([REJECTED_Sample_Missing] as int))[REJECTED_Sample_Missing]

                ,sum( cast([REJECTED_Quality_issue] as int)+
                cast([REJECTED_Quantity_insuff]as int)+
                cast([REJECTED_Patient_SampleINFO]as int)+
                cast([REJECTED_Missing_requestForm]as int)+
                cast( [REJECTED_Sample_Missing]as int)) total_rejected

                , (cast (sum (cast([REJECTED_Missing_requestForm] as int)) as float))/ 
                    
                    NULLIF ((sum( cast([REJECTED_Quality_issue] as int)+
                    cast([REJECTED_Quantity_insuff]as int)+
                    cast([REJECTED_Patient_SampleINFO]as int)+
                    cast([REJECTED_Missing_requestForm]as int)+
                    cast( [REJECTED_Sample_Missing]as int))),0) percent_missingForm

                , (cast (sum (cast([REJECTED_Sample_Missing] as int)) as float))/ 
                    
                    NULLIF ((sum( cast([REJECTED_Quality_issue] as int)+
                    cast([REJECTED_Quantity_insuff]as int)+
                    cast([REJECTED_Patient_SampleINFO]as int)+
                    cast([REJECTED_Missing_requestForm]as int)+
                    cast( [REJECTED_Sample_Missing]as int))),0) percent_sampleMissing

                    ,sum( cast([REJECTED_Quality_issue] as int)) [REJECTED_Quality_issue]
                    ,sum(cast([REJECTED_Quantity_insuff]as int)) [REJECTED_Quantity_insuff]
                    ,sum( cast([REJECTED_Patient_SampleINFO]as int)) [REJECTED_Patient_SampleINFO]         
              
            FROM [LSS].[dbo].[Dash_This_week_Rec_Samples]
            where  
            cast(date as date) between %s and %s
            and status='lab'

            group by Name_of_lab
            order by Name_of_lab         
              
                """ ,'parameters': (startdate,enddate)
            
        },







         {
                'query_name': 'IST_line_list',
                'sql': """
                    
                   select *       from ist_national
                    where  cast([date] as date) >= %s AND cast([date] as date) <= %s
                   
                    """,
                       'parameters': (startdate, enddate)
                }, 
            
            
            {
            'query_name': 'missed_pickupLinelist',
            'sql': """
                    select  cast( [date] as date)  [date] ,
                        [Name_of_Rider_]
                        ,[Bike_Registration_Number]
                        ,[Province_]
                        ,[District_]
                        ,[Type_of_PEPFAR_Support]
                        ,[Bike_Functionality]
                        ,[Bike_breakdown_]
                        ,[Bike_on_routine_service_and_mainte0nce]
                        ,[Bike_had_no_fuel]
                        ,[Rider_on_COVID_19_isolation_quarantine]
                        ,[Rider_on_Sick_Leave]
                        ,[Rider_on_Leave]
                        ,[Rider_sticking_to_sample_pick_up_schedule]
                        ,[Inclement_weather]
                        ,[Accident_damaged_bike_vehicle]
                        ,[Clinical_IPs_related_issues]
                        ,[Other_Reasons__Specify_]
                        ,[Mitigation_measures_]
                        ,[Comments]
                        

                    from ist_national
                    where    cast(date as date) between %s and %s
                                --(cast([DATE] as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                    --AND cast([DATE] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                    and  [Number_of_Visits_to_Clinic_per_week]<   [Number_of__Scheduled_Visits_to_Clinic_per__Week]
         
              
                """ ,'parameters': (startdate,enddate)
            
        },








            
            
      
    ]
    return SQLqueries



def cdcweeklyreport(startdate,enddate):
    parameters = (startdate,enddate)
    SQLqueries = [
         {
            'query_name': 'vlreceived_model',
            'sql': """
                  EXEC   [dbo].[sp_vlreceived_model]        
              
                """ ,'parameters': ()
            
        },
         
         {
            'query_name': 'EIDreceived_model',
            'sql': """
                  EXEC   [dbo].[sp_EIDreceived_model]        
              
                """ ,'parameters': ()
            
        },
         
          {
            'query_name': 'Downtime_model',
            'sql': """
                  EXEC   [dbo].[sp_Downtime_model]        
              
                """ ,'parameters': ()
            
        },
          
           {
            'query_name': 'Referal_reasons_model',
            'sql': """
                  EXEC   [dbo].[sp_Referral_Reasons_model]        
              
                """ ,'parameters': ()
            
        },
           
            {
            'query_name': 'Referred_model',
            'sql': """
                  EXEC   [dbo].[sp_Referral_model]        
              
                """ ,'parameters': ()
            
        },
            
            
             {
            'query_name': 'SUPPLY_CHAIN_Ref',
            'sql': """
                  EXEC   dbo.sp_SUPPLYCHAIN_referral_model   @StartDate=%s, @EndDate=%s   
              
                """ ,'parameters': (startdate,enddate)
            
        },          
           
           
             
             {
            'query_name': 'supplu_chain_failures',
            'sql': """
                  EXEC   dbo.[sp_Supplychain_run_reasons_model]   @StartDate=%s, @EndDate=%s   
              
                """ ,'parameters': (startdate,enddate)
            
        },        
           
           
            {
            'query_name': 'lab_dispatch_model',
            'sql': """
                  EXEC   [dbo].[sp_lab_dispatch_model]        
              
                """ ,'parameters': ()
            
        },
         
           {
            'query_name': 'sample_runVL_model',
            'sql': """
                  EXEC   [dbo].[sp_VL_sample_run_model]        
              
                """ ,'parameters': ()
            
        },
           
            {
            'query_name': 'sample_runEID_model',
            'sql': """
                  EXEC   [dbo].[sp_sample_run_model]        
              
                """ ,'parameters': ()
            
        },
         
       
           
            {
            'query_name': 'VL_run_reasons_model',
            'sql': """
                  EXEC   [dbo].[sp_VL_sample_run_reasons_model]        
              
                """ ,'parameters': ()
            
        },
         
         
           {
            'query_name': 'EID_run_reasons_model',
            'sql': """
                  EXEC   [dbo].[sp_EID_sample_run_reasons_model]        
              
                """ ,'parameters': ()

        },
         
         
           {
            'query_name': 'power_outage_model',
            'sql': """
                  EXEC  [dbo].[sp_power_outage_model]       
              
                """ ,'parameters': ()            
        },
           
           
            {
            'query_name': 'reagent_expiary',
            'sql': """
                    SELECT   cast([Date] as date) [Date]
                                ,d.Lab
                                ,[Platform_Roche_Abbott_Hologic_BMX]+' '+[Test_Type] [platform]
                                ,[Reagent_tests_kits_Stock_on_hand]
                                , [Reagent_tests_kits_available_Expiry_Date] 

                                    
                            FROM   DATIM_Facility_names d
                            
                            Left join [LSS].[dbo].[Dash_Testing_Capacity] t on d.Facility=t.Name_of_Lab
                            where     (cast([DATE] as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                                    AND cast([DATE] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                            and t.status='lab'
                            and [Test_Type]!='TB'
              
                """ ,'parameters': ()
            
        },
         
         
      
    ]
    return SQLqueries




def ISTMOdelSQLQueries(start_date_r,end_date_r):
  

    SQLqueries = [
       {
            'query_name': 'rider_model',
            'sql': """
              SELECT
                [Name_of_Rider_],
                [Bike_Registration_Number],
                [Province_],
                [District_],
                [Type_of_PEPFAR_Support],

                cast([vl_plasma_sam] as float ) AS vl_plasma_sam,
                cast([vl_dbs_sam] as float ) AS vl_dbs_sam,
                cast([eid_sam] as float ) AS eid_sam,
                cast([eid_dbs] as float ) AS eid_dbs,
                cast ([sputum_sam] as float ) AS sputum_sam,
                cast([Sputum_Culture_DR_NTBRL] as float ) AS Sputum_Culture_DR_NTBRL,
                cast([HPV] as float ) AS HPV,

                [other_sam],
                cast([vl_plasma_res] as float ) AS vl_plasma_res,
                cast([vl_dbs_res] as float ) AS vl_dbs_res,
                cast([eid_res] as float ) AS eid_res,
                cast([eid_dbs_res] as float ) AS eid_dbs_res,
                cast([sputum_res] as float ) AS sputum_res,
                cast([Sputum_Culture_DR_NTBRL_res] as float ) AS Sputum_Culture_DR_NTBRL_res,
                cast([HPV_res] as float ) AS HPV_res,

                [other_res] AS other_res,
                cast([Fuel_allocated_to_rider_per_week] as float ) AS Fuel_allocated_to_rider_per_week,
                cast([Fuel_used_by_rider_per_week] as float ) AS Fuel_used_by_rider_per_week,
                cast([Distance_travelled_by_rider_per_week] as float ) AS Distance_travelled_by_rider_per_week,
                cast([Number_of_days_bike_was_functional] as float ) AS Number_of_days_bike_was_functional,
                cast([Number_of__Scheduled_Visits_to_Clinic_per__Week] as float ) AS Number_of__Scheduled_Visits_to_Clinic_per__Week,
                cast([Number_of_Visits_to_Clinic_per_week] as float ) AS Number_of_Visits_to_Clinic_per_week,
                cast([Bike_breakdown_] as float ) AS Bike_breakdown_,
                cast([Bike_on_routine_service_and_mainte0nce] as float ) AS Bike_on_routine_service_and_mainte0nce,
                cast([Bike_had_no_fuel] as float ) AS Bike_had_no_fuel,
                cast([Rider_on_Sick_Leave] as float ) AS Rider_on_Sick_Leave,
                cast([Rider_on_Leave] as float ) AS Rider_on_Leave,
                cast([Inclement_weather] as float ) AS Inclement_weather,
                cast([Accident_damaged_bike_vehicle] as float ) AS Accident_damaged_bike_vehicle,

                cast([Clinical_IPs_related_issues] as float ) AS Clinical_IPs_related_issues,
                [Other_Reasons__Specify_] AS Other_Reasons__Specify_,
                [Mitigation_measures_] AS Mitigation_measures_,
                [Comments] AS Comments
            FROM
                ist_NATIONAL
            WHERE
                -- CAST([DATE] AS date) BETWEEN '2023-12-18' AND '2024-01-07'
                 (cast([DATE] as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                AND cast([DATE] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                AND [status] = 'rider'
            order by  [Province_]


            """ ,  
            'parameters': ()
       },
       
        {
            'query_name': 'reliefrider_model',
            'sql': """
            
                SELECT
                [Name_of_Rider_],
                [Bike_Registration_Number],
                [Province_],
                [District_],
                [Type_of_PEPFAR_Support],

                cast([vl_plasma_sam] as float ) AS vl_plasma_sam,
                cast([vl_dbs_sam] as float ) AS vl_dbs_sam,
                cast([eid_sam] as float ) AS eid_sam,
                cast([eid_dbs] as float ) AS eid_dbs,
                cast ([sputum_sam] as float ) AS sputum_sam,
                cast([Sputum_Culture_DR_NTBRL] as float ) AS Sputum_Culture_DR_NTBRL,
                cast([HPV] as float ) AS HPV,

                [other_sam] AS other_sam,
                cast([vl_plasma_res] as float ) AS vl_plasma_res,
                cast([vl_dbs_res] as float ) AS vl_dbs_res,
                cast([eid_res] as float ) AS eid_res,
                cast([eid_dbs_res] as float ) AS eid_dbs_res,
                cast([sputum_res] as float ) AS sputum_res,
                cast([Sputum_Culture_DR_NTBRL_res] as float ) AS Sputum_Culture_DR_NTBRL_res,
                cast([HPV_res] as float ) AS HPV_res,

                [other_res] AS other_res,
                cast([Fuel_allocated_to_rider_per_week] as float ) AS Fuel_allocated_to_rider_per_week,
                cast([Fuel_used_by_rider_per_week] as float ) AS Fuel_used_by_rider_per_week,
                cast([Distance_travelled_by_rider_per_week] as float ) AS Distance_travelled_by_rider_per_week,
                cast([Number_of_days_bike_was_functional] as float ) AS Number_of_days_bike_was_functional,
                cast([Number_of__Scheduled_Visits_to_Clinic_per__Week] as float ) AS Number_of__Scheduled_Visits_to_Clinic_per__Week,
                cast([Number_of_Visits_to_Clinic_per_week] as float ) AS Number_of_Visits_to_Clinic_per_week,
                cast([Bike_breakdown_] as float ) AS Bike_breakdown_,
                cast([Bike_on_routine_service_and_mainte0nce] as float ) AS Bike_on_routine_service_and_mainte0nce,
                cast([Bike_had_no_fuel] as float ) AS Bike_had_no_fuel,
                cast([Rider_on_Sick_Leave] as float ) AS Rider_on_Sick_Leave,
                cast([Rider_on_Leave] as float ) AS Rider_on_Leave,
                cast([Inclement_weather] as float ) AS Inclement_weather,
                cast([Accident_damaged_bike_vehicle] as float ) AS Accident_damaged_bike_vehicle,
                cast([Clinical_IPs_related_issues] as float ) AS Clinical_IPs_related_issues,
                
                [Other_Reasons__Specify_] AS Other_Reasons__Specify_,
                [Mitigation_measures_] AS Mitigation_measures_,
                [Comments] AS Comments
            FROM
                ist_NATIONAL
            WHERE
                -- CAST([DATE] AS date) BETWEEN '2023-12-18' AND '2024-01-07'
                (cast([DATE] as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                AND cast([DATE] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                and  [status] ='relief rider'
                
            
               
            """ , 
            'parameters': ()
       },
        
         {
            'query_name': 'driver_model',
            'sql': """
              
               SELECT 
                [Driver_Sample_Status],
                [Name_of_Rider_],  
                [Bike_Registration_Number],
                [Province_],
                [District_],

                cast([vl_plasma_sam] as float) AS vl_plasma_sam,
                cast([vl_dbs_sam] as float) AS vl_dbs_sam,
                cast([eid_sam] as float) AS eid_sam,
                cast([eid_dbs] as float) AS eid_dbs,
                cast([sputum_sam] as float) AS sputum_sam,
                cast([Sputum_Culture_DR_NTBRL] as float) AS Sputum_Culture_DR_NTBRL,
                cast([HPV] as float) AS HPV,
                [other_sam] AS other_sam,

                cast([vl_plasma_res] as float) AS vl_plasma_res,
                cast([vl_dbs_res] as float) AS vl_dbs_res,
                cast([eid_res] as float) AS eid_res,
                cast([eid_dbs_res] as float) AS eid_dbs_res,
                cast([sputum_res] as float) AS sputum_res,
                cast([Sputum_Culture_DR_NTBRL_res] as float) AS Sputum_Culture_DR_NTBRL_res,
                cast([HPV_res] as float) AS HPV_res,
                [other_res] AS other_res,

                cast([Fuel_allocated_to_rider_per_week] as float) AS Fuel_allocated_to_rider_per_week,
                cast([Fuel_used_by_rider_per_week] as float) AS Fuel_used_by_rider_per_week,
                cast([Distance_travelled_by_rider_per_week] as float) AS Distance_travelled_by_rider_per_week,
                cast([Number_of_days_bike_was_functional] as float) AS Number_of_days_bike_was_functional,
                cast([Number_of__Scheduled_Visits_to_Clinic_per__Week] as float) AS Number_of__Scheduled_Visits_to_Clinic_per__Week,
                cast([Number_of_Visits_to_Clinic_per_week] as float) AS Number_of_Visits_to_Clinic_per_week,
                cast([Bike_breakdown_] as float) AS Bike_breakdown_,
                cast([Bike_on_routine_service_and_mainte0nce] as float) AS Bike_on_routine_service_and_mainte0nce,
                cast([Bike_had_no_fuel] as float) AS Bike_had_no_fuel,
                cast([Rider_on_Sick_Leave] as float) AS Rider_on_Sick_Leave,
                cast([Rider_on_Leave] as float) AS Rider_on_Leave,
                cast([Inclement_weather] as float) AS Inclement_weather,
                cast([Accident_damaged_bike_vehicle] as float) AS Accident_damaged_bike_vehicle,
                cast([Clinical_IPs_related_issues] as float) AS Clinical_IPs_related_issues,
                
                [Other_Reasons__Specify_] AS Other_Reasons__Specify_,
                [Mitigation_measures_] AS Mitigation_measures_,
                [Comments] AS Comments
            FROM 
                ist_NATIONAL
                Where  
                (cast([DATE] as date) >= DATEADD(DAY, -6, DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                AND cast([DATE] as date) < DATEADD(DAY, DATEDIFF(DAY, 0, GETDATE()), 0))
                and  [status] ='Driver'               
             
                order by [Driver_Sample_Status]
              
              
              
            """ , 
            'parameters': ()
       },
         
        
       
    ]
    return SQLqueries