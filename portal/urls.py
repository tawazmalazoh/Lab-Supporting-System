from django.urls import include, path
from . import views
from django.urls import path, re_path





urlpatterns = [
        path('', views.login_user, name='login'),
        path('login/', views.login_user, name='login'),
        path('logout/', views.logout_user, name='logout'),
        path("BASE", views.BASE, name='BASE'),        
        path('index_SIE/', views.index_sie, name='index_SIE'),  

        path('explore/<path:folder_name>/', views.explore_folder, name='explore_folder'),
        path('explore/', views.explore_folder, name='explore'),
        
        
        
        path('explore_Admin/<path:folder_name>/', views.explore_folder_Admin, name='explore_Admin'),
        path('explore_Admin/', views.explore_folder_Admin, name='explore_Admin'),
                

        path('upload_other_files/', views.upload_files, name='upload_other_files'),
        path('upload_HUB_files/', views.HUBupload_files, name='upload_HUB_files'),
        path('upload_IST_files/', views.ISTupload_files, name='upload_IST_files'),
         path('upload_SMS_files/', views.SMSupload_files, name='upload_SMS_files'),
        
        
        path('Supply_chain_view/', views.Supply_chain_view, name='Supply_chain_view'),
        path('CDC_narrative_word/', views.CDC_narrative_word, name='CDC_narrative_word'),
        
        path('IST_excel_file_view/',views.IST_excel_file_view, name='IST_excel_file_view'),
        
        path('excel_file_handler/', views.excel_file_handler, name='excel_file_handler'),
        path('download_excel/', views.download_excel, name='download_excel'),
          path('download/<str:file_name>/', views.download_file, name='download_file'),
          
        path('Home_generate_excel/',views.Home_generate_excel,name='Home_generate_excel'),
        path('cdc_list_files/', views.cdc_list_files, name='cdc_list_files'),
          
              
        path('rtcql/', views.rtcql_view, name='rtcql'),
        path('ist/', views.ist_view, name='ist'),
        path('cli/', views.cli_view, name='cli'),
        path('admin_manage/', views.admin_manage, name='admin_manage'),
        path('verify_admin_password/', views.verify_admin_password, name='verify_admin_password'),
        
        
        # path('delete_weekly_dashboard/', views.delete_weekly_dashboard, name='delete_weekly_dashboard'),
        path('viral_load/', views.viral_load_received, name='viral_load'),
         path('fetch_province_data/', views.fetch_province_data, name='fetch_province_data'),
         
        
        path('update_dashboard_CLI/', views.update_dashboard_CLI, name='update_dashboard_CLI'),
         path('update_dashboard_power_outage/', views.update_dashboard_power_outage, name='update_dashboard_power_outage'),
        path('update_dashboard_Specimen_Transport/', views.update_dashboard_Specimen_Transport, name='update_dashboard_Specimen_Transport'),
        path('update_dashboard_lims/', views.update_dashboard_lims, name='update_dashboard_lims'),
        path('update_dashboard_operational_matrix/', views.update_dashboard_operational_matrix, name='update_dashboard_operational_matrix'),
          path('update_dashboard_testing_capasity/', views.update_dashboard_testing_capasity, name='update_dashboard_testing_capasity'),
        path('update_dashboard_sample_run/', views.update_dashboard_sample_run, name='update_dashboard_sample_run'),
        path('update_dashboard_referred_samples/', views.update_dashboard_referred_samples, name='update_dashboard_referred_samples'),
        path('update_dashboard_thisweek_sample/', views.update_dashboard_thisweek_sample, name='update_dashboard_thisweek_sample'),
        
        path('delete_Dashboard_Entry/', views.delete_Dashboard_Entry, name='delete_Dashboard_Entry'),
        path('delete_ist_Entry/', views.delete_ist_Entry, name='delete_ist_Entry'),
        path('delete_record_sie/', views.delete_record_sie, name='delete_record_sie'),
        path('delete_record_vl_received/', views.delete_record_vl_received, name='delete_record_vl_received'),
        path('odk_specimen_and_results_sync/', views.odk_specimen_and_results_sync, name='odk_specimen_and_results_sync'),
        
        
        path('IST_specimens_and_results',views.IST_specimens_and_results,name='IST_specimens_and_results'),    
        
        path('relief_spec_and_res',views.relief_spec_and_res,name='relief_spec_and_res'), 
        path('shipments',views.shipments,name='shipments'),  
        path('sample_rejections',views.sample_rejections, name='sample_rejections'),
       
        
        
        path('specimens_results/', views.specimens_results, name='specimens_results'),
        path('overalTAT/', views.overalTAT, name='overalTAT'),
        path('provincelevelTAT', views.provincelevelTAT, name='provincelevelTAT'),
        
        path('odk_fxn_view', views.odk_fxn_view, name='odk_fxn_view'),
        path('odk_fxnProvince/', views.odk_fxnProvince, name='odk_fxnProvince'),
        path('odk_summaries/', views.odk_summaries, name='odk_summaries'),
        
        # path('SIE_odk_fxn_view', views.SIE_odk_fxn_view, name='SIE_odk_fxn_view'),
        # path('SIE_odk_fxnProvince/', views.SIE_odk_fxnProvince, name='SIE_odk_fxnProvince'),
        path('ridersgranular/', views.ridersgranular , name='ridersgranular'),
        
        
        
        # The download_folder URL pattern
        path('download_folder/<str:folder_path>/', views.download_folder, name='download_folder'),
        path('download_folder/<path:folder_path>/', views.download_folder, name='download_folder'),
        path('download_folder', views.download_folder, name='download_folder'),

              
        
        path('weeklyNarrative_view/', views.weeklyNarrative_view, name='weeklyNarrative_view'),
        
        path('bike_function_view/', views.bike_function_view, name='bike_function_view'),
      
        #path('download_istaggcsv/', views.download_istaggcsv, name='download_istaggcsv')
        
        path('download_query_output/', views.download_query_output, name='download_query_output'),
           path('admintools/', views.adminMETools_view, name='admintools'),
           
        
   re_path(r'^delete-file/(?P<file_path>.*)/$', views.delete_file, name='delete_file'),
   re_path(r'^delete-folder/(?P<folder_path>.*)/$', views.delete_folder, name='delete_folder'),
   
   



]
