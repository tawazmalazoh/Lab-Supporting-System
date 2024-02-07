
$(document).ready(function(){

   //Lets stop default submit behaviour of buttons
    $('[data-toggle="popover"]').popover();

    $("#btnDeleteIVQ").hide();

   $("#example1").DataTable({
      "responsive": true, "lengthChange": false, "autoWidth": false,
      "buttons": ["copy", "csv", "excel", "pdf", "print", "colvis"]
    }).buttons().container().appendTo('#example1_wrapper .col-md-6:eq(0)');

    $('#example2').DataTable({
      "paging": true,
      "lengthChange": false,
      "searching": false,
      "ordering": true,
      "info": true,
      "autoWidth": false,
      "responsive": true,
    });

    $('#tblDocuments').DataTable( {
        dom: 'Bfrtip',
        buttons: [
            'copy', 'csv', 'excel', 'pdf', 'print',
             {
                text: 'Upload New Document',
                action: function ( e, dt, node, config ) {
                    alert( 'Button activated' );
                }
            },
            'colvis'
        ]
    } );

    $('#tblUserManager').DataTable( {
        dom: 'Bfrtip',
        "ordering": false,
        buttons: [
            'copy', 'csv', 'excel', 'pdf', 'print','colvis',
             {
                text: 'Add New User',
                action: function ( e, dt, node, config ) {
                    $("#modalNewUser").modal({ backdrop: "static ", keyboard: false });
                }
            }
        ]
    } );

    $('#tblSoapChecklists').DataTable( {
        dom: 'Bfrtip',
        "ordering": false,
        buttons: [
             'excel',
             {
                text: 'Generate Soap Checklist',
                action: function ( e, dt, node, config ) {
                    $("#modalGenerateSoapChecklist").modal({ backdrop: "static ", keyboard: false });
                }
            }
        ]
    } );

    $('#BasicDatatable').DataTable( {
        dom: 'Bfrtip',
        "pageLength": 30,
        buttons: [
            'copy', 'csv', 'excel', 'pdf', 'print','colvis'             
        ]
    } );

    $('.ReportTable').DataTable( {
        dom: 'Bfrtip',
        "lengthChange": false,
        "searching": false,
        "ordering": false,
        buttons: [
            'copy','excel','colvis'             
        ]
    } );

    $('.BasicDatatable').DataTable( {
        dom: 'Bfrtip',
        "pageLength": 30,
        "searching": true,
        buttons: [
            'copy', 'csv', 'excel', 'pdf', 'print','colvis'             
        ]
    } );

    $('#BasicDatatableNoSort').DataTable( {
        dom: 'Bfrtip',
        "pageLength": 30,
        "ordering": false,
        buttons: [
            'copy', 'csv', 'excel', 'pdf', 'print','colvis'             
        ]
    } );

    $('.BasicDatatableNoOptions').DataTable( {
        dom: 'Bfrtip',
        "pageLength": 30,
        "searching": false,
        "ordering": false
    } );

    $('#tblRAEffort').DataTable( {
        dom: 'Bfrtip',
        "pageLength": 6,
        "ordering": false,
        "searching": false,
        buttons: [
            'copy', 'excel', 'pdf', 'print'             
        ]
    } );    

       
    $("#tblInActiveSystemUsers").DataTable({
      "responsive": true, "lengthChange": false, "autoWidth": false,
      "buttons": ["copy", "csv", "excel", "pdf", "print", "colvis"]
    }).buttons().container().appendTo('#tblSchoolTerms .col-md-6:eq(0)');

    $('#tblAirtimePurchaseHistory').DataTable( {
        dom: 'Bfrtip',
        "searching": false,
        "pageLength": 12,
        buttons: [
            'copy', 'csv', 'excel', 'pdf', 'print',
             {
                text: 'New Airtime Purchase',
                action: function ( e, dt, node, config ) {
                    $("#modalNewAirtimePurchase").modal({ backdrop: "static ", keyboard: false });
                }
            },
            'colvis'
        ]
    } );

    //Lets initialise Date Pickers

    $('.DateMaxDateToday').datepicker({
        dateFormat: "yy-mm-dd",
        maxDate: '0'
    });



    $('.DateMinDateToday').datepicker({
        dateFormat: "yy-mm-dd",
        minDate: '0'
    });

    $("#student_disability").change(function(){
        
        SelectValue = $("#student_disability").val();

        if (SelectValue==1){
           $("#student_conditions").prop('disabled',false);
        }else{
           $("#student_conditions").prop('disabled',true);
           $("#student_conditions").val('');
        }

    });

    $("#btnSearchStudentInfo").click(function(){
      
        if ($("#txtStudentRegNumber")==''){
          alert('Please enter Student Registration Number');
          $("#txtStudentRegNumber").focus();
        }else{
           SearchStudentInfo();
        }
    });

    

    $("#btnGeneratePWD").click(function(){
        $("#btnGeneratePWD").prop('disabled',true);
        $("#btnCreateUser").prop('disabled',true);
        
        $("#btnGeneratePWD").html('<i class="fa fa-spinner fa-spin"></i>&nbspGenerating Password');
            $.ajax({
              type: "POST",
              url: "http://localhost/portal/settings/GenerateRandomUserPassword",
              cache: false, 
              data: {},
              dataType: "json",
              success: function(data) {                  
                $("#txtPWD").val(data.GeneratedPWD);
                $("#txtCPWD").val(data.GeneratedPWD);
                $("#btnGeneratePWD").prop('disabled',false);
                $("#btnCreateUser").prop('disabled',false);
                $("#btnGeneratePWD").html('Generate Password');
              },

              error: function(xhr, status, error){   

                 DisplayErrorMessage('System encountered an error. Password could not be generated : '+status);
                 $("#btnGeneratePWD").prop('disabled',false);
                 $("#btnCreateUser").prop('disabled',false);
                 $("#btnGeneratePWD").html('Generate Password');
                 
              }
             }); //END OF AJAX

});

    $("#btnCreateUser").click(function(){

          FirstName = $("#txtFirstName").val();
          LastName  = $("#txtLastName").val();
          Email  = $("#txtEmail").val();
          Mobile  = $("#txtMobile").val();
          UserName  = $("#txtUserName").val();
          Role  = $("#lstUserGroup").val();
          PWD  = $("#txtPWD").val();
          CPWD  = $("#txtCPWD").val();
          Occupation  = $("#txtOccupation").val();
          NationalID = $("#txtNationalID").val();


          // lets validate the data
          if (FirstName==""){

                DisplayWarningMessage('First name is required');
                $("#txtFirstName").focus();

          }else if(LastName==""){

                DisplayWarningMessage('Last name is required');
                $("#txtFirstName").focus();
               
          }else if(Occupation==""){
               
                DisplayWarningMessage('Enter position title of employee');
                $("#txtOccupation").focus();

          }else if (Mobile=="" || isNaN(Mobile) || Mobile.startsWith("+") || Mobile.startsWith("0") || Mobile.length != 12){
                DisplayWarningMessage('Enter a valid mobile number. Make sure you have put a valid mobile number with full international code without + eg 263');
                $("#txtMobile").focus();

          }else if (UserName==""){
               
                DisplayWarningMessage('Assign employee code');
               $("#txtUserName").focus();

          }else if(Role==0){

                DisplayWarningMessage('Select employee access rights group');
                $("#lstUserGroup").focus();

          }else if(PWD==""){
                
              DisplayWarningMessage('Assign use password or autogenerate user password');
              $("#txtPWD").focus();

          }else if(CPWD==""){
               
              DisplayWarningMessage('Please confirm user password');
              $("#txtCPWD").focus();

          }else if (PWD != CPWD){
               
              DisplayWarningMessage('Entered password and confirmation are not equal');
              $("#txtCPWD").focus();

          }else{
            // Validation was successful. Lets post the user data and create their account
             $("#btnCreateUser").html('<i class="fa fa-spinner fa-spin"></i>&nbspCreating Account');
             $("#btnCreateUser").prop('disabled',true);
             $.ajax({
              type: "POST",
              url: "http://localhost/portal/account/CreateUserAccount",
              cache: false, 
              data: {'FirstName':FirstName,'LastName':LastName,'Email':Email,'Mobile':Mobile,'UserName':UserName,'Role':Role,'PWD':PWD,'Occupation':Occupation,'NationalID':NationalID},
              dataType: "json",
              success: function(data) {                  
                  if (data.UserAdded){
                     $( "#tblUserManager" ).load( "http://localhost/portal/administration/users #tblUserManager" );
                       $("#btnCreateUser").html('Add User');
                       $("#btnCreateUser").prop('disabled',false);                     
                      
                      DisplaySuccessMessage('New user successfully added. User can now log in');
          
                  }else{       
                     $("#btnCreateUser").html('Add User');
                     $("#btnCreateUser").prop('disabled',false);     
                     DisplayWarningMessage('User could not be added please try again');                    
                   
                  }     
              },

              error: function(xhr, status, error){     

                $("#btnCreateUser").html('Add User');
                $("#btnCreateUser").prop('disabled',false);
                DisplayErrorMessage('System encountered an error processing. Please try again: Error : '+error);
                
                   
              }
             }); //END OF AJAX
          }


        });

        $("#lstMobileNetwork").change(function(){

              RACode = $("#txtRACode").val();
              RANetwork = $("#lstMobileNetwork").val();

              if (RANetwork==0){
                 DisplayToastAlert('danger','System Notification','Please select the Mobile Network you are topping up for the RA');
                 $("#lstMobileNetwork").focus();
              }else{
                  $.ajax({
                      type: "POST",
                      url: "http://localhost/portal/round9/workflow/GetRAAssignedMobileInfo",
                      cache: false, 
                      data: {'RACode':RACode,'RANetwork':RANetwork},
                      dataType: "json",
                      success: function(data) {
                          
                          $("#txtRAMobileNumber").val(data.MobileNumberToRecharge);                  

                      },

                      error: function(xhr, status, error){   
                             DisplayErrorMessage('Error loading RA Mobile Information : '+error);       
                      }
                  }); //END OF AJAX

              }

          });

        /// This function processes airtime topup to the RA's Account
          $("#btnProcessAirtimeTopup").click(function(){

              RACode = $("#txtRACode").val();
              MNO = $("#lstMobileNetwork").val();
              MobileNumber = $("#txtRAMobileNumber").val();
              AirtimeAmount = $("#txtAirtimeTopupAmount").val();
              RechargeDate = $("#txtRechargeDate").val();
              RechargeCards = $("#txtRechargeCards").val();

              if (MNO=="" || MNO ==0){
                 DisplayWarningMessage('Select the SIM Network recharged');
                 $("#lstMobileNetwork").focus();
              }else if(AirtimeAmount=="" || AirtimeAmount<=0){
                  DisplayWarningMessage('Enter value of airtime recharge');
                 $("#txtAirtimeTopupAmount").focus();
              }else if (RechargeDate==""){
                 DisplayWarningMessage('Select the date RA was credited with airtime');
                 $("#txtRechargeDate").focus();
              }else{

                  // WE have all the info we need lets post the transaction.
                   $("#btnProcessAirtimeTopup").prop('disabled',true);
                   $.ajax({
                      type: "POST",
                      url: "http://localhost/portal/round9/workflow/ProcessRAAirtimeRecharge",
                      cache: false, 
                      data: {'RACode':RACode,'MNO':MNO,'MobileNumber':MobileNumber,'AirtimeAmount':AirtimeAmount,'RechargeDate':RechargeDate,'RechargeCards':RechargeCards},
                      dataType: "json",
                      success: function(data) {
                          $("#btnProcessAirtimeTopup").prop('disabled',false);
                          if (data.TransactionCompleted==true){
                               DisplaySuccessMessage(RACode+' has been topped up with USD '+AirtimeAmount);
                               $("#lstMobileNetwork").val(0);
                               $("#txtRAMobileNumber").val('');
                               $("#txtAirtimeTopupAmount").val('');
                               $("#txtRechargeDate").val('');
                               $("#txtRechargeCards").val('');
                          }else{
                              DisplayWarningMessage('Failed to topup RA account');
                          }
                          

                      },

                      error: function(xhr, status, error){   
                              $("#btnProcessAirtimeTopup").prop('disabled',false);
                             DisplayErrorMessage('Error processing topup : '+error);       
                      }
                  }); //END OF AJAX

              }

      });

          $("#lstDuplicatedIVQ").change(function(){

             InstanceID = $("#lstDuplicatedIVQ").val();
             $("#txtIVQInstanceID").val(InstanceID);
             $("#btnDeleteIVQ").html('<i class="fa fa-spinner fa-spin"></i>&nbspLoading.....');
             $("#btnProcessIVQChange").html('<i class="fa fa-spinner fa-spin"></i>&nbspLoading.....');
             $.ajax({
              type: "POST",
              url: "http://localhost/portal/round9/workflow/GetIVQDetails",
              cache: false, 
              data: {'InstanceID':InstanceID},
              dataType: "json",
              success: function(data) {

                 $("#tblIVQInfo").html(''); 
                 $("#tblIVQInfo").html(data.IVQInformationTable);
                 $("#btnDeleteIVQ").html('Delete IVQ');
                 $("#btnProcessIVQChange").html('Modify Identifiers');
                  
                  

              },

              error: function(xhr, status, error){   
                     
                     DisplayErrorMessage('Error loading IVQ information : '+error);
                     $("#btnDeleteIVQ").html('Delete IVQ');
                     $("#btnProcessIVQChange").html('Modify Identifiers');       
              }
          }); //END OF AJAX



          });

      $("#btnProcessAirtimePurchase").click(function(){

              MNO = $("#lstMobileNetworkPurchased").val();
              AirtimePurchaseAmount = $("#txtAirtimePurchaseAmount").val();
              AirtimePurchaseDate = $("#txtAirtimePurchaseDate").val();
              AirtimePurchaseRef = $("#txtAirtimePurchaseRef").val();

              if (MNO=="" || MNO ==0){
                 DisplayWarningMessage('Select Network Service Provider');
                 $("#lstMobileNetworkPurchased").focus();
              }else if(AirtimePurchaseAmount=="" || AirtimePurchaseAmount<=0){
                  DisplayWarningMessage('Enter value of airtime that was purchased');
                 $("#txtAirtimePurchaseAmount").focus();
              }else if (AirtimePurchaseDate==""){
                 DisplayWarningMessage('Select the date airtime was purchased');
                 $("#txtAirtimePurchaseDate").focus();
              }else{

                  // WE have all the info we need lets post the transaction.
                   $("#btnProcessAirtimePurchase").prop('disabled',true);
                   $("#btnProcessAirtimePurchase").html('<i class="fa fa-spinner fa-spin"></i>&nbspProcessing');
                   $.ajax({
                      type: "POST",
                      url: "http://localhost/portal/round9/workflow/ProcessAirtimePurchaseRecord",
                      cache: false, 
                      data: {'MNO':MNO,'AirtimePurchaseAmount':AirtimePurchaseAmount,'AirtimePurchaseDate':AirtimePurchaseDate,'AirtimePurchaseRef':AirtimePurchaseRef},
                      dataType: "json",
                      success: function(data) {
                          $("#btnProcessAirtimePurchase").prop('disabled',false);
                          $("#btnProcessAirtimePurchase").html('Add Purchase');
                          if (data.TransactionCompleted==true){
                               DisplaySuccessMessage('Airtime Purchase record has been captured ');
                               $("#lstMobileNetworkPurchased").val(0);
                               $("#txtAirtimePurchaseAmount").val('');
                               $("#txtAirtimePurchaseDate").val('');
                               $("#txtAirtimePurchaseRef").val('');
                               $("#txtRechargeCards").val('');
                                $("#btnProcessAirtimeTopup").prop('disabled',false);
                          }else{
                              DisplayErrorMessage('Failed to capture Airtime Purchase Record');
                               $("#btnProcessAirtimeTopup").prop('disabled',false);
                          }
                          

                      },

                      error: function(xhr, status, error){   
                             $("#btnProcessAirtimeTopup").prop('disabled',false);
                             $("#btnProcessAirtimePurchase").html('Add Purchase');
                             DisplayErrorMessage('Error processing airtime purchase record : '+error);       
                      }
                  }); //END OF AJAX

              }

      });

      $("#btnConfirmGeneratesoap").click(function(){

        var StudySite = $("#lstStudySite").val();
        var InterviewType = $("#lstInterviewType").val();

        if (StudySite==0){
            DisplayErrorMessage('Select Study Site to Generate Soap');
            $("#lstStudySite").focus();

        }else if(InterviewType==0){
            DisplayErrorMessage('Select type of Interview to generate soap checklist');
            $("#lstInterviewType").focus();
        }else{
           $("#MessageNotification").html('');
           $("#btnConfirmGeneratesoap").prop('disabled',true);
           $("#btnConfirmGeneratesoap").html('<i class="fa fa-spinner fa-spin"></i>&nbspProcessing');
           $.ajax({
              type: "POST",
              url: "http://localhost/portal/round9/workflow/GenerateSoapIncentiveChecklist",
              cache: false, 
              data: {'StudySite':StudySite,'InterviewType':InterviewType},
              dataType: "json",
              success: function(data) {
                  $("#btnConfirmGeneratesoap").prop('disabled',false);
                  $("#btnConfirmGeneratesoap").html('Generate Soap Checklist');
                  if (data.TransactionCompleted==true){
                       DisplaySuccessMessage('Site checklist has been generated successfully');
                       $("#lstStudySite").val(0); 
                       $("#lstInterviewType").val(0);                        
                       $("#btnConfirmGeneratesoap").prop('disabled',false);
                  }else{
                      DisplayErrorMessage('Checklist not generated.'+data.ErrorMessage);
                       $("#btnConfirmGeneratesoap").prop('disabled',false);
                  }
                  

              },

              error: function(xhr, status, error){   
                     $("#btnConfirmGeneratesoap").prop('disabled',false);
                     $("#btnConfirmGeneratesoap").html('Generate Soap Checklist');
                     DisplayErrorMessage('Error generating Soap Checklist : '+error);       
              }
          }); //END OF AJAX

        }

      });

      $("#btnPostBatchSoapCollection").click(function(){

          if ($('input[type=checkbox]:checked').length > 0) {
            $("#btnPostBatchSoapCollection").html('<i class="fa fa-spinner fa-spin"></i>&nbspPosting');
            $("#btnPostBatchSoapCollection").prop('disabled',true);
            $.ajax({
              type: "POST",
              url: "http://localhost/portal/round9/workflow/BulkSoapCollectionConfirm",
              cache: false, 
              data: $("#frmPostSoapCollected").serialize(),
              dataType: "json",
              success: function(data) {

                $("#btnPostBatchSoapCollection").html('Confirm Collection');
                $("#btnPostBatchSoapCollection").prop('disabled',false);

                if (data.TransactionCompleted){
                    DisplaySuccessMessage('The selected participants flagged as received soap');
                }else{
                    DisplayErrorMessage(data.ErrorMessage);

                }
                 
                               

              },

              error: function(xhr, status, error){   
                    $("#btnPostBatchSoapCollection").html('Confirm Collection');
                    $("#btnPostBatchSoapCollection").prop('disabled',false);
                     $("#btnProcessHHKEYChanges").html('Modify Identifier');
                     DisplayErrorMessage('Error encountered updating soap collection status: ERR '+error);       
              }
                
                
          }); //END OF AJAX

          } else {
              DisplayWarningMessage('Please select at least one participant to post batch soap receive');
          }





      });

      $("#lstDuplicatedHouseholds").change(function(){

        var Metakey = $("#lstDuplicatedHouseholds").val();
        $("#txtStudySite").val('');
        $("#txtLastHHID").val('');


        if (Metakey==0){
            //Do nothing
        }else{
            $("#txtInstanceID").val(Metakey);
            $("#btnProcessHHKEYChanges").html('<i class="fa fa-spinner fa-spin"></i>&nbspPlease Wait..');
            $.ajax({
              type: "POST",
              url: "http://localhost/portal/round9/workflow/GetMembersInHousehold",
              cache: false, 
              data: {'Metakey':Metakey},
              dataType: "json",
              success: function(data) {
                 
                 $("#btnProcessHHKEYChanges").html('Modify Identifier');
                 $("#table").html(''); 
                 $("#table").html(data.MembersTable);
                 $("#txtNewHHID").prop('disabled',false);
                 $("#txtConfirmNewHHID").prop('disabled',false);
                 $("#btnProcessHHKEYChanges").prop('disabled',false); 
                 $("#txtStudySite").val(data.SiteDetails.site+' - '+data.SiteDetails.SiteName);
                 $("#txtLastHHID").val(data.SiteDetails.LastHHID);                

              },

              error: function(xhr, status, error){   
                     $("#btnProcessHHKEYChanges").html('Modify Identifier');
                     DisplayErrorMessage('Error encountered retrieving list of household members : '+error);       
              }
          }); //END OF AJAX
        }

      });

      $("#btnProcessHHKEYChanges").click(function(){


        NewHHID = $("#txtNewHHID").val();
        ConfirmHHID = $("#txtConfirmNewHHID").val();
        Metakey = $("#lstDuplicatedHouseholds").val();


        if($("#chkConfirmIDChanges").prop("checked") == true){
            Checked = 1;
        }else{
            Checked = 0;
        }

        if($("#chkTargetR8").prop("checked") == true){
            TargetR8Tables = 1;
        }else{
            TargetR8Tables = 0;
        }

        if($("#chkTargetR7").prop("checked") == true){
            TargetR7Tables = 1;
        }else{
            TargetR7Tables = 0;
        }

        if (Checked==0){
            DisplayWarningMessage('Confirm the changes by selecting the checkbox');
            $("#chkConfirmIDChanges").focus();
        }else if(NewHHID==""){
              DisplayWarningMessage('Please assign an HHID');
              $("#txtNewHHID").focus();
        }else if(ConfirmHHID==""){
              DisplayWarningMessage('Please confirm the HHID');
              $("#txtConfirmNewHHID").focus();
        }else if(NewHHID.localeCompare(ConfirmHHID) !=0){
              DisplayWarningMessage('The entered HHIDs do not match');
        }else{
            $("#MessageNotification").html('');
            $("#btnProcessHHKEYChanges").prop('disabled',true);
            $("#btnProcessHHKEYChanges").html('<i class="fa fa-spinner fa-spin"></i>&nbspPlease Wait..');

            //Lets send new HHID and METAKEY to server
            $.ajax({
              type: "POST",
              url: "http://localhost/portal/round9/workflow/UpdateHouseholdQuestionnaireIDS",
              cache: false, 
              data: {'Metakey':Metakey,'NewHHID':NewHHID,'TargetR8Tables':TargetR8Tables,'TargetR7Tables':TargetR7Tables},
              dataType: "json",
              success: function(data) {

                   $("#txtNewHHID").val('');
                   $("#txtConfirmNewHHID").val('');
                   $("#txtNewHHID").prop('disabled',false);
                   $("#txtConfirmNewHHID").prop('disabled',false);
                   $("#btnProcessHHKEYChanges").html('Modify Identifier');
                 
                 if (data.TransactionCompleted){

                    DisplaySuccessMessage('Household successfully updated together with members who had proceeded to IVQ');
                    $("#table").html(''); 
                    $("#table").html(data.MembersTable);
                    $("#R8Information").html(data.R8MembersTable);

                 }else{
                    DisplayErrorMessage('Error updating HHID. '+data.ErrorMessage);
                 }               

              },

              error: function(xhr, status, error){   
                     $("#btnProcessHHKEYChanges").html('Modify Identifier');
                     DisplayErrorMessage('Error updating the Household Data : '+error);
                     $("#txtNewHHID").val('');
                     $("#txtConfirmNewHHID").val('');
                     $("#txtNewHHID").prop('disabled',false);
                     $("#txtConfirmNewHHID").prop('disabled',false);;       
              }
            }); //END OF AJAX



        }

      });

      $('#chkConfirmIVQDelete').click(function() { 
        if ($(this).is(':checked')) { 
           $("#btnDeleteIVQ").show();
           $("#divCorrections").hide();
           $("#btnProcessIVQChange").hide();
        }else  if (!$(this).is(':checked')){
           $("#btnDeleteIVQ").hide();
           $("#btnProcessIVQChange").show();
           $("#divCorrections").show();
        }
      });

      $("#btnConfirmSoapReceived").click(function(){

         InterviewType = $("#InterviewType").val();
         InstanceID = $("#instanceID").val();
         InterviewDate = $("#txtSoapCollectionDate").val();
         

         if (InterviewDate==''){
               DisplayErrorMessage('Please select soap collection date');
         }else{
                $("#btnConfirmSoapReceived").prop('disabled',true);
                $("#btnConfirmSoapReceived").html('<i class="fa fa-spinner fa-spin"></i>&nbspProcessing');
                $("#MessageNotification").html('');
                $.ajax({
                  type: "POST",
                  url: "http://localhost/portal/round9/workflow/ConfirmSoapCollected",
                  cache: false, 
                  data: {'InterviewType':InterviewType,'InstanceID':InstanceID,'InterviewDate':InterviewDate},
                  dataType: "json",
                  success: function(data) {

                                         
                     
                    if (data.TransactionCompleted){
                        DisplaySuccessMessage('Soap collection success');
                        $("#InterviewType").val('')
                        $("#instanceID").val('');
                        $("#txtSoapCollectionDate").val('');
                        $("#btnConfirmSoapReceived").html('Confirm Soap Received');
                        $("#btnConfirmSoapReceived").prop('disabled',false);
                     }else{
                        $("#btnConfirmSoapReceived").html('Confirm Soap Received');
                        $("#btnConfirmSoapReceived").prop('disabled',false);
                        DisplayErrorMessage('Error marking soap collection. Error - '+data.ErrorMessage);
                     }               

                  },

                  error: function(xhr, status, error){   
                         
                         DisplayErrorMessage('Error updating the Household Data : '+error);
                         $("#btnConfirmSoapReceived").html('Confirm Soap Received');
                         $("#btnConfirmSoapReceived").prop('disabled',false);
          
                  }
                }); //END OF AJAX

         }

      });


        

});

function ProcessTopup(RACode,RAName){

   $("#modalToupAirtime").modal({
       backdrop: 'static',
       keyboard: false
   });

   $("#txtRAName").val(RAName);
   $("#txtRACode").val(RACode);
   $("#lstMobileNetwork").val(0);
   $("#txtRAMobileNumber").val('');
   $("#txtAirtimeTopupAmount").val('');
   $("#txtRechargeDate").val('');
   $("#txtRechargeCards").val('');

   //$("#btnProcessAirtimeTopup").prop('disabled',true);

}

function DisplayErrorMessage(MessageAlert){

   Msg ='<div class="alert alert-danger"><button type="button" class="close" data-dismiss="alert" aria-label="Close"> <span aria-hidden="true">&times;</span> </button>'
                                      +'<h6> Error Notification </h6>'+MessageAlert+'</div>';

     $("#MessageNotification").html(Msg);

}

function DisplayWarningMessage(MessageAlert){

   Msg ='<div class="alert alert-warning"><button type="button" class="close" data-dismiss="alert" aria-label="Close"> <span aria-hidden="true">&times;</span> </button>'
                                      +'<h6> Warning Notification</h6>'+MessageAlert+'</div>';

     $("#MessageNotification").html(Msg);

}

function DisplaySuccessMessage(MessageAlert){

   Msg ='<div class="alert alert-success"><button type="button" class="close" data-dismiss="alert" aria-label="Close"> <span aria-hidden="true">&times;</span> </button>'
                                      +'<h6> Success Notification </h6>'+MessageAlert+'</div>';

   $("#MessageNotification").html(Msg);

}

function ViewVillageDetails(VillageName){
    $("#modalVillageStatistics").modal({
       backdrop: 'static',
       keyboard: false
   });
}

function FlagIncentiveReceived(IncentiveType,Identifier){
    $("#modalFlagIncentiveReceived").modal({
       backdrop: 'static',
       keyboard: false
   });
   $("#txtParticipantIdentifier").val('Identifier');
   $("#txtInterviewType").val('IncentiveType');
}

function CorrectHHQIdentifiers(hhkey){
    $("#modalCorrectHHID").modal({
       backdrop: 'static',
       keyboard: false
   });

   Table = '<table class="BasicDatatableNoOptions table table-striped table-bordered border-top-0 border-bottom-0" style="width:100%"><thead><tr><th>Membername</th><th>KEY</th><th>Age</th><th>IVQ</th></tr></thead><tbody id="HouseholdInformation"></tbody></table>';
   $("#table").html(Table);
   $("#MessageNotification").html('');
   $("#txtNewHHID").val('');
   $("#txtConfirmNewHHID").val('');
   $("#txtNewHHID").prop('disabled',true);
   $("#txtConfirmNewHHID").prop('disabled',true);
   // Lets get the list of households on that hhkey
   $("#btnProcessHHKEYChanges").html('<i class="fa fa-spinner fa-spin"></i>&nbspPlease Wait..');
   $("#txtInstanceID").val('');
   $.ajax({
          type: "POST",
          url: "http://localhost/portal/round9/workflow/GetHHQInstancesOnHHKEY",
          cache: false, 
          data: {'hhkey':hhkey},
          dataType: "json",
          success: function(data) {
              $("#btnProcessHHKEYChanges").html('Modify Identifier');  

              if (data.TransactionCompleted==true){

                  var Households = data.HouseholdsObject;
                  NumberOfHouseholds = Households.length;
                
                   $("#lstDuplicatedHouseholds").html('');
                   $("#txtStudySite").val('');
                   $("#txtLastHHID").val('');
                   $("#lstDuplicatedHouseholds").append('<option selected disabled value="0">Select Household</option>');
                      
                   for (index = 0; index < NumberOfHouseholds; index++) {
                      var HouseholdsOptions = '<option value="' + Households[index].instanceID + '">' + Households[index].hhkey + ' - '+ Households[index].hhname +'</option>'; 
                      $("#lstDuplicatedHouseholds").append(HouseholdsOptions);                 
                   }
                   
              }else{
                  
              }
              

          },

          error: function(xhr, status, error){   
                 $("#btnProcessHHKEYChanges").html('Modify Identifier');     
                 DisplayErrorMessage('Error retriving records. Try again '+error);       
          }
      }); //END OF AJAX

}

function CorrectIVQIdentifiers(hhmem_key){
    $("#modalCorrectIVQIdenitifiers").modal({
       backdrop: 'static',
       keyboard: false
   });

   $("#btnProcessIVQChange").html('<i class="fa fa-spinner fa-spin"></i>&nbspPlease Wait..');
   $("#txtIVQInstanceID").val('');

   $.ajax({
          type: "POST",
          url: "http://localhost/portal/round9/workflow/GetDuplicatedIVQInstances",
          cache: false, 
          data: {'hhmem_key':hhmem_key},
          dataType: "json",
          success: function(data) {
              $("#btnProcessIVQChange").html('Modify Identifiers');  

              if (data.TransactionCompleted==true){

                  var IVQs = data.IVQObject;
                  NumberOfIVQs = IVQs.length;
                
                   $("#lstDuplicatedIVQ").html('');
                   $("#lstDuplicatedIVQ").append('<option selected disabled value="0">Select IVQ Participant</option>');
                      
                   for (index = 0; index < NumberOfIVQs; index++) {
                      var IVQOptions = '<option value="' + IVQs[index].instanceID + '">' + IVQs[index].hhmem_key + ' - '+ IVQs[index].indvname +'</option>'; 
                      $("#lstDuplicatedIVQ").append(IVQOptions);                 
                   }
                   
              }else{
                  
              }
              

          },

          error: function(xhr, status, error){   
                 $("#btnProcessIVQChange").html('Modify Identifiers');     
                 DisplayErrorMessage('Error retriving records. Try again '+error);       
          }
      }); //END OF AJAX
}


function MarkHHQSoapReceived(instanceID,ParticipantName,InterviewType){
    
    $("#MessageNotification").html('');
    $("#modalMarkSoapReceived").modal();
    $("#instanceID").val(instanceID);
    $("#ParticipantName").val(ParticipantName);
    $("#InterviewType").val(InterviewType);

}

function MarkIVQSoapReceived(instanceID,ParticipantName,InterviewType){
    $("#MessageNotification").html('');
    $("#modalMarkSoapReceived").modal();
    $("#instanceID").val(instanceID);
    $("#ParticipantName").val(ParticipantName);
    $("#InterviewType").val(InterviewType);

}
