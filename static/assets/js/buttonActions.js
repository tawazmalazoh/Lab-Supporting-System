
$(document).ready(function() {
    // Add event listener to delete buttons
    $('.delete-btn').on('click', function() {
        var uniqueKey = $(this).data('unique-key');
        console.log(uniqueKey);
        
        if (confirm('Are you sure you want to delete this entry?')) {
            $.ajax({
                url: '/delete_Dashboard_Entry/',
                method: 'POST',
                data: {
                    'unique_key': uniqueKey,
                    'csrfmiddlewaretoken': $('input[name=csrfmiddlewaretoken]').val()
                },
              

                success: function(response) {
                    if (response.status === "success") {
                        // Display the success message from the server in the #msg div
                        $('#msg').html('<div class="alert alert-success alert-dismissible fade show" role="alert">' +
                                       '<i class="bi bi-check-circle-fill me-1"></i>' + response.message +
                                       '<button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>' +
                                       '</div>');

                        // Delay the page reload by 3 seconds (3000 milliseconds)
                        setTimeout(function() {
                            location.reload();
                        }, 3000);

                    } else if (response.status === "error") {
                        // Display the error message from the server in the #msg div
                        $('#msg').html('<div class="alert alert-danger alert-dismissible fade show" role="alert">' +
                                       '<i class="bi bi-exclamation-triangle-fill me-1"></i>' + response.message +
                                       '<button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>' +
                                       '</div>');
                    }
                },
                error: function(error) {
                    // Display a generic error message in the #msg div
                    $('#msg').html('<div class="alert alert-danger alert-dismissible fade show" role="alert">' +
                                   '<i class="bi bi-exclamation-triangle-fill me-1"></i>There was a network or server error. Please try again.' +
                                   '<button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>' +
                                   '</div>');
                    console.error("AJAX error: ", error);
                }




            });
        }
    }); //endcall






});



