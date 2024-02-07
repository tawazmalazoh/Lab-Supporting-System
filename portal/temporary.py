@login_required
def BASE(request):
   
    user = request.user  # Get the authenticated user
    form = IstForm(request.POST or None)
    if form.is_valid():
        start_date = form.cleaned_data['START_DATE']
        end_date = form.cleaned_data['END_DATE']
        form.save()
    
        startdate = '2020-01-01'
        enddate = datetime.datetime.now().strftime('%Y-%m-%d')
        selected_provinces = ['Mashonaland West','Masvingo','Matebeleland North','Metebeleland South','Mashonaland Central','Midlands,Harare','Manicaland','Bulawayo','Mashonaland East']
        selected_provinces = tuple(selected_provinces)
        pepfar_support = ('DSD', 'TA-SDI')
        
        SQLqueries = allqueries(startdate, enddate, selected_provinces, pepfar_support)
        data = []
        rejected = []  # Initialize rejected as an empty list
        for query in SQLqueries:
            try:
                with connections['default'].cursor() as cursor:
                    cursor.execute(query['sql'], query['parameters'])
                    columns = [col[0] for col in cursor.description]
                    query_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    data.append({'query_name': query['query_name'], 'query_data': query_data})

                    # Check if the query_name is 'CLIanalysis'
                    if query['query_name'] == 'CLIanalysis':
                        # Loop through the query_data
                        for row in query_data:
                            # Append the value of RESOLVED_Specimens_rejected to the rejected list
                            rejected.append(row['RESOLVED_Specimens_rejected'])
            except Exception as e:
                print(f"An error occurred for {query['query_name']}: {e}")

        # Prepare data for the sankey chart
 
        sankey_data = [
                        { "name": 'Resolved ' },
                        { "name": 'UnResolved' },
                        { "name": 'Pending Investigation' },
                        { "name": 'LIMS Failed' },
                        { "name": 'Rejected' },
                        { "name": 'Waiting' },
                        { "name": 'Pending Testing' },
                        { "name": 'Not Received' },
                        { "name": 'Not Documented' },
                        { "name": 'Pending Published' },
                        { "name": 'Not Dispatched' },
                        { "name": 'Wrong Publishing' }
                    ]
       
        sankey_links= [
        { "source": 'Resolved', "target": 'LIMS Failed', "value": RESOLVED_Specimens_rejected[0] },
        { "source": 'UnResolved',"target": 'Pending Investigation', "value": UNRESOLVED_Investigation_in_progress[0] },
        { "source": 'Resolved', "target": 'Not Received', "value": Specimens_not_received[0] },
        { "source": 'Resolved', "target": 'Not Documented', "value": not_documented[0] },
        { "source": 'UnResolved',"target": 'Pending Testing', "value": UNRESOLVED_Pending_testing[0] },
        { "source": 'Resolved', "target": 'Pending Published', "value": pending_publishing[0] },
        { "source": 'Resolved', "target": 'Not Dispatched', "value": not_yet_dispatched_printed[0] },
        { "source": 'Resolved', "target": 'Wrong Publishing', "value": Results_wrong_facility[0] },
        { "source": 'UnResolved',"target": 'Waiting', "value": UNRESOLVED_Referred_awaiting_results[0] }
      ],

        # Calculate the sum of the values of the incoming and outgoing links for each node
        node_values = {}
        for link in sankey_links:
            node_values[link["source"]] = node_values.get(link["source"], 0) + link["value"]
            node_values[link["target"]] = node_values.get(link["target"], 0) + link["value"]

        # Include the calculated values in the node's name
        for node in sankey_data:
            node["name"] += " (" + str(node_values[node["name"]]) + ")"

        context = {
            'form': form,
            'data': data,
            'user': user,
            'now': datetime.datetime.now,
            'sankey_data': sankey_data,
            'sankey_links': sankey_links,
        }
        return render(request, 'index.html', context)
    else: 
        # Rest of your code...
