from django.http import HttpResponseRedirect, HttpResponse
from django.shortcuts import render
from django.template import loader
from webui.dataforj import get_flow
from webui.forms.dag import AddSourceForm
from dataforj import api
from webui.dataforj import set_flow
import json  

def view(request):
    template = loader.get_template('dag/view.html')
    flow_types = {step.name:step.dagre_type()
                  for step in get_flow()._steps.values()}
    flow_descs ={step.name:step.description
                  for step in get_flow()._steps.values()}     
    context = {
        'name': get_flow().name,
        'description': get_flow().description,
        'type_dict_dson': json.dumps(flow_types),
        'desc_dict_dson': json.dumps(flow_descs),
        'dagre_nodes_edges_js': get_flow().to_dagre_nodes_edges()
    }
    return HttpResponse(template.render(context, request))


def add_source(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddSourceForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            updated_flow = api.add_source_step(
                flow=get_flow(),
                name=form.cleaned_data['step_name'],
                description=form.cleaned_data['step_description'],
                uri=form.cleaned_data['uri'],
                format_type=form.cleaned_data['format_type'],
                options={})
            set_flow(updated_flow)
            return HttpResponseRedirect('/dag/view')

    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddSourceForm()

    return render(request, 'dag/add_source.html', {'form': form})
