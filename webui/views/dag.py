from django.http import HttpResponse
from django.shortcuts import render
from django.template import loader
from webui.dataforj import get_flow
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
