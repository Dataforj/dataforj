from django.http import HttpResponse
from django.shortcuts import render
from django.template import loader
from webui.dataforj import get_flow

def view(request):
    template = loader.get_template('dag/view.html')
    context = {
        'name': get_flow().name
    }
    return HttpResponse(template.render(context, request))
