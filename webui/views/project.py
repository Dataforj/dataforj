from django.http import HttpResponseRedirect, HttpResponse
from django.shortcuts import render
from django.template import loader
from webui.forms.project import NewProjectForm, ProjectOpenForm
from dataforj import cli
from webui.dataforj import set_flow


def new(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = NewProjectForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            flow = cli.init(
                form.cleaned_data['project_location'],
                form.cleaned_data['project_name'])
            set_flow(flow)
            return HttpResponseRedirect('/dag/view')

    # if a GET (or any other method) we'll create a blank form
    else:
        form = NewProjectForm()

    return render(request, 'project/new.html', {'form': form})


def open(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = ProjectOpenForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            flow = cli.open_flow(
                form.cleaned_data['project_location'])
            set_flow(flow)
            return HttpResponseRedirect('/dag/view')

    # if a GET (or any other method) we'll create a blank form
    else:
        form = ProjectOpenForm()

    return render(request, 'project/open.html', {'form': form})
