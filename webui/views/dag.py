from django.http import HttpResponseRedirect, HttpResponse
from django.shortcuts import render
from django.template import loader
from webui.dataforj import get_flow, set_flow
from webui.forms.dag import AddSourceForm, AddSinkForm, SelectedStepForm, AddChildForm, AddUnionForm, AddSqlForm, AddPySparkForm
from dataforj import api
from webui.views import run
import json  
import os


def create_file(file_name: str, content: str):
    '''Util fucnction to create a file for SQL or PySpark steps'''
    #If the file does not exist, let's create it
    if not os.path.exists(file_name):
        #If the dir does not exist, let's create it
        if not os.path.exists(os.path.dirname(file_name)):
            os.makedirs(os.path.dirname(file_name))
        with open(file_name, 'w') as writer:
            writer.write(content)

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


def parse_options(options_text: str):
    print(f'options[{options_text}]')
    if options_text == None or options_text == '':
        return None
    else:    
        options = {key_value.split("=")[0]:key_value.split("=")[1].rstrip()
                for key_value in options_text.split('\n')}
        return options  


def add_source(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddSourceForm(request.POST)
        # check whether it's valid:
        if form.is_valid():
            options_dict = parse_options(form.cleaned_data['options_text'])
            updated_flow = api.add_source_step(
                flow=get_flow(),
                name=form.cleaned_data['step_name'],
                description=form.cleaned_data['step_description'],
                uri=form.cleaned_data['uri'],
                format_type=form.cleaned_data['format_type'],
                options=options_dict)
            set_flow(updated_flow)
            return HttpResponseRedirect('/dag/view')

    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddSourceForm()

    return render(request, 'dag/add_source.html', {'form': form})


def run_delete_add_step(request):
    if 'run' in request.POST:
        return run_step(request)
    elif 'delete' in request.POST:
        return delete_step(request)
    elif 'add' in request.POST:
        return add_step(request)
    else:
        return HttpResponseRedirect('/dag/view')


def run_step(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = SelectedStepForm(request.POST)
        if form.is_valid():
            step_name = form.cleaned_data['step_name']
            return run.run_step(step_name, request)
    else:
        return HttpResponseRedirect('/dag/view')


def delete_step(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = SelectedStepForm(request.POST)
        if form.is_valid():
            step_name = form.cleaned_data['step_name']
            updated_flow = api.remove_step(flow=get_flow(), name=step_name)
            set_flow(updated_flow)
            return HttpResponseRedirect('/dag/view')
    else:
        return HttpResponseRedirect('/dag/view')


def add_step(request):
    form = AddChildForm()
    return render(request, 'dag/add_step.html', {'form': form})


def add_step_submit(request):
    print('add_step')
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddChildForm(request.POST)
        print(form.errors)
        # check whether it's valid:
        if form.is_valid():
            step_type = form.cleaned_data['step_type']
            if step_type == 'union':
                return HttpResponseRedirect('add_union')
            elif step_type == 'sql':
                return HttpResponseRedirect('add_sql')
            elif step_type == 'pyspark':
                return HttpResponseRedirect('add_pyspark')
            elif step_type == 'sink':
                return HttpResponseRedirect('add_sink')
            else:
                return HttpResponseRedirect('/dag/view')
    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddChildForm()

    return render(request, 'dag/add_step.html', {'form': form})


def add_sink(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddSinkForm(request.POST, flow=get_flow())
        if form.is_valid():
            step_name = form.cleaned_data['step_name']
            step_description = form.cleaned_data['step_description']
            depends_ons = form.cleaned_data['depends_ons']
            uri = form.cleaned_data['uri']
            format_type = form.cleaned_data['format_type']
            mode = form.cleaned_data['mode']
            options = parse_options(form.cleaned_data['options_text'])
            updated_flow = api.add_sink_step(flow=get_flow(), name=step_name, depends_on=[depends_ons], 
                                             description=step_description, uri=uri, format_type=format_type,
                                             mode=mode, options=options)                               
            set_flow(updated_flow)
            return HttpResponseRedirect('/dag/view')
    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddSinkForm(flow=get_flow())

    return render(request, 'dag/add_sink.html', {'form': form})



def add_union(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddUnionForm(request.POST, flow=get_flow())
        if form.is_valid():
            step_name = form.cleaned_data['step_name']
            step_description = form.cleaned_data['step_description']
            depends_ons = form.cleaned_data['depends_ons']
            updated_flow = api.add_union_step(flow=get_flow(), name=step_name, depends_on=depends_ons, 
                                             description=step_description)                                
            set_flow(updated_flow)
            return HttpResponseRedirect('/dag/view')
    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddUnionForm(flow=get_flow())

    return render(request, 'dag/add_union.html', {'form': form})


def add_sql(request):

    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddSqlForm(request.POST, flow=get_flow())
        if form.is_valid():
            step_name = form.cleaned_data['step_name']
            step_description = form.cleaned_data['step_description']
            depends_ons = form.cleaned_data['depends_ons']
            sql_table_names = ', '.join([f'{table} {table}' for table in depends_ons])
            sql_select = ', '.join([f'{table}.*' for table in depends_ons])
            sql_query = f'select {sql_select} from {sql_table_names}'
            sql_file_path = form.cleaned_data['sql_file_path']

            create_file(sql_file_path, sql_query)

            updated_flow = api.add_sql_step(flow=get_flow(), name=step_name, depends_on=depends_ons, 
                                             description=step_description, sql_file_path=sql_file_path)                                
            set_flow(updated_flow)
            return HttpResponseRedirect('/dag/view')
    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddSqlForm(flow=get_flow())

    return render(request, 'dag/add_sql.html', {'form': form})


def add_pyspark(request):

    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = AddPySparkForm(request.POST, flow=get_flow())
        if form.is_valid():
            step_name = form.cleaned_data['step_name']
            step_description = form.cleaned_data['step_description']
            depends_ons = form.cleaned_data['depends_ons']
            pyspark_file_path = form.cleaned_data['pyspark_file_path']

            create_file(pyspark_file_path, f'return {depends_ons[0]}_df')

            updated_flow = api.add_pyspark_step(flow=get_flow(), name=step_name, depends_on=depends_ons, 
                                                description=step_description, pyspark_file_path=pyspark_file_path)                                
            set_flow(updated_flow)
            return HttpResponseRedirect('/dag/view')
    # if a GET (or any other method) we'll create a blank form
    else:
        form = AddPySparkForm(flow=get_flow())

    return render(request, 'dag/add_pyspark.html', {'form': form})