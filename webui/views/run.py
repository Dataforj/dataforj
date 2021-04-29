from django.http import HttpResponseRedirect, HttpResponse
from django.shortcuts import render
from django.template import loader
from webui.forms.run import RunSqlForm, RunPySparkForm
from dataforj import api
from webui.dataforj import set_flow, get_flow, set_df
from dataforj.envs import DataforjEnv
import json  
from django.template.defaultfilters import register


@register.filter(name='dict_key')
def dict_key(d, k):
    '''Returns the given key from a dictionary.'''
    return d.get(k, '')


def run_step(step_name: str, request):
    step_type = get_flow()._steps[step_name].dagre_type()
    print(f'step_type = [{step_type}]')
    if step_type == 'SQL':
        return run_sql(step_name, request)
    if step_type == 'PySpark':
        return run_pyspark(step_name, request)
    else:  
        return run_basic_step(step_name, request) 

def run_basic_step(step_name: str, request):
    env = DataforjEnv(get_flow().name, 'local')
    error_message = None
    header_columns = None
    rows = None
    try:
        spark_df = get_flow().dataframe_for_step(env, step_name)
        set_df(spark_df)
        header_columns = spark_df.schema.names
        rows = spark_df.rdd.map(lambda row: row.asDict(True)).take(10)
    except Exception as e:
        error_message = str(e)
            
    template = loader.get_template('run/run_step.html')    
    context = {
        'step_name': step_name,
        'error_message': error_message,
        'header_columns': header_columns,
        'rows': rows
    }
    return HttpResponse(template.render(context, request))


def run_sql(step_name: str, request):
    env = DataforjEnv(get_flow().name, 'local')
    sql_text = get_flow()._steps[step_name].get_sql_code()
    error_message = None
    header_columns = None
    rows = None
    try:
        spark_df = get_flow().dataframe_for_step(env, step_name)
        set_df(spark_df)
        header_columns = spark_df.schema.names
        rows = spark_df.rdd.map(lambda row: row.asDict(True)).take(10)
    except Exception as e:
        error_message = str(e)
            
    template = loader.get_template('run/run_sql.html')    
    context = {
        'step_name': step_name,
        'sql_text': sql_text,
        'error_message': error_message,
        'header_columns': header_columns,
        'rows': rows
    }
    return HttpResponse(template.render(context, request))


def run_sql_submit(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = RunSqlForm(request.POST)
        if form.is_valid():
            env = DataforjEnv(get_flow().name, 'local')
            step_name = form.cleaned_data['step_name']
            sql_text = form.cleaned_data['sql']
            get_flow()._steps[step_name].sql_from_editor = sql_text
            error_message = None
            header_columns = None
            rows = None
            try:
                spark_df = get_flow().dataframe_for_step(env, step_name)
                set_df(spark_df)
                header_columns = spark_df.schema.names
                rows = spark_df.rdd.map(lambda row: row.asDict(True)).take(10)
            except Exception as e:
                error_message = str(e)
            
            template = loader.get_template('run/run_sql.html')    
            context = {
                'step_name': step_name,
                'sql_text': sql_text,
                'error_message': error_message,
                'header_columns': header_columns,
                'rows': rows
            }
            return HttpResponse(template.render(context, request))

    # if a GET (or any other method) we'll create a blank form
    else:
        form = RunSqlForm()

    return render(request, 'run/run_sql.html', {'form': form})


def run_pyspark(step_name: str, request):
    env = DataforjEnv(get_flow().name, 'local')
    pyspark_code = get_flow()._steps[step_name].get_pyspark_code()
    function_def = get_flow()._steps[step_name].get_function_def()
    error_message = None
    header_columns = None
    rows = None
    try:
        spark_df = get_flow().dataframe_for_step(env, step_name)
        set_df(spark_df)
        header_columns = spark_df.schema.names
        rows = spark_df.rdd.map(lambda row: row.asDict(True)).take(10)
    except Exception as e:
        error_message = str(e)

    template = loader.get_template('run/run_pyspark.html')    
    context = {
        'step_name': step_name,
        'function_def': function_def,
        'pyspark_code': pyspark_code,
        'error_message': error_message,
        'header_columns': header_columns,
        'rows': rows
    }
    return HttpResponse(template.render(context, request))


def run_pyspark_submit(request):
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = RunPySparkForm(request.POST)
        if form.is_valid():
            env = DataforjEnv(get_flow().name, 'local')
            step_name = form.cleaned_data['step_name']
            pyspark_code = form.data['pyspark_code']
            function_def = get_flow()._steps[step_name].get_function_def()
            get_flow()._steps[step_name].pyspark_code_from_editor = pyspark_code
            error_message = None
            header_columns = None
            rows = None
            try:
                spark_df = get_flow().dataframe_for_step(env, step_name)
                set_df(spark_df)
                header_columns = spark_df.schema.names
                rows = spark_df.rdd.map(lambda row: row.asDict(True)).take(10)
            except Exception as e:
                error_message = str(e)
            
            template = loader.get_template('run/run_pyspark.html')    
            context = {
                'step_name': step_name,
                'function_def': function_def,
                'pyspark_code': pyspark_code,
                'error_message': error_message,
                'header_columns': header_columns,
                'rows': rows
            }
            return HttpResponse(template.render(context, request))

    # if a GET (or any other method) we'll create a blank form
    else:
        form = RunPySparkForm()

    return render(request, 'run/run_pyspark.html', {'form': form})
