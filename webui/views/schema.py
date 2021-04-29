from django.http import HttpResponseRedirect, HttpResponse
from django.template import loader
import yaml
from webui.dataforj import get_flow, set_flow, set_schema, get_schema, get_df
from django.template.defaultfilters import register
from webui.forms.schema import EditSchemaForm

# DELETE ME !!!!

city_yaml_text = '''
- name: city
  tests: 
    - not_null
    - accepted_values: ['Amsterdam', 'Dublin', 'Frankfurt']
 '''  

flag_yaml_text = '''
- name: flag
  tests: 
    - not_null
 '''  

combined_yaml_text = f'{city_yaml_text}\n{flag_yaml_text}'


@register.filter(name='get_accepted_values')
def get_accepted_values(array):
    ''' This is horendous and should be removed.  The schemas were built to '''
    ''' be human readable but end up needing code like this to display them. '''
    ''' They need to be refactored '''
    accepted_values = [test['accepted_values'] for test in array if isinstance(test, dict)] 
    flat_list = [item for sublist in accepted_values for item in sublist]
    return ', '.join(flat_list)


def edit_schema(request): 
    print('edit_schema') 
    # schema = yaml.safe_load(combined_yaml_text)
    schema = [{'name': field.name, 'type': field.dataType, 'mandatory': field.nullable, 'accepted_values': None} 
              for field in get_df().schema]
    set_schema(schema)
    return show_schema(request)  


def submit_schema(request): 
    print('submit_schema') 
    # if this is a POST request we need to process the form data
    if request.method == 'POST':
        # create a form instance and populate it with data from the request:
        form = EditSchemaForm(request.POST)
        print(form.__dict__)
        if form.is_valid():pass

    # schema = yaml.safe_load(combined_yaml_text)
    schema = [{'name': field.name, 'type': field.dataType, 'mandatory': field.nullable, 'accepted_values': None} 
              for field in get_df().schema]
    set_schema(schema)
    return show_schema(request) 


def remove_field(request):
    step_name = request.GET.get('step_name', '')
    field_name = request.GET.get('field_name', '')
    schema = get_schema()
    [print(field) for field in schema]
    updated_schema = [field for field in schema if field['name'] != field_name]
    set_schema(updated_schema)
    return show_schema(request)

def edit_field(request):
    print('edit_field') 
    step_name = request.GET.get('step_name', '')
    field_name = request.GET.get('field_name', '')
    return show_schema(request, edit_field = field_name)


def show_schema(request, edit_field: str = ''):
    template = loader.get_template('schema/edit_schema.html')   
    schema = get_schema()
    context = {
        'step_name': 'tester',
        'edit_field': edit_field,
        'fields': schema,
        'fields_old': [
            {'name': 'city', 'type': 'String', 'mandatory': True, 'accepted_values': ['Amsterdam', 'Dublin', 'Frankfurt']},
            {'name': 'flag', 'type': 'Boolean', 'mandatory': False, 'accepted_values': None}
        ]
    }
    return HttpResponse(template.render(context, request))
