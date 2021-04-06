from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Div, Submit, HTML, Button, Row, Field
from crispy_forms.bootstrap import AppendedText, PrependedText, FormActions
from webui.dataforj import get_flow

class SelectedStepForm(forms.Form):
    step_name = forms.CharField(label='Step name', max_length=100)

class AddSourceForm(forms.Form):
    step_name = forms.CharField(label='Step name', max_length=100)
    step_description = forms.CharField(label='Step description')
    uri = forms.CharField(label='URI, i.e. location of the source data')
    format_type = forms.CharField(label='Format of the data (e.g. CSV, Parquet, etc)')
    options_text = forms.CharField(widget=forms.Textarea, 
                                   required=False,
                                   label='Options seperated by newlines in the form k=v, e.g. delimiter=True')

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('step_name', css_class='input-xlarge'),
        Field('step_description', css_class='input-xlarge'),
        Field('uri', css_class='input-xlarge'),
        Field('format_type', css_class='input-xlarge'),
        Field('options_text', css_class='input-xlarge'),
        FormActions(
            Submit('new', 'Add Sournce', css_class="btn-primary")
        )
    )


class AddChildForm(forms.Form):
    
    CHOICES=[
        ('sql', 'SQL'),
        ('pyspark', 'PySpark'),
        ('union', 'Union'),
        ('sink', 'Sink')
    ]

    step_type = forms.ChoiceField(label='Choose the type of step you wish to create', choices=CHOICES, widget=forms.RadioSelect)

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('step_type', css_class='input-xlarge')
    )


def create_dependson_tuple(flow):
    depends_on_list=[step.name for step in flow._steps.values() if step.dagre_type != 'Sink']
    return tuple((name, name) for name in depends_on_list)


class AddSinkForm(forms.Form):

    step_name = forms.CharField(label='Step name', max_length=100)
    step_description = forms.CharField(label='Step description')
    depends_ons = forms.ChoiceField(label='Which step should become input for this one', choices=[])

    uri = forms.CharField(label='URI, i.e. location of the source data')
    format_type = forms.CharField(label='Format of the data (e.g. CSV, Parquet, etc)')
    mode = forms.CharField(label='Mode (e.g. overwrite)')
    options_text = forms.CharField(widget=forms.Textarea, 
                                   label='Options seperated by newlines in the form k=v, e.g. delimiter=True',
                                   required=False)

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('step_type', css_class='input-xlarge'),
        Field('step_name', css_class='input-xlarge'),
        Field('step_description', css_class='input-xlarge'),
        Field('depends_ons', rows="3", css_class='input-xlarge'),
        Field('uri', css_class='input-xlarge'),
        Field('format_type', css_class='input-xlarge'),
        Field('mode', css_class='input-xlarge'),
        Field('options_text', css_class='input-xlarge')
    )

    def __init__(self, *args, **kwargs):
        flow = kwargs.pop('flow')
        super(AddSinkForm, self).__init__(*args, **kwargs)
        self.fields['depends_ons'].choices = create_dependson_tuple(flow)


class AddUnionForm(forms.Form):

    step_name = forms.CharField(label='Step name', max_length=100)
    step_description = forms.CharField(label='Step description')
    depends_ons = forms.MultipleChoiceField(label='Which steps should become input for this one', choices=[])

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('step_type', css_class='input-xlarge'),
        Field('step_name', css_class='input-xlarge'),
        Field('step_description', css_class='input-xlarge'),
        Field('depends_ons', rows="3", css_class='input-xlarge')
    )

    def __init__(self, *args, **kwargs):
        flow = kwargs.pop('flow')
        super(AddUnionForm, self).__init__(*args, **kwargs)
        self.fields['depends_ons'].choices = create_dependson_tuple(flow)


class AddSqlForm(forms.Form):

    step_name = forms.CharField(label='Step name', max_length=100)
    step_description = forms.CharField(label='Step description')
    depends_ons = forms.MultipleChoiceField(label='Which steps should become input for this one', choices=[])

    sql_file_path = forms.CharField(label='Location where the SQL code will be stored')

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('step_type', css_class='input-xlarge'),
        Field('step_name', css_class='input-xlarge'),
        Field('step_description', css_class='input-xlarge'),
        Field('depends_ons', rows="3", css_class='input-xlarge'),
        Field('sql_file_path', css_class='input-xlarge')
    )

    def __init__(self, *args, **kwargs):
        flow = kwargs.pop('flow')
        super(AddSqlForm, self).__init__(*args, **kwargs)
        self.fields['depends_ons'].choices = create_dependson_tuple(flow)


class AddPySparkForm(forms.Form):

    step_name = forms.CharField(label='Step name', max_length=100)
    step_description = forms.CharField(label='Step description')
    depends_ons = forms.MultipleChoiceField(label='Which steps should become input for this one', choices=[])

    pyspark_file_path = forms.CharField(label='Location where the PySpark code will be stored')

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('step_type', css_class='input-xlarge'),
        Field('step_name', css_class='input-xlarge'),
        Field('step_description', css_class='input-xlarge'),
        Field('depends_ons', rows="3", css_class='input-xlarge'),
        Field('pyspark_file_path', css_class='input-xlarge')
    )

    def __init__(self, *args, **kwargs):
        flow = kwargs.pop('flow')
        super(AddPySparkForm, self).__init__(*args, **kwargs)
        self.fields['depends_ons'].choices = create_dependson_tuple(flow)
