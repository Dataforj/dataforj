from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Div, Submit, HTML, Button, Row, Field
from crispy_forms.bootstrap import AppendedText, PrependedText, FormActions

class AddSourceForm(forms.Form):
    step_name = forms.CharField(label='Step name', max_length=100)
    step_description = forms.CharField(label='Step description')
    uri = forms.CharField(label='URI, i.e. location of the source data')
    format_type = forms.CharField(label='Forat of the data (e.g. CSV, Parquet, etc)')
    options_text = forms.CharField(widget=forms.Textarea, label='Options seperated by newlines in the form k=v, e.g. delimiter=True')

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