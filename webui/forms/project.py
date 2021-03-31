from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Div, Submit, HTML, Button, Row, Field
from crispy_forms.bootstrap import AppendedText, PrependedText, FormActions

class NewProjectForm(forms.Form):
    project_name = forms.CharField(label='Dataforj project name', max_length=100)
    project_description = forms.CharField(label='Description of the project')
    project_location = forms.CharField(label='Dataforj project location')

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('project_name', css_class='input-xlarge'),
        Field('project_description', css_class='input-xlarge'),
        Field('project_location', css_class='input-xlarge'),
        FormActions(
            Submit('new', 'New Project', css_class="btn-primary")
        )
    )


class ProjectOpenForm(forms.Form):
    project_location = forms.CharField(label='Dataforj project location')

    helper = FormHelper()
    helper.form_class = 'form-horizontal'
    helper.layout = Layout(
        Field('project_location', css_class='input-xlarge')
    )
