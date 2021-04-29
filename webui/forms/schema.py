from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Div, Submit, HTML, Button, Row, Field
from crispy_forms.bootstrap import AppendedText, PrependedText, FormActions
from webui.dataforj import get_flow


class EditSchemaForm(forms.Form):
    field_name = forms.CharField()
    field_type = forms.CharField()
    field_mandatory = forms.CharField()
    field_accepted_values = forms.CharField()
