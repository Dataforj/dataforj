from django import forms
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Div, Submit, HTML, Button, Row, Field
from crispy_forms.bootstrap import AppendedText, PrependedText, FormActions
from webui.dataforj import get_flow


class RunSqlForm(forms.Form):
    step_name = forms.CharField()
    sql = forms.CharField()


class RunPySparkForm(forms.Form):
    step_name = forms.CharField()
    pyspark_code = forms.CharField()