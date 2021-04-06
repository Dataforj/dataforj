"""webui URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from webui.views import project, dag, run

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', project.open),
    path('project/open', project.open),
    path('project/new', project.new),
    path('dag/view', dag.view),
    path('dag/add_source', dag.add_source),
    path('dag/run_delete_add_step', dag.run_delete_add_step),
    path('dag/add_step_submit', dag.add_step_submit),
    path('dag/add_sql', dag.add_sql),
    path('dag/add_pyspark', dag.add_pyspark),
    path('dag/add_union', dag.add_union),
    path('dag/add_sink', dag.add_sink),
    path('run/run_sql_submit', run.run_sql_submit),
    path('run/run_pyspark_submit', run.run_pyspark_submit)
]
