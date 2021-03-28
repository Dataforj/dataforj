from django.http import HttpResponse
from django.template import loader


def open(request):
    template = loader.get_template('project/open.html')
    context = {
        'latest_question_list': 'latest_question_list',
    }
    return HttpResponse(template.render(context, request))

# Leave the rest of the views (detail, results, vote) unchanged