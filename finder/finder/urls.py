from django.conf.urls import patterns, include, url

from django.contrib import admin
from suggest.views import SuggestView
admin.autodiscover()

urlpatterns = patterns('',
    url(r'^(?P<uid>\d+)/suggest$', SuggestView.as_view()),
)
