from django.urls import path

from .views import MovieListApi, MovieDetailApi, SerialListApi, SerialDetailApi

urlpatterns = [
    path('movies/', MovieListApi.as_view()),
    path('movies/<uuid:pk>/', MovieDetailApi.as_view()),
    path('serials/', SerialListApi.as_view()),
    path('serials/<uuid:pk>/', SerialDetailApi.as_view()),
]