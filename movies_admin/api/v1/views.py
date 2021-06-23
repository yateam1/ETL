from django.contrib.postgres.aggregates import ArrayAgg
from django.db import models
from django.db.models import F, Value, Q
from django.db.models.functions import Concat
from django.http import JsonResponse
from django.views.generic.list import BaseListView

from movie.models import Movie, Serial, Genre, RoleType


class MovieApiMixin:
    http_method_names = ['get']

    def get_queryset(self):
        queryset = Movie.objects.all()
        pk = self.kwargs.get('pk')
        if pk:
            queryset = queryset.get(id=pk)
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context, json_dumps_params={'indent': 4})


class MovieListApi(MovieApiMixin, BaseListView):
    paginate_by = 10

    def get_context_data(self, *, object_list=None, **kwargs):

        queryset = self.get_queryset()
        page_size = self.get_paginate_by(queryset)
        paginator, page, queryset, is_paginated = self.paginate_queryset(queryset, page_size)

        filmwork_type = Value(queryset[0]._meta.verbose_name,
                              output_field=models.CharField()) if queryset else 'movie'
        full_name = Concat(F('moviepersonrole__person__last_name'),
                           Value(' '),
                           F('moviepersonrole__person__first_name')
                           )

        movies = queryset.prefetch_related(
            'genres'
        ).values(
            'id',
            'title',
            'description',
            'creation_date',
            'rating'
        ).annotate(
            type=filmwork_type
        ).annotate(
            genres_names=ArrayAgg('genres__name', distinct=True)
        ).annotate(
            actors=ArrayAgg(full_name, distinct=True, filter=Q(moviepersonrole__role=RoleType.ACTOR))
        ).annotate(
            directors=ArrayAgg(full_name, distinct=True, filter=Q(moviepersonrole__role=RoleType.DIRECTOR))
        ).annotate(
            writers=ArrayAgg(full_name, distinct=True, filter=Q(moviepersonrole__role=RoleType.WRITER))
        )

        context = {
            'movies': list(movies)
        }
        return context


class MovieDetailApi(MovieApiMixin, BaseListView):

    def get_context_data(self, *, object_list=None, **kwargs):
        movie = self.get_queryset()

        context = {
            'id': movie.id,
            'title': movie.title,
            'description': movie.description,
            'creation_date': movie.creation_date,
            'rating': movie.rating,
            'type': movie._meta.verbose_name,
            'genres': [genre.name for genre in movie.genres.all()],
            'actors': [f'{person.first_name} {person.last_name}' for person in
                       movie.persons.filter(moviepersonrole__role=RoleType.ACTOR)],
            'directors': [f'{person.first_name} {person.last_name}' for person in
                          movie.persons.filter(moviepersonrole__role=RoleType.DIRECTOR)],
            'writers': [f'{person.first_name} {person.last_name}' for person in
                        movie.persons.filter(moviepersonrole__role=RoleType.WRITER)]
        }

        return context


class SerialApiMixin:
    http_method_names = ['get']

    def get_queryset(self):
        queryset = Serial.objects.all()
        pk = self.kwargs.get('pk')
        if pk:
            queryset = queryset.get(id=pk)
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context, json_dumps_params={'indent': 4})


class SerialListApi(SerialApiMixin, BaseListView):
    paginate_by = 10

    def get_context_data(self, *, object_list=None, **kwargs):

        queryset = self.get_queryset()
        page_size = self.get_paginate_by(queryset)
        paginator, page, queryset, is_paginated = self.paginate_queryset(queryset, page_size)

        filmwork_type = Value(queryset[0]._meta.verbose_name,
                              output_field=models.CharField()) if queryset else 'movie'
        full_name = Concat(F('serialpersonrole__person__last_name'),
                           Value(' '),
                           F('serialpersonrole__person__first_name')
                           )

        serials = queryset.prefetch_related(
            'genres'
        ).values(
            'id',
            'title',
            'description',
            'creation_date',
            'rating'
        ).annotate(
            type=filmwork_type
        ).annotate(
            genres_names=ArrayAgg('genres__name', distinct=True)
        ).annotate(
            actors=ArrayAgg(full_name, distinct=True, filter=Q(serialpersonrole__role=RoleType.ACTOR))
        ).annotate(
            directors=ArrayAgg(full_name, distinct=True, filter=Q(serialpersonrole__role=RoleType.DIRECTOR))
        ).annotate(
            writers=ArrayAgg(full_name, distinct=True, filter=Q(serialpersonrole__role=RoleType.WRITER))
        )

        context = {
            'serials': list(serials)
        }
        return context


class SerialDetailApi(SerialApiMixin, BaseListView):

    def get_context_data(self, *, object_list=None, **kwargs):
        serial = self.get_queryset()

        context = {
            'id': serial.id,
            'title': serial.title,
            'description': serial.description,
            'creation_date': serial.creation_date,
            'rating': serial.rating,
            'type': serial._meta.verbose_name,
            'genres': [genre.name for genre in serial.genres.all()],
            'actors': [f'{person.first_name} {person.last_name}' for person in
                       serial.persons.filter(serialpersonrole__role=RoleType.ACTOR)],
            'directors': [f'{person.first_name} {person.last_name}' for person in
                          serial.persons.filter(serialpersonrole__role=RoleType.DIRECTOR)],
            'writers': [f'{person.first_name} {person.last_name}' for person in
                        serial.persons.filter(serialpersonrole__role=RoleType.WRITER)]
        }

        return context
