from django.contrib.postgres.aggregates import ArrayAgg
from django.db import models
from django.db.models import F, Value, Q
from django.db.models.functions import Concat
from django.http import JsonResponse
from django.views.generic.list import BaseListView

from movie.models import Movie, Serial, RoleType


class FilmworkApiMixin:
    http_method_names = ['get']

    def get_queryset(self):
        queryset = self.model.objects.all()
        pk = self.kwargs.get('pk')
        if pk:
            queryset = queryset.get(id=pk)
        return queryset

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context, json_dumps_params={'indent': 4})


class FilmworkDetailApiMixin:

    def get_context_data(self, *args, object_list=None, **kwargs):
        filmwork = args[0]
        context = {
            'id': filmwork.id,
            'title': filmwork.title,
            'description': filmwork.description,
            'creation_date': filmwork.creation_date,
            'rating': filmwork.rating,
            'type': filmwork._meta.verbose_name,
            'genres': [genre.name for genre in filmwork.genres.all()]
        }

        return context


class MovieListApi(FilmworkApiMixin, BaseListView):
    paginate_by = 10
    model = Movie
    context_object_key = 'movies'

    def get_context_data(self, *, object_list=None, **kwargs):

        queryset = self.get_queryset()

        page_size = self.get_paginate_by(queryset)
        paginator, page, queryset, is_paginated = self.paginate_queryset(queryset, page_size)

        if not queryset:
            return {self.context_object_key: []}

        filmwork_type = Value(queryset[0]._meta.verbose_name, output_field=models.CharField())
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
            self.context_object_key: list(movies)
        }
        return context


class MovieDetailApi(FilmworkApiMixin, FilmworkDetailApiMixin, BaseListView):
    model = Movie

    def get_context_data(self, *, object_list=None, **kwargs):
        movie = self.get_queryset()
        context = super().get_context_data(movie)

        context['actors'] = [f'{person.first_name} {person.last_name}' for person in
                             movie.persons.filter(moviepersonrole__role=RoleType.ACTOR)]
        context['directors'] = [f'{person.first_name} {person.last_name}' for person in
                                movie.persons.filter(moviepersonrole__role=RoleType.DIRECTOR)]
        context['writers'] = [f'{person.first_name} {person.last_name}' for person in
                              movie.persons.filter(moviepersonrole__role=RoleType.WRITER)]

        return context


class SerialListApi(FilmworkApiMixin, BaseListView):
    paginate_by = 10
    model = Serial
    context_object_key = 'serials'

    def get_context_data(self, *, object_list=None, **kwargs):

        queryset = self.get_queryset()

        page_size = self.get_paginate_by(queryset)
        paginator, page, queryset, is_paginated = self.paginate_queryset(queryset, page_size)

        if not queryset:
            return {self.context_object_key: []}

        filmwork_type = Value(queryset[0]._meta.verbose_name, output_field=models.CharField())
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
            self.context_object_key: list(serials)
        }
        return context


class SerialDetailApi(FilmworkApiMixin, FilmworkDetailApiMixin, BaseListView):
    model = Serial

    def get_context_data(self, *, object_list=None, **kwargs):
        serial = self.get_queryset()
        context = super().get_context_data(serial)

        context['actors'] = [f'{person.first_name} {person.last_name}' for person in
                             serial.persons.filter(serialpersonrole__role=RoleType.ACTOR)]
        context['directors'] = [f'{person.first_name} {person.last_name}' for person in
                                serial.persons.filter(serialpersonrole__role=RoleType.DIRECTOR)]
        context['writers'] = [f'{person.first_name} {person.last_name}' for person in
                              serial.persons.filter(serialpersonrole__role=RoleType.WRITER)]

        return context
