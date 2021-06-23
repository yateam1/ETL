# Создаем фикстурные данные
import random
from datetime import datetime

import factory
from factory.django import DjangoModelFactory
from factory.fuzzy import FuzzyChoice
from django.db import transaction

from .models import Movie, Person, Genre, Episode, Season, RoleType
from .models import Serial, AgeClassification, MoviePersonRole, SerialPersonRole

PERSONS_COUNT = 30  # 1000
GENRES_COUNT = 20  # 10000
ROLES_COUNT = len(RoleType)
MOVIES_COUNT = 75  # 1000000
SERIALS_COUNT = 20  # 200000
SEASONS_MAX = 9  # максимальное количество сезонов на каждый сериал
EPISODES_MAX = 9  # максимальное количество эпизодов на каждый сезон
RATINGS = range(0, 11)
AGE_IDS = [age[0] for age in AgeClassification.choices]


class PersonFactory(DjangoModelFactory):
    class Meta:
        model = Person

    first_name = factory.Sequence(lambda n: f'First name{n}')
    last_name = factory.Sequence(lambda n: f'Last name{n}')
    birth_date = factory.LazyFunction(datetime.now)


class GenreFactory(DjangoModelFactory):
    class Meta:
        model = Genre

    name = factory.Sequence(lambda n: f'Genre_{n}')


class MovieFactory(DjangoModelFactory):
    class Meta:
        model = Movie

    title = factory.Sequence(lambda n: f'Movie title: {n}')
    description = factory.Sequence(lambda n: f'Plot of movie {n}')
    creation_date = factory.LazyFunction(datetime.now)
    certificate = factory.Sequence(lambda n: f'Certificate {n}')
    rating = FuzzyChoice(RATINGS)
    age_classification = FuzzyChoice(AGE_IDS)
    file_path = factory.Sequence(lambda n: f'Link to file{n}')

    @factory.post_generation
    def genres(self, create, extracted, **kwargs):
        ThroughModel = Movie.genres.through

        _genres = [
            ThroughModel(
                genre=genre,
                movie=self
            )
            for genre in Genre.objects.all().order_by('?')[:random.randint(1, GENRES_COUNT + 1)]
        ]
        ThroughModel.objects.bulk_create(_genres)

    @factory.post_generation
    def persons(self, create, extracted, **kwargs):
        _relations = [
            MoviePersonRole(
                movie=self,
                person=person,
                role=role
            )
            for person in Person.objects.all().order_by('?')[:random.randint(1, PERSONS_COUNT + 1)]
            for role in random.sample(RoleType.values, random.randint(1, ROLES_COUNT))]

        MoviePersonRole.objects.bulk_create(_relations)


class SerialFactory(DjangoModelFactory):
    class Meta:
        model = Serial

    title = factory.Sequence(lambda n: f'Serial title: {n}')
    description = factory.Sequence(lambda n: f'Plot of serial {n}')
    creation_date = factory.LazyFunction(datetime.now)
    certificate = factory.Sequence(lambda n: f'Certificate for serial {n}')
    rating = FuzzyChoice(RATINGS)
    age_classification = FuzzyChoice(AGE_IDS)

    @factory.post_generation
    def genres(self, create, extracted, **kwargs):
        ThroughModel = Serial.genres.through

        _genres = [
            ThroughModel(
                genre=genre,
                serial=self
            )
            for genre in Genre.objects.all().order_by('?')[:random.randint(1, GENRES_COUNT + 1)]
        ]
        ThroughModel.objects.bulk_create(_genres)

    @factory.post_generation
    def persons(self, create, extracted, **kwargs):
        _relations = [
            SerialPersonRole(
                serial=self,
                person=person,
                role=role
            )
            for person in Person.objects.all().order_by('?')[:random.randint(1, PERSONS_COUNT + 1)]
            for role in random.sample(RoleType.values, random.randint(1, ROLES_COUNT))
        ]
        SerialPersonRole.objects.bulk_create(_relations)

    @factory.post_generation
    def seasons(self, create, extracted, **kwargs):

        _seasons = []
        _episodes = []

        for season_number in range(0, random.randint(1, SEASONS_MAX + 1)):
            season = Season(
                    number=season_number + 1,
                    description=f'Seasons {season_number + 1} description',
                    serial=self
            )
            _seasons.append(season)

            for episode_number in range(0, random.randint(1, SEASONS_MAX + 1)):
                episode = Episode(
                    number=episode_number + 1,
                    season=season,
                    file_path=f'Link {self.title}/{season_number}/{episode_number}'
                )
                _episodes.append(episode)

        Season.objects.bulk_create(_seasons)
        Episode.objects.bulk_create(_episodes)


class EpisodeFactory(DjangoModelFactory):
    class Meta:
        model = Episode


@transaction.atomic
def make_objects():
    MoviePersonRole.objects.all().delete()
    SerialPersonRole.objects.all().delete()
    Episode.objects.all().delete()
    Season.objects.all().delete()

    Person.objects.all().delete()
    PersonFactory.create_batch(PERSONS_COUNT)

    Genre.objects.all().delete()
    GenreFactory.create_batch(GENRES_COUNT)

    Movie.objects.all().delete()
    MovieFactory.create_batch(MOVIES_COUNT)

    Serial.objects.all().delete()
    SerialFactory.create_batch(SERIALS_COUNT)
