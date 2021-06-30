import uuid

from django.core.validators import MinValueValidator
from django.db import models
from django.utils.formats import date_format
from django.utils.translation import gettext_lazy as _
from model_utils.models import TimeStampedModel

"""
Фильм — заголовок, содержание, дата создания, режиссёры, актёры, сценаристы, жанры, ссылка на файл, возрастной ценз,
Сериал — заголовок, содержание, даты создания, режиссёры, актёры, сценаристы, жанры, ссылка на файл.
Актёр — имя, фамилия, его фильмы.
Режиссёр — имя, фамилия, его фильмы.
Сценарист — имя, фамилия, его фильмы.
Жанр — описание.
"""


class AgeClassification(models.IntegerChoices):
    LEVEL_0 = 0, '0+'
    LEVEL_6 = 6, '6+'
    LEVEL_12 = 12, '12+'
    LEVEL_16 = 16, '16+'
    LEVEL_18 = 18, '18+'


class RoleType(models.IntegerChoices):
    ACTOR = 0, _('actor')
    DIRECTOR = 1, _('director')
    WRITER = 2, _('screenwriter')
    PRODUCER = 3, _('producer')


class Genre(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_('name'), max_length=255, unique=True)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        verbose_name = _('genre')
        verbose_name_plural = _('genres')

    def __str__(self):
        return self.name


class Person(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    first_name = models.CharField(_('first name'), max_length=255)
    last_name = models.CharField(_('last name'), max_length=255)
    birth_date = models.DateField(_('date of birth'), blank=True)

    class Meta:
        verbose_name = _('person')
        verbose_name_plural = _('persons')
        indexes = [
            models.Index(fields=['last_name', 'first_name'], name='person_name_idx'),
        ]

    def __str__(self):
        return f'{self.first_name} {self.last_name} {date_format(self.birth_date)}'


class MoviePersonRole(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    movie = models.ForeignKey('Movie', on_delete=models.DO_NOTHING)
    person = models.ForeignKey(Person, on_delete=models.DO_NOTHING)
    role = models.IntegerField(_('role'), choices=RoleType.choices)

    class Meta:
        verbose_name = _('creator')
        verbose_name_plural = _('creators')
        unique_together = ['movie', 'person', 'role']
        indexes = [
            models.Index(fields=['movie']),
            models.Index(fields=['person']),
        ]

    def __str__(self):
        return f'{self.movie}: {self.role} {self.person}'


class SerialPersonRole(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    serial = models.ForeignKey('Serial', on_delete=models.DO_NOTHING)
    person = models.ForeignKey(Person, on_delete=models.DO_NOTHING)
    role = models.IntegerField(_('role'), choices=RoleType.choices)

    class Meta:
        verbose_name = _('creator')
        verbose_name_plural = _('creators')
        unique_together = ['serial', 'person', 'role']
        indexes = [
            models.Index(fields=['serial']),
            models.Index(fields=['person']),
        ]

    def __str__(self):
        return f'{self.serial}: {self.role} {self.person}'


class Filmwork(TimeStampedModel):
    """
    Это родительский класс, в котором хранится информация о единице кинопроизведения (кроме ссылки на видеофайл).
    Будь то полнометражное кино или эпизод из сериала.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(_('title'), max_length=255, db_index=True)
    description = models.TextField(_('plot'), blank=True)
    creation_date = models.DateField(_('release date'), blank=True)
    certificate = models.TextField(_('certificate'), blank=True)
    age_classification = models.IntegerField(_('age rating system'), choices=AgeClassification.choices, default=18)
    rating = models.FloatField(_('rating'), validators=[MinValueValidator(0)], blank=True)

    class Meta:
        abstract = True
        verbose_name = _('film work')
        verbose_name_plural = _('film works')
        indexes = [
            models.Index(fields=['title']),
        ]

    def __str__(self):
        return self.title


class Movie(Filmwork):
    """
    Фильм - это самостоятельная киноработа со ссылкой на видеофайл.
    """
    genres = models.ManyToManyField(Genre)
    persons = models.ManyToManyField(Person, through=MoviePersonRole)
    file_path = models.URLField(_('link to the file'), unique=True)

    class Meta:
        verbose_name = _('movie')
        verbose_name_plural = _('movies')


class Serial(Filmwork):
    """
    Сериал.
    """
    genres = models.ManyToManyField(Genre)
    persons = models.ManyToManyField(Person, through=SerialPersonRole)

    class Meta:
        verbose_name = _('serial')
        verbose_name_plural = _('serials')


class Season(TimeStampedModel):
    """
    Сезон сериала.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    number = models.PositiveIntegerField(_('number'))
    description = models.TextField(_('plot'), blank=True)
    serial = models.ForeignKey(Serial, on_delete=models.DO_NOTHING)

    class Meta:
        verbose_name = _('season')
        verbose_name_plural = _('seasons')
        unique_together = ['serial', 'number']

    def __str__(self):
        return f'{_("season")} {self.number} '


class Episode(TimeStampedModel):
    """
    Эпизод (серия) сериала.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    number = models.PositiveIntegerField(_('number'))
    season = models.ForeignKey(Season, on_delete=models.DO_NOTHING)
    file_path = models.URLField(_('link to the file'), unique=True)

    class Meta:
        verbose_name = _('episode')
        verbose_name_plural = _('episodes')
        unique_together = ['season', 'number']
