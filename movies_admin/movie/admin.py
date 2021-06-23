from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from .models import Movie, Person, Genre, MoviePersonRole, SerialPersonRole
from .models import Serial, Season, Episode


class MoviePersonRoleInline(admin.TabularInline):
    model = MoviePersonRole
    fields = ('role', 'person', )
    extra = 0
    ordering = ('role', 'person__last_name', 'person__first_name', )
    classes = ('collapse',)


class SerialPersonRoleInline(admin.TabularInline):
    model = SerialPersonRole
    fields = ('role', 'person', )
    extra = 0
    ordering = ('role', 'person__last_name', 'person__first_name', )
    classes = ('collapse',)


class MovieGenreInline(admin.TabularInline):
    model = Movie.genres.through
    extra = 0
    ordering = ('genre__name', )
    classes = ('collapse', )
    verbose_name_plural = _('genres')


class SerialGenreInline(admin.TabularInline):
    model = Serial.genres.through
    extra = 0
    ordering = ('genre__name', )
    classes = ('collapse', )
    verbose_name_plural = _('genres')


class EpisodeInline(admin.TabularInline):
    model = Episode
    extra = 0
    ordering = ('number', )
    classes = ('collapse',)


class SeasonInline(admin.TabularInline):
    model = Season
    extra = 0
    ordering = ('number', )
    classes = ('collapse',)


@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):

    def creation_year(self, obj):
        return obj.creation_date.strftime("%Y")
    creation_year.short_description = _('year')

    list_display = ('title', 'creation_year', 'rating', 'age_classification', )
    ordering = ('-creation_date', 'title')
    list_filter = ('creation_date', 'rating', 'age_classification', )
    search_fields = ('title', 'description', 'genres__name', )
    list_editable = ('rating', 'age_classification', )
    list_per_page = 10

    fieldsets = (
        (_('movie'), {
            'fields': (('title', 'creation_date', 'rating', 'age_classification', ),)
        }),
        (_('video file'), {
            'classes': ('collapse', ),
            'fields': (('file_path', ),)
        }),
        (_('description and certificate'), {
            'classes': ('collapse', ),
            'fields': (('description',  'certificate', ),)
        })
    )

    inlines = [
        MovieGenreInline,
        MoviePersonRoleInline,
    ]
    save_on_top = True
    save_as = True


@admin.register(Serial)
class SerialAdmin(admin.ModelAdmin):
    def creation_year(self, obj):
        return obj.creation_date.strftime("%Y")
    creation_year.short_description = _('year')

    list_display = ('title', 'creation_year', 'rating', 'age_classification', )
    ordering = ('-creation_date', 'title')
    list_filter = ('creation_date', 'rating', 'age_classification', )
    search_fields = ('title', 'description', 'genres__name', )
    list_editable = ('rating', 'age_classification', )

    fieldsets = (
        (_('serial'), {
            'fields': (('title', 'creation_date', 'rating', 'age_classification', ),)
        }),
        (_('description and certificate'), {
            'classes': ('collapse', ),
            'fields': (('description',  'certificate', ),)
        })
    )

    inlines = [
        SeasonInline,
        SerialGenreInline,
        SerialPersonRoleInline,
    ]

    save_on_top = True
    save_as = True


@admin.register(Season)
class SeasonAdmin(admin.ModelAdmin):

    list_display = ('serial', 'number', )
    ordering = ('serial', 'number',)
    list_filter = ('number', )
    list_display_links = ('number', )
    readonly_fields = ('serial', 'number', 'description', )
    search_fields = ('number', 'description', )
    fields = ('serial', 'number', 'description', )
    list_per_page = 10

    inlines = [
        EpisodeInline,
    ]


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('first_name', 'last_name', 'birth_date', )
    ordering = ('last_name', 'first_name', '-birth_date',)
    list_filter = ('first_name', 'last_name', )
    search_fields = ('last_name', 'first_name', )
    fields = ('first_name', 'last_name', 'birth_date', )
    list_per_page = 10

    inlines = [
        MoviePersonRoleInline,
        SerialPersonRoleInline,
    ]


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    ordering = ('name',)
    list_filter = ('name', )
    search_fields = ('name', )
    list_per_page = 10
