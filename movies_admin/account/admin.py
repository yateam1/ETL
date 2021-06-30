from typing import Set
from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin


admin.site.unregister(User)


@admin.register(User)
class CustomUserAdmin(UserAdmin):

    readonly_fields = ('date_joined', )

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        is_superuser = request.user.is_superuser
        disabled_fields = set()  # type: Set[str]
        if not is_superuser:
            disabled_fields |= {
                'username',           # NOTE не суперпользователь не может менять имя пользователя
                'is_superuser',       # NOTE не суперпользователь не может ставить статус суперпользователя
                'user_permissions',   # NOTE теперь управляем разрешениями только с помощью групп
            }
        for f in disabled_fields:
            if f in form.base_fields:
                form.base_fields[f].disabled = True
        return form
