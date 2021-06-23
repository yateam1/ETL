# Панель администратора

## Необходимые требования
Для разврачивания проекта на хостовой машине необходимо наличие:
1. [Docker](https://docs.docker.com/engine/install/)  
1. [docker-compose](https://docs.docker.com/compose/install/)
1. VCS, например [GIT](https://git-scm.com/download/win)

## Копирование и запуск проекта
1. Скопировать/склонировать репозиторий на хостовую машину,
1. В директории **movies_admin** проекта переименовать файл .env.dist в .env
1. В файле .env заполнить учетные данные
1. В директории **movies_admin** проекта выполнить две команды
    - ```$ docker-compose build```
    - ```$ docker-compose up-d```
    
## Остановка проекта
В директории **movies_admin** проекта выполнить команду ```$ docker-compose down```