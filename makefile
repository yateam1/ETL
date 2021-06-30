THIS_FILE := $(lastword $(MAKEFILE_LIST))
.PHONY: help build up start down destroy stop restart logs logs-api ps login-timescale login-api db-shell
help:
	make -pRrq  -f $(THIS_FILE) : 2>/dev/null | awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'

build:
	cd movies_admin && docker-compose build

up:
	cd movies_admin && docker-compose up -d
	cd postgres_to_es && docker-compose up -d

restart:
	cd movies_admin && docker-compose restart
	cd postgres_to_es && docker-compose restart

down:
	cd movies_admin && docker-compose down
	cd postgres_to_es && docker-compose down