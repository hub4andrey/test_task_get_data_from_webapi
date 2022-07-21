#!/bin/bash
DIR_PROJ_ROOT='/srv/app/backend_app1'

cd /srv/app/backend_app1

declare -p | grep -E 'declare -x DB_HOST=|declare -x DB_NAME=|declare -x BASHOPTS=|declare -x BASH_VERSINFO=|declare -x BASH=|declare -x LANG=|declare -x PATH=' > container.env

crontab ./cron/crontab_user.cron
cron -f
