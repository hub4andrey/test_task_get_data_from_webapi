#!/bin/bash

source /srv/app/backend_app1/sh_script/init_load_env.sh

DIR_PROJ_ROOT=/srv/app/backend_app1
cd $DIR_PROJ_ROOT/src

/usr/local/bin/python -m main --run email_portfolio_monthly_return >> /var/log/cron.log 2>&1