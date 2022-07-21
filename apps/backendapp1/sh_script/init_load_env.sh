#!/bin/bash

DIR_PROJ_ROOT=/srv/app/backend_app1

# # How to copy ENV variables from file into non-interactive shell session:
# # Method 1: r
# export $(cat ${DIR_PROJ_ROOT}/container.env | xargs)
# # Method 2: add into crontab script below lien at the very top:
# SHELL=/bin/bash
# BASH_ENV=/srv/app/backend_app1/container.env 


# Customize the environment still further by explicitly setting variables.
#export myvar1=value
#export myvar2=value
#export myvar3=value




# Example Generate an environment list file.
/usr/bin/env | sort > /tmp/cronEnv.out



# You can now change the current directory if you need to.
cd $DIR_PROJ_ROOT/src
