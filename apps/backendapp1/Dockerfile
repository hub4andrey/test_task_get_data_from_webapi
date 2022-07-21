# syntax=docker/dockerfile:1
FROM python:3.9-slim-buster

ARG DIR_PROJECT_ROOT=/srv/app/backend_app1
ARG GEN_USER=gen_user_1

RUN apt-get update && \
    apt-get -y install cron vim && \
    useradd -ms /bin/bash $GEN_USER

WORKDIR $DIR_PROJECT_ROOT
RUN chmod u+s /usr/sbin/cron && \ 
    touch /var/log/cron.log && \
    chmod a+w /var/log/cron.log && \
    touch container.env

COPY  . .
RUN chown -R $GEN_USER:$GEN_USER $DIR_PROJECT_ROOT

USER $GEN_USER
WORKDIR $DIR_PROJECT_ROOT

RUN python -m pip install --upgrade pip  && \
    pip install --no-cache-dir  -r ${DIR_PROJECT_ROOT}/requirements.txt

RUN chmod -R 0755 ./sh_script/*

ENTRYPOINT ["/bin/bash","docker_entrypoint.sh"]

# EXPOSE 8000/tcp