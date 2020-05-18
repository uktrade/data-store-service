FROM python:3.6

RUN apt-get update -y
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs


ADD requirements.txt /tmp/requirements.txt

ADD scripts scripts
RUN scripts/install_dockerize.sh
RUN scripts/install_psql_client.sh
RUN scripts/install_python_packages.sh

WORKDIR /app

COPY . /app

RUN scripts/compile_assets.sh
CMD /app/scripts/entrypoint.sh
