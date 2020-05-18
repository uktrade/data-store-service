FROM python:3.6

RUN apt-get update -y
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs


ADD requirements.txt /tmp/requirements.txt
ADD package.json package.json
ADD package-lock.json package-lock.json
ADD webpack.config.js webpack.config.js
ADD static static
ADD scripts scripts

RUN scripts/install_dockerize.sh
RUN scripts/install_psql_client.sh
RUN scripts/install_python_packages.sh
RUN scripts/compile_assets.sh

WORKDIR /app

COPY . /app

CMD /app/scripts/entrypoint.sh
