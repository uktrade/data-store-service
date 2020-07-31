FROM python:3.6

RUN apt-get update -y

ADD requirements.txt /tmp/requirements.txt

ADD scripts scripts
RUN scripts/install_dockerize.sh
RUN scripts/install_psql_client.sh
RUN scripts/install_python_packages.sh
RUN scripts/install_node.sh

WORKDIR /app

COPY . /app

RUN npm install
CMD /app/scripts/entrypoint.sh
