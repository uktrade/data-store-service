FROM python:3.6

RUN apt-get update -y

ADD scripts scripts
RUN scripts/install_dockerize.sh
RUN scripts/install_psql_client.sh

ADD requirements.txt /tmp/requirements.txt
ADD package.json package-lock.json ./

RUN scripts/install_python_packages.sh
RUN scripts/install_node.sh
RUN npm install

WORKDIR /app

COPY . /app

CMD /app/scripts/entrypoint.sh
