FROM python:3.9

RUN apt-get update -y

ADD scripts scripts
RUN scripts/install_dockerize.sh
RUN scripts/install_psql_client.sh

ADD requirements.txt /tmp/requirements.txt
ADD package.json package-lock.json ./

RUN scripts/install_python_packages.sh

# Install node: based on https://stackoverflow.com/a/57546198/1319998
ENV NODE_VERSION=14.16.1
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash
ENV NVM_DIR=/root/.nvm
RUN . "$NVM_DIR/nvm.sh" && nvm install ${NODE_VERSION}
RUN . "$NVM_DIR/nvm.sh" && nvm use v${NODE_VERSION}
RUN . "$NVM_DIR/nvm.sh" && nvm alias default v${NODE_VERSION}
ENV PATH="/root/.nvm/versions/node/v${NODE_VERSION}/bin/:${PATH}"
RUN node --version
RUN npm --version

# Install node packages
RUN npm install

WORKDIR /app

COPY . /app

CMD /app/scripts/entrypoint.sh
