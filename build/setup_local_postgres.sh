#!/bin/bash
set -eux

if [[ $(uname -s) == "Linux" ]]; then
  if [[ $(sudo --version) ]]; then
    SUDO=sudo
  else
    SUDO=
  fi
  # install and setup postgres on Linux
  if [[ $(dpkg -l | grep 'ii  postgresql ') ]]; then
    $SUDO /etc/init.d/postgresql start || echo ""
    echo "postgresql: Installed already"
  else
    echo "postgresql: Installing ..."
    $SUDO apt update
    $SUDO apt install -y postgresql
    $SUDO /etc/init.d/postgresql start
    echo "postgresql: Installation successful"
  fi
  su - postgres -c "psql << end_of_sql
alter user postgres with password 'postgres';
drop database tmp_db;
create database tmp_db;
end_of_sql"

elif [[ $(uname -s) == "Darwin" ]]; then
  # install and setup postgres on Mac
  if [[ $(which postgres) ]]; then
    echo "postgresql: Installed already"
  else
    echo "postgresql: Installing ..."
    brew install postgresql@14
    brew services restart postgresql@14
    sleep 5
    brew services info postgresql@14 --json | jq '.[0].running' | grep true
    echo "postgresql: Installation successful"
  fi
  dropdb tmp_db --if-exists
  createdb tmp_db
  echo "create user postgres superuser" | psql tmp_db || echo "User postgres exists"
  echo "alter user postgres with password 'postgres'" | psql tmp_db
  echo "alter user postgres with superuser;" | psql tmp_db
fi

export DB_NAME=tmp_db DB_PORT=5432 DB_ADMIN_USERNAME=postgres DB_ADMIN_PASSWORD=postgres DB_HOST=localhost SSL_MODE=disable
