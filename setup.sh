#!/bin/bash
echo "Iniciando instalação..."

pip install psycopg2

pip install tabulate psycopg2

sudo apt-get install --reinstall libpq-dev

echo "Instação concluída!"