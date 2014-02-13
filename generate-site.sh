#!/bin/bash
set -e
mvn site
cd ../davidmoten.github.io
git pull
cp -r ../rxjava-jdbc/target/site/* rxjava-jdbc/
git add .
git commit -am "update site reports"
git push
