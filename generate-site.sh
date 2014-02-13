#!/bin/bash
mvn site
cp -r target/site/* ../davidmoten.github.io/rxjava-jdbc/
