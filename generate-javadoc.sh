#!/bin/bash
mvn javadoc:javadoc
cp -r target/site/apidocs/ ../davidmoten.github.io/rxjava-jdbc/
