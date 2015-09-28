#!/bin/sh
# Pushes javadocs for an given release version
# Run from top level dir

PROJECT=atomix

echo "Enter the API version to generate docs for: "
read apiVersion

rm -rf target/docs
git clone git@github.com:atomix/atomix.github.io.git target/docs > /dev/null
mvn javadoc:javadoc -Djv=$apiVersion
git rm -rf target/docs/$PROJECT/api/$apiVersion
mv target/site/apidocs/api/$apiVersion target/docs/$PROJECT/api
cd target/docs
git add -A -f $PROJECT/api/$apiVersion
git commit -m "Updated JavaDocs for $apiVersion"
git push -fq origin master > /dev/null

echo "Published $apiVersion Javadoc to atomix.github.io.\n"