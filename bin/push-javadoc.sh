#!/bin/sh
# Pushes javadocs for an given release version
# Run from top level dir

PROJECT=atomix

echo "Enter the API version to generate docs for: "
read apiVersion

mvn javadoc:javadoc -Djv=$apiVersion
rm -rf target/docs
git clone git@github.com:atomix/atomix.github.io.git target/docs > /dev/null
git rm -rf target/docs/$PROJECT/api/$apiVersion
mkdir -p target/docs/$PROJECT/api/$apiVersion
mv -v target/site/apidocs/* target/docs/$PROJECT/api/$apiVersion
cd target/docs
git add -A -f $PROJECT/api/$apiVersion
git commit -m "Updated JavaDocs for $apiVersion"
git push -fq origin master > /dev/null

echo "Published $apiVersion Javadoc to atomix.github.io.\n"