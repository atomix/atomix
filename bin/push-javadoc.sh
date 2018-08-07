#!/bin/sh
# Pushes javadocs for an given release version
# Run from top level dir

echo "Enter the API version to generate docs for: "
read apiVersion

mvn javadoc:aggregate -Djv=$apiVersion
rm -rf target/docs
git clone git@github.com:atomix/atomix.github.io.git target/docs > /dev/null
cd target/docs
git rm -rf api/$apiVersion
mkdir -p api/$apiVersion
mv -v ../site/apidocs/* api/$apiVersion
git add -A -f api/$apiVersion
git commit -m "Updated JavaDocs for $apiVersion"
git push -fq origin master > /dev/null

echo "Published $apiVersion Javadoc to atomix.github.io.\n"