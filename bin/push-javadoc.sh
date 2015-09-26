#!/bin/sh
# Pushes javadocs for an given release version
# Run from top level dir

REPO="atomix/copycat"

echo "Enter the API version to generate docs for: "
read apiVersion

rm -rf target/docs
git clone git@github.com:$REPO.git target/docs -b gh-pages > /dev/null
mvn javadoc:javadoc -Djv=$apiVersion
git rm -rf target/docs/api/$apiVersion
mv target/site/apidocs/api/$apiVersion target/docs/api
cd target/docs
git add -A -f api/$apiVersion
git commit -m "Updated JavaDocs for $apiVersion"
git push -fq origin gh-pages > /dev/null

echo "Published Javadoc to gh-pages.\n"