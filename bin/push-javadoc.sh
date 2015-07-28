#!/bin/sh
# run from top level dir

echo "Enter the API version to generate docs for: "
read apiVersion

rm -rf target/docs
git clone git@github.com:kuujo/copycat.git target/docs -b gh-pages
mvn javadoc:javadoc -Pjavadoc -Djv=$apiVersion
cd target/docs
git add -A
git commit -m "Updated JavaDocs"
git push origin gh-pages