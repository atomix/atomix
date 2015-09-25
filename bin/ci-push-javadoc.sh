# Called by Travis CI to push latest javadoc
# From http://benlimmer.com/2013/12/26/automatically-publish-javadoc-to-gh-pages-with-travis-ci/

REPO="atomix/copycat"

if [ "$TRAVIS_REPO_SLUG" == "$REPO" ] && \
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ] && \
   [ "$TRAVIS_PULL_REQUEST" == "false" ] && \
   [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e "Publishing javadoc...\n"
  
  mvn javadoc:javadoc -Djv=latest
  TARGET="$(pwd)/target"

  cd $HOME
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/$REPO gh-pages > /dev/null
  
  cd gh-pages
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git rm -rf api/latest 
  mv ${TARGET}/site/apidocs/api/latest api
  git add -A -f api/latest
  git commit -m "Latest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
  git push -fq origin gh-pages > /dev/null

  echo -e "Published Javadoc to gh-pages.\n"
fi
