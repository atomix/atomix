# Called by Travis CI to push latest javadoc
# From http://benlimmer.com/2013/12/26/automatically-publish-javadoc-to-gh-pages-with-travis-ci/

PROJECT=atomix

if [ "$TRAVIS_REPO_SLUG" == "atomix/$PROJECT" ] && \
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ] && \
   [ "$TRAVIS_PULL_REQUEST" == "false" ] && \
   [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e "Publishing Javadoc...\n"
  
  mvn javadoc:aggregate -Djv=latest --batch-mode -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn
  TARGET="$(pwd)/target"

  cd $HOME
  git clone --quiet https://${GH_TOKEN}@github.com/atomix/atomix.github.io gh-pages > /dev/null
  
  cd gh-pages
  git config user.email "travis@travis-ci.org"
  git config user.name "travis-ci"
  git rm -rf docs/latest/api
  mkdir -p docs/latest/api
  mv -v $TARGET/site/apidocs/* docs/latest/api
  git add -A -f docs/latest/api
  git commit -m "Travis generated Javadoc for $PROJECT build $TRAVIS_BUILD_NUMBER"
  git push -fq origin > /dev/null

  echo -e "Published Javadoc to atomix.github.io.\n"
fi
