# From https://coderwall.com/p/9b_lfq

REPO="atomix/copycat"

if [ "$TRAVIS_REPO_SLUG" == "$REPO" ] && \
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ] && \
   [ "$TRAVIS_PULL_REQUEST" == "false" ] && \
   [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e "Publishing maven snapshot...\n"

  wget https://raw.githubusercontent.com/$REPO/master/bin/settings.xml
  mvn clean source:jar deploy --settings=settings.xml -DskipTests=true -Dmaven.javadoc.skip=true

  echo -e "Published maven snapshot"
fi