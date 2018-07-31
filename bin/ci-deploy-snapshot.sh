# From https://coderwall.com/p/9b_lfq

REPO="atomix/atomix"

if [ "$TRAVIS_REPO_SLUG" == "$REPO" ] && \
   [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ] && \
   [ "$TRAVIS_PULL_REQUEST" == "false" ] && \
   [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e "Publishing maven snapshot...\n"

  $TRAVIS_BUILD_DIR/mvnw clean source:jar deploy --batch-mode --settings="bin/settings.xml" -Dmaven.test.skip=true -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn

  echo -e "Published maven snapshot"
fi