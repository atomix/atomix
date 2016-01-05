package io.atomix.testing;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;
import org.testng.IClass;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

/**
 * Logs test invocations.
 */
public class TestCaseLogger implements IInvokedMethodListener {
  private static final Map<ITestNGMethod, Long> START_TIMES = new ConcurrentHashMap<>();

  public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
    if (!method.isTestMethod())
      return;

    ITestNGMethod testMethod = method.getTestMethod();
    IClass clazz = testMethod.getTestClass();

    START_TIMES.put(testMethod, System.currentTimeMillis());
    LoggerFactory.getLogger("BEFORE").info("{}#{}", clazz.getRealClass().getName(), testMethod.getMethodName());
  }

  public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
    if (!method.isTestMethod())
      return;

    ITestNGMethod testMethod = method.getTestMethod();
    IClass clazz = testMethod.getTestClass();
    double elapsed = (System.currentTimeMillis() - START_TIMES.remove(testMethod)) / 1000.0;

    if (elapsed > 1)
      LoggerFactory.getLogger("AFTER").info("{}#{} Ran for {} seconds", clazz.getRealClass().getName(),
          testMethod.getMethodName(), elapsed);
  }
}
