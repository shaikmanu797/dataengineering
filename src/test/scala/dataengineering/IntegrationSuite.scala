package dataengineering

import org.scalatest.Suites

class IntegrationSuite extends Suites(
  new LoadFraudTest,
  new LoadTransactionTest,
  new ReportingTest
)