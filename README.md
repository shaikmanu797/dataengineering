# Solution

### Run tests in Docker container
```bash
docker-compose up
```

#### Refer here for [Problem](/src/test/resources/README.md)

### Code
- [Source Code Main](/src/main/scala/dataengineering)
- [Source Code Test](/src/main/test/dataengineering)

### Unit Test Suites
#### Part 0
- [Docker Compose](docker-compose.yml)
- [Dockerfile](Dockerfile)

#### Part 1
- [LoadFraudTest.scala](/src/test/scala/dataengineering/LoadFraudTest.scala)

#### Part 2
- [LoadTransactionTest.scala](/src/test/scala/dataengineering/LoadTransactionTest.scala)

#### Part 3
- [ReportingTest.scala](/src/test/scala/dataengineering/ReportingTest.scala)

### Integration Test Suite
- [IntegrationSuite.scala](/src/test/scala/dataengineering/IntegrationSuite.scala)

#### Outputs
- Part 0 [Run log](/reports/docker.log)
- Part 1 SQL Database Table
- Part 2 [Sanitized Transactions](reports/tasks/txn)
- Part 3 [Fraudulent Txn Per State](reports/tasks/fraud_txn_per_state)
- Part 3 Fraudulent Txn Per Vendor -- Refer to Question below
- Part 3 [Masked Dataset](reports/tasks/masked_dataset)

### HTML Reports
- [Scala docs](/reports/scaladoc/index.html)
- [Code Coverage](/reports/scoverage/index.html)
- [Scala Tests](/reports/tests/index.html)

### Questions
#### In Part 3
`Create a report of the number of fraudulent transactions per card vendor,eg: maestro => 45, amex => 78, etc..`

An instruction was given within the part 2 & 3 as below which would contract the above question, so I have neglected it.
##### *Candidate should assume that going forward, only the sanitized dataset should be used.*
##### *Using PySpark, sanitize data of both transaction-001.zip and transaction-002.zip by removing transactions where column credit_card_number is not part of the previous provided list. example: a credit card that start with 98 is not a valid card, it should be discarded from the sanitized dataset.*