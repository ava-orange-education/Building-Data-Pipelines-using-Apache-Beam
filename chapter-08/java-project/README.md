# Chapter 08 - Java Project

## Run dead-letter demo
```bash
mvn -q -DskipTests package
mvn -q exec:java -Dexec.mainClass=com.example.chapter08.DeadLetterDemo
```

## Run tests
```bash
mvn -q test
```
