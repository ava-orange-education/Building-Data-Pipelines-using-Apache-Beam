# Apache Beam Examples - Chapter 05

This folder contains the extracted code examples from **Chapter 5**, arranged into:

- `chapter-05/java-project/`  (Standard Maven structure)
- `chapter-05/python-project/` (with `requirements.txt`)

## Java
```bash
cd chapter-05/java-project
mvn -q -DskipTests package
# Run examples (pick one):
mvn -q exec:java -Dexec.mainClass=com.example.chapter05.MapSquareExample
mvn -q exec:java -Dexec.mainClass=com.example.chapter05.FilterLongWordsExample
mvn -q exec:java -Dexec.mainClass=com.example.chapter05.GroupByKeySalesExample
mvn -q exec:java -Dexec.mainClass=com.example.chapter05.CoGroupByKeyJoinExample
mvn -q exec:java -Dexec.mainClass=com.example.chapter05.PartitionEvenOddExample
```

## Python
```bash
cd chapter-05/python-project
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
python script.py
```
