# Chapter 4 — Code


## Structure

- `chapter-04/java-project/` — Apache Beam Java examples (Maven)
- `chapter-04/python-project/` — Apache Beam Python examples

## Java (Maven)

From `chapter-04/java-project/`:

```bash
mvn -q -DskipTests package
# Run a batch example
mvn -q exec:java -Dexec.mainClass=com.example.chapter04.BatchWordCount
```

> Some examples (Kafka/Flink) require external infrastructure and runner-specific options; see class-level comments.

## Python

From `chapter-04/python-project/`:

```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
python script.py
```
