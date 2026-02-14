# Apache Beam Examples - Chapter 07

Extracted from **Chapter 7** and arranged into the standard layout:

- `chapter-07/java-project/`  (Maven)
- `chapter-07/python-project/` (with `requirements.txt`)

This chapter focuses on **runners & deployment** (Dataflow, Flink, Spark) for both batch and streaming.

## Quick start (Python)
```bash
cd chapter-07/python-project
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt
python dataflow_batch_wordcount.py --help
```

## Quick start (Java)
```bash
cd chapter-07/java-project
mvn -q -DskipTests package
# Example:
mvn -q exec:java -Dexec.mainClass=com.example.chapter07.dataflow.WordCountDataflow
```

> Replace placeholders like `YOUR_GCP_PROJECT_ID` and `YOUR_BUCKET` before running.
