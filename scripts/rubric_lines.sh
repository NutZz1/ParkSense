#!/usr/bin/env bash
# Markdown table: rubric item -> file:line, from the "// RUBRIC:" tags in the Scala sources
cd "$(dirname "$0")/../spark-scala/src/main/scala/parksense"
echo "| Rubric item | File:line |"; echo "|---|---|"
grep -n "// RUBRIC:" RddPipeline.scala SparkConcepts.scala | grep -v "Lines tagged" | sed -E 's/^([^:]+):([0-9]+):.*\/\/ RUBRIC: (.*)$/| \3 | `\1:\2` |/'
