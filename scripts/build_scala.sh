#!/usr/bin/env bash
cd "$(dirname "$0")/../spark-scala" && source ../scripts/env.sh && sbt -batch package
