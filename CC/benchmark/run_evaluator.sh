#!/bin/bash

# Run the Java CC Evaluator
# Usage: ./run_evaluator.sh [InitialProgram.java] [output.json]
#
# Arguments:
#   InitialProgram.java - Path to the CC implementation file (default: InitialProgram.java)
#   output.json         - Path to output JSON file (default: output.json)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Parse arguments
INPUT_FILE="${1:-InitialProgram.java}"
OUTPUT_FILE="${2:-output.json}"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found"
    exit 1
fi

# If input file is not in current directory, copy it
INPUT_BASENAME=$(basename "$INPUT_FILE")
if [ "$INPUT_FILE" != "$INPUT_BASENAME" ] && [ "$INPUT_FILE" != "./$INPUT_BASENAME" ]; then
    cp "$INPUT_FILE" "$INPUT_BASENAME"
fi

echo "Compiling Java files..."
javac -d . KVStore.java InitialProgram.java Evaluator.java

if [ $? -ne 0 ]; then
    echo "Error: Compilation failed"
    exit 1
fi

echo "Running evaluator..."
java -Xmx120g benchmark.Evaluator --output "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo ""
    echo "Results:"
    cat "$OUTPUT_FILE"
else
    echo "Error: Evaluation failed"
    exit 1
fi





