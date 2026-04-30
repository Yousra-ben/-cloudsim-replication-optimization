#!/bin/bash
echo "Test simple..."

# Nettoyer
rm -rf classes
mkdir -p classes

# Compiler le test
javac -cp "lib/*" -d classes src/Test.java

# Exécuter
java -cp "lib/*:classes" Test
