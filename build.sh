#!/bin/bash
echo "📦 Compilation du code CloudSim..."

mkdir -p classes

if [ ! -d "lib" ] || [ -z "$(ls -A lib/*.jar 2>/dev/null)" ]; then
    echo "❌ Aucun fichier JAR trouve dans lib/"
    exit 1
fi

javac -cp "lib/*" -d classes src/ReplicaPlacementSimulationOptimized2.java

if [ $? -eq 0 ]; then
    echo "✅ Compilation reussie !"
else
    echo "❌ Erreur de compilation"
    exit 1
fi