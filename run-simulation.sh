#!/bin/bash
echo "🚀 Lancement de la simulation CloudSim..."

mkdir -p ~/results

java -cp "lib/*:classes" ReplicaPlacementSimulationOptimized2

if [ $? -eq 0 ]; then
    echo "✅ Simulation terminee !"
    mkdir -p results
    cp ~/results/*.csv results/ 2>/dev/null
    echo "📁 Resultats sauvegardes dans results/"
else
    echo "❌ Erreur lors de l'execution"
    exit 1
fi