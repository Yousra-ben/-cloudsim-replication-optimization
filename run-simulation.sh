#!/bin/bash
echo "========================================="
echo "🔍 DIAGNOSTIC COMPLET"
echo "========================================="

echo "📁 1. Contenu du dossier actuel :"
ls -la

echo ""
echo "📁 2. Contenu du dossier src/ :"
ls -la src/

echo ""
echo "📁 3. Vérification du fichier Java :"
if [ -f "src/ReplicaPlacementSimulationOptimized2.java" ]; then
    echo "✅ Fichier trouvé : src/ReplicaPlacementSimulationOptimized2.java"
    echo "   Premières lignes du fichier :"
    head -20 src/ReplicaPlacementSimulationOptimized2.java
else
    echo "❌ Fichier NON trouvé !"
    echo "   Recherche d'autres fichiers Java :"
    find . -name "*.java" -type f
fi

echo ""
echo "📁 4. Contenu du dossier lib/ :"
ls -la lib/

echo ""
echo "📁 5. Compilation..."
rm -rf classes
mkdir -p classes

javac -cp "lib/*" -d classes src/ReplicaPlacementSimulationOptimized2.java 2>&1

if [ $? -ne 0 ]; then
    echo "❌ Erreur de compilation"
    exit 1
fi

echo "✅ Compilation réussie"

echo ""
echo "📁 6. Contenu du dossier classes/ :"
ls -la classes/
find classes/ -name "*.class" -type f

echo ""
echo "📁 7. Tentative d'exécution..."
mkdir -p ~/results

echo "   → Depuis classes avec chemin relatif"
cd classes
java -cp "../lib/*:." ReplicaPlacementSimulationOptimized2
RESULT=$?
cd ..

if [ $RESULT -ne 0 ]; then
    echo "   → Tentative 2 : depuis la racine"
    java -cp "lib/*:classes" ReplicaPlacementSimulationOptimized2
    RESULT=$?
fi

if [ $RESULT -eq 0 ]; then
    echo ""
    echo "✅ Simulation terminée avec succès !"
    mkdir -p results
    cp ~/results/*.csv results/ 2>/dev/null
    echo "📁 Résultats copiés dans results/"
else
    echo "❌ Échec de l'exécution"
    exit 1
fi

echo "========================================="
echo "✅ FIN DU SCRIPT"
echo "========================================="
