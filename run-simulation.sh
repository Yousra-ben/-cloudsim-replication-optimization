#!/bin/bash
echo "Test simple..."

# Nettoyer
rm -rf classes
mkdir -p classes

# Compiler le test
javac -cp "lib/*" -d classes src/Test.java

# Exécuter
java -cp "lib/*:classes" Test#!/bin/bash
echo "🚀 Lancement de la simulation CloudSim..."

# Afficher le contenu des dossiers pour debug
echo "📁 Contenu du dossier src :"
ls -la src/

echo "📁 Contenu du dossier classes :"
ls -la classes/ 2>/dev/null || echo "  (classes pas encore créé)"

# Nettoyer et recompiler
rm -rf classes
mkdir -p classes

echo "📦 Compilation..."
javac -cp "lib/*" -d classes src/ReplicaPlacementSimulationOptimized2.java

if [ $? -ne 0 ]; then
    echo "❌ Erreur de compilation"
    exit 1
fi

echo "✅ Compilation réussie"

# Afficher les classes compilées
echo "📁 Classes compilées :"
ls -la classes/

# Créer le dossier résultats
mkdir -p ~/results

# Exécuter - essayer différentes syntaxes
echo "🏃 Exécution..."

# Syntaxe 1 : depuis le dossier classes
cd classes
java -cp "../lib/*:." ReplicaPlacementSimulationOptimized2
cd ..

# Si la syntaxe 1 échoue, essayez le retour
if [ $? -ne 0 ]; then
    echo "⚠️ Tentative avec syntaxe alternative..."
    java -cp "lib/*:classes" ReplicaPlacementSimulationOptimized2
fi

if [ $? -eq 0 ]; then
    echo "✅ Simulation terminée !"
    mkdir -p results
    cp ~/results/*.csv results/ 2>/dev/null
    echo "📁 Résultats sauvegardés"
else
    echo "❌ Erreur lors de l'exécution"
    exit 1
fi
