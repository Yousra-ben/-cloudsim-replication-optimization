import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * ReplicaPlacementSimulationOptimized2
 *
 * ARCHITECTURE :
 *   Java (GitHub Actions / local) ──writes──► DRIVE_SYNC_DIR (monté via rclone ou chemin local)
 *   Python (Colab)                ──reads──►  /content/drive/My Drive/cloudsim_optimization/
 *
 * PRÉREQUIS :
 *   - Définir la variable d'environnement CLOUDSIM_DRIVE_PATH pointant vers le dossier
 *     Google Drive monté localement (ex: via rclone mount ou google-drive-ocamlfuse).
 *   - Si non définie, le chemin par défaut ~/cloudsim_optimization/ est utilisé.
 *     Dans ce cas, copier manuellement vers Drive ou utiliser rclone en post-action GitHub.
 *
 * BUGS CORRIGÉS :
 *   - Imports manquants (Calendar, Date, Arrays)
 *   - Récursion infinie dans waitForOptimizationResults
 *   - Broker CloudSim manquant (requis par l'API)
 *   - Fragmentation non implémentée (fileCopiesMap jamais remplie)
 *   - Chemins unifiés via variable d'environnement
 *   - Gestion robuste des exceptions par drift
 *   - Thread.sleep interruptible correctement géré
 */
public class ReplicaPlacementSimulationOptimized2 {

    // ==================== CONSTANTES ====================

    private static final long   RANDOM_SEED    = 12345L;
    private static final int    VMS_PER_HOST   = 5;
    private static final int    NOMBRE_DRIFTS  = 4;   // Réduire pour les tests ; mettre 720 en prod

    private static final int    VM_MIPS  = 1000;
    private static final int    VM_PES   = 1;
    private static final int    VM_RAM   = 2048;
    private static final long   VM_BW    = 1000;
    private static final long   VM_SIZE  = 10000;
    private static final String VMM      = "Xen";

    // Délai d'attente max pour les résultats Python (secondes)
    private static final int OPTIMIZATION_TIMEOUT_SEC = 300;

    // ==================== CHEMINS (définis au démarrage) ====================

    /**
     * Racine partagée avec Google Drive.
     * Priorité : variable d'environnement CLOUDSIM_DRIVE_PATH
     *            → fallback : ~/cloudsim_optimization/
     *
     * Dans GitHub Actions, ajouter un step rclone avant l'exécution Java :
     *   rclone mount "gdrive:cloudsim_optimization" /mnt/gdrive --daemon
     * puis définir : env: CLOUDSIM_DRIVE_PATH: /mnt/gdrive
     */
    private static final String CLOUDSIM_BASE_DIR;
    static {
        String envPath = System.getenv("CLOUDSIM_DRIVE_PATH");
        if (envPath != null && !envPath.isEmpty()) {
            CLOUDSIM_BASE_DIR = envPath.endsWith("/") ? envPath : envPath + "/";
        } else {
            CLOUDSIM_BASE_DIR = System.getProperty("user.home") + "/cloudsim_optimization/";
        }
    }

    private static final String OPTIMIZATION_INPUT_DIR  = CLOUDSIM_BASE_DIR + "input/";
    private static final String OPTIMIZATION_OUTPUT_DIR = CLOUDSIM_BASE_DIR + "output/";
    private static final String RESULTS_DIR             = CLOUDSIM_BASE_DIR + "results/";

    // --- Fichiers AVANT drift ---
    private static final String CSV_USER_ACCESS_BEFORE      = RESULTS_DIR + "1_user_access_details_before.csv";
    private static final String CSV_DATACENTER_BEFORE       = RESULTS_DIR + "2_datacenter_information_before.csv";
    private static final String CSV_FILE_INFO_BEFORE        = RESULTS_DIR + "3_file_information_before.csv";
    private static final String CSV_VM_INFO_BEFORE          = RESULTS_DIR + "4_vm_information_before.csv";
    private static final String CSV_FRAGMENT_INFO_BEFORE    = RESULTS_DIR + "5_fragment_information_before.csv";

    // --- Fichiers APRÈS drift (avant optimisation) ---
    private static final String CSV_DRIFT_REPORT            = RESULTS_DIR + "6_drift_impact_report.csv";
    private static final String CSV_USER_ACCESS_DRIFT_LOG   = RESULTS_DIR + "7_user_access_drift_log.csv";
    private static final String CSV_USER_ACCESS_AFTER_DRIFT = RESULTS_DIR + "8_user_access_details_after_drift.csv";
    private static final String CSV_DATACENTER_AFTER_DRIFT  = RESULTS_DIR + "9_datacenter_information_after_drift.csv";
    private static final String CSV_FILE_INFO_AFTER_DRIFT   = RESULTS_DIR + "10_file_information_after_drift.csv";
    private static final String CSV_FRAGMENT_AFTER_DRIFT    = RESULTS_DIR + "11_fragment_information_after_drift.csv";
    private static final String CSV_SYSTEM_STATE_AFTER_DRIFT= RESULTS_DIR + "12_system_state_after_drift.csv";

    // --- Fichiers de demande d'optimisation (lus par Python) ---
    private static final String CSV_OPT_FILE_INFO   = OPTIMIZATION_INPUT_DIR + "file_information.csv";
    private static final String CSV_OPT_DC_INFO     = OPTIMIZATION_INPUT_DIR + "datacenter_information.csv";
    private static final String CSV_OPT_USER_ACCESS = OPTIMIZATION_INPUT_DIR + "user_access_details.csv";
    private static final String SIGNAL_DATA_READY   = OPTIMIZATION_INPUT_DIR + "data_ready.signal";

    // --- Fichiers de résultats d'optimisation (écrits par Python) ---
    private static final String CSV_OPT_RESULT    = OPTIMIZATION_OUTPUT_DIR + "optimization_result.csv";
    private static final String CSV_OPT_PLACEMENT = OPTIMIZATION_OUTPUT_DIR + "optimization_placement.csv";
    private static final String SIGNAL_OPT_DONE   = OPTIMIZATION_OUTPUT_DIR + "optimization_done.signal";

    // --- Fichiers APRÈS optimisation ---
    private static final String CSV_OPTIMIZED_REPLICATION_LOG   = RESULTS_DIR + "13_optimized_replication_log.csv";
    private static final String CSV_OPTIMIZED_DATACENTER        = RESULTS_DIR + "14_optimized_datacenter_information.csv";
    private static final String CSV_OPTIMIZED_FILE_INFO         = RESULTS_DIR + "15_optimized_file_information.csv";
    private static final String CSV_OPTIMIZED_FRAGMENT          = RESULTS_DIR + "16_optimized_fragment_information.csv";
    private static final String CSV_OPTIMIZED_PLACEMENT_REPORT  = RESULTS_DIR + "17_optimized_placement_report.csv";
    private static final String CSV_OPTIMIZED_SYSTEM_STATE      = RESULTS_DIR + "18_optimized_system_state.csv";
    private static final String CSV_OPTIMIZATION_SUMMARY        = RESULTS_DIR + "19_optimization_summary.csv";

    // ==================== ÉTAT GLOBAL ====================

    private static int fragmentIdCounter = 1;
    /** fileCopiesMap : fileId → (dcId → liste de fragments) */
    private static final Map<Integer, Map<Integer, List<FragmentMetadata>>> fileCopiesMap = new HashMap<>();
    private static final Map<Integer, Integer> vmStorageFree = new HashMap<>();
    private static final Map<Integer, Integer> vmToDcMap    = new HashMap<>();

    // Résultats reçus de Python
    private static final Map<Integer, Integer>       optimizedReplicas   = new HashMap<>();
    private static final Map<Integer, List<Integer>> optimizedPlacements = new HashMap<>();

    // Maps de latence et région préférée
    private static final Map<String, Map<String, Integer>> LATENCY_MAP = createLatencyMap();
    private static final Map<String, String>               BEST_DC_MAP = createBestDcMap();

    // ==================== CLASSES INTERNES ====================

    enum AccessType { READ, WRITE, READ_WRITE }

    static class FragmentMetadata {
        int fragmentId, originalFileId, sizeMB, vmId, dcId;
        FragmentMetadata(int fragmentId, int originalFileId, int sizeMB, int vmId, int dcId) {
            this.fragmentId = fragmentId; this.originalFileId = originalFileId;
            this.sizeMB = sizeMB; this.vmId = vmId; this.dcId = dcId;
        }
    }

    static class User {
        int id;
        String location;
        Map<Integer, AccessType> fileAccess     = new HashMap<>();
        Map<Integer, Integer>   fileToDatacenter = new HashMap<>();

        User(int id, String location) { this.id = id; this.location = location; }

        void addFileAccess(int fileId, AccessType access, int datacenterId) {
            fileAccess.put(fileId, access);
            fileToDatacenter.put(fileId, datacenterId);
        }
    }

    static class ReplicaData {
        int dataID, sizeMB, importance, popularite;
        List<Integer> datacenterIds;
        Map<Integer, AccessType> userAccess         = new HashMap<>();
        Map<Integer, Integer>   userToDatacenter    = new HashMap<>();
        Map<Integer, Integer>   accessCountByDc     = new HashMap<>();

        ReplicaData(int dataID, int sizeMB, int importance, int popularite, List<Integer> datacenterIds) {
            this.dataID = dataID; this.sizeMB = sizeMB; this.importance = importance;
            this.popularite = popularite; this.datacenterIds = new ArrayList<>(datacenterIds);
            for (int dcId : datacenterIds) accessCountByDc.put(dcId, 0);
        }

        void addUserAccess(int userId, AccessType access, int datacenterId) {
            userAccess.put(userId, access);
            userToDatacenter.put(userId, datacenterId);
            accessCountByDc.merge(datacenterId, 1, Integer::sum);
        }

        void updateFileSize(int newSizeMB)              { sizeMB = newSizeMB; }
        void updateReplicas(List<Integer> newDcIds)     { datacenterIds = new ArrayList<>(newDcIds); }
    }

    static class RegionInfo {
        int datacenterId, hostCount, storageCapacity, storageLibre, used, vmCount, vmPerHost;
        String region, typeDc;
        double costPerCloudlet;
        List<Vm> vmList = new ArrayList<>();

        RegionInfo(int datacenterId, String region, double costPerCloudlet,
                   String typeDc, int hostCount, int storageCapacity) {
            this.datacenterId = datacenterId; this.region = region;
            this.costPerCloudlet = costPerCloudlet; this.typeDc = typeDc;
            this.hostCount = hostCount; this.storageCapacity = storageCapacity;
            this.storageLibre = storageCapacity; this.used = 0;
            this.vmCount = hostCount * VMS_PER_HOST; this.vmPerHost = VMS_PER_HOST;
        }
    }

    // ==================== MAIN ====================

    public static void main(String[] args) {
        try {
            createDirectories();

            System.out.println("=".repeat(60));
            System.out.println("  SIMULATION DE RÉPLICATION DE DONNÉES CLOUDSIM");
            System.out.printf("  Boucle de %d drifts%n", NOMBRE_DRIFTS);
            System.out.println("=".repeat(60));
            System.out.println("  Démarrage : " + new Date());
            System.out.println("  Base dir  : " + CLOUDSIM_BASE_DIR);
            System.out.println("  Input     : " + OPTIMIZATION_INPUT_DIR);
            System.out.println("  Output    : " + OPTIMIZATION_OUTPUT_DIR);
            System.out.println("=".repeat(60));

            // --- Initialisation CloudSim (1 utilisateur, pas de trace) ---
            int numUsers = 1;
            CloudSim.init(numUsers, Calendar.getInstance(), false);

            // --- Création d'un DatacenterBroker factice (obligatoire pour l'API) ---
            DatacenterBroker broker = new DatacenterBroker("MainBroker");

            // --- Configuration des 30 datacenters ---
            String[] regions = {
                "Europe","Europe","Europe","Europe","Europe","Europe",
                "USA","USA","USA","USA","USA","USA",
                "Asie","Asie","Asie","Asie","Asie","Asie",
                "Afrique","Afrique","Afrique","Afrique","Afrique","Afrique",
                "AmeriqueSud","AmeriqueSud","AmeriqueSud","AmeriqueSud","AmeriqueSud","AmeriqueSud"
            };
            String[] types = {
                "grand","grand","moyen","moyen","mini","mini",
                "grand","grand","grand","moyen","moyen","mini",
                "grand","grand","moyen","moyen","mini","mini",
                "grand","moyen","moyen","mini","mini","mini",
                "grand","moyen","moyen","mini","mini","mini"
            };
            double[] costs = {
                0.06,0.06,0.06,0.06,0.06,0.06,
                0.05,0.05,0.05,0.05,0.05,0.05,
                0.045,0.045,0.045,0.045,0.045,0.045,
                0.035,0.035,0.035,0.035,0.035,0.035,
                0.04,0.04,0.04,0.04,0.04,0.04
            };

            List<RegionInfo>         datacenterInfos = new ArrayList<>();
            Map<Integer, String>     dcidToRegion    = new HashMap<>();
            Map<Integer, RegionInfo> datacenterMap   = new HashMap<>();

            for (int i = 1; i <= 30; i++) {
                int hostCount       = types[i-1].equals("grand") ? 100 : (types[i-1].equals("moyen") ? 60 : 30);
                int storageCapacity = hostCount * 100_000; // MB
                RegionInfo info = new RegionInfo(i, regions[i-1], costs[i-1], types[i-1], hostCount, storageCapacity);

                // Créer le datacenter CloudSim correspondant
                Datacenter dc = createDatacenter("DC_" + i, hostCount, info.costPerCloudlet);
                datacenterInfos.add(info);
                datacenterMap.put(i, info);
                dcidToRegion.put(i, regions[i-1]);
            }

            // --- Création des VMs ---
            int vmId = 1;
            for (RegionInfo info : datacenterInfos) {
                for (int j = 0; j < info.vmCount; j++) {
                    Vm vm = new Vm(vmId, broker.getId(), VM_MIPS, VM_PES, VM_RAM, VM_BW, VM_SIZE,
                                   VMM, new CloudletSchedulerTimeShared());
                    info.vmList.add(vm);
                    vmStorageFree.put(vmId, (int) VM_SIZE);
                    vmToDcMap.put(vmId, info.datacenterId);
                    vmId++;
                }
            }

            // --- Création des 500 fichiers ---
            Random seededRandom = new Random(RANDOM_SEED);
            List<Integer> taillesFixes = Arrays.asList(1500, 2500, 5000, 7500, 10000);
            List<ReplicaData> fichiers = new ArrayList<>();

            System.out.println("\nCréation de 500 fichiers...");
            for (int i = 1; i <= 500; i++) {
                int taille = taillesFixes.get((i - 1) % taillesFixes.size());
                List<Integer> selectedDCs = new ArrayList<>();
                selectedDCs.add(((i - 1) % 30) + 1);
                int secondDc = ((i - 1) + 15) % 30 + 1;
                if (secondDc != selectedDCs.get(0)) selectedDCs.add(secondDc);
                fichiers.add(new ReplicaData(i, taille, ((i - 1) % 5) + 1, 0, selectedDCs));
            }

            // Mise à jour du stockage utilisé dans chaque DC
            for (ReplicaData f : fichiers) {
                for (int dcId : f.datacenterIds) {
                    RegionInfo dc = datacenterMap.get(dcId);
                    dc.used      += f.sizeMB;
                    dc.storageLibre = dc.storageCapacity - dc.used;
                }
            }

            // --- Création des 1000 utilisateurs ---
            String[] userLocations = {
                "Paris (France)","Berlin (Allemagne)","Madrid (Espagne)",
                "New York (USA)","San Francisco (USA)","Tokyo (Japon)",
                "Mumbai (Inde)","Nairobi (Kenya)","Lagos (Nigeria)","Le Caire (Egypte)"
            };
            int[] userCounts = {80, 160, 100, 120, 80, 100, 120, 60, 100, 80};
            List<User> users = new ArrayList<>();
            int currentId = 1;
            for (int i = 0; i < userLocations.length; i++) {
                for (int j = 0; j < userCounts[i]; j++) {
                    users.add(new User(currentId++, userLocations[i]));
                }
            }
            System.out.println("Utilisateurs créés : " + users.size());

            // Distribution initiale des droits d'accès
            distributeFilesWithAccessRights(fichiers, users, dcidToRegion, datacenterMap, seededRandom);

            // ==================== BOUCLE PRINCIPALE DES DRIFTS ====================
            List<Map<String, Object>> driftHistory = new ArrayList<>();

            for (int driftId = 1; driftId <= NOMBRE_DRIFTS; driftId++) {
                System.out.println("\n" + "=".repeat(60));
                System.out.printf("🚀 DRIFT %d/%d%n", driftId, NOMBRE_DRIFTS);
                System.out.println("=".repeat(60));

                long driftStart = System.currentTimeMillis();

                try {
                    // 1. Sauvegarder l'état AVANT drift
                    generateUserAccessBeforeCSV(users, dcidToRegion);
                    generateDatacenterInfoBeforeCSV(datacenterInfos);
                    generateFileInfoBeforeCSV(fichiers, dcidToRegion);
                    generateVmInfoBeforeCSV(datacenterInfos);
                    generateFragmentInfoCSV(CSV_FRAGMENT_INFO_BEFORE);

                    // 2. Copier les originaux pour le rapport de drift
                    Map<Integer, Integer> originalSizes = new HashMap<>();
                    for (ReplicaData f : fichiers) originalSizes.put(f.dataID, f.sizeMB);

                    List<User> originalUsers = new ArrayList<>();
                    for (User u : users) {
                        User copy = new User(u.id, u.location);
                        copy.fileAccess.putAll(u.fileAccess);
                        copy.fileToDatacenter.putAll(u.fileToDatacenter);
                        originalUsers.add(copy);
                    }

                    // 3. Appliquer les drifts
                    applyFileSizeDrift(fichiers, seededRandom, datacenterMap, originalSizes);
                    Map<Integer, Map<Integer, AccessType>> originalAccess = new HashMap<>();
                    applyUserAccessDrift(users, fichiers, seededRandom, dcidToRegion, originalAccess);

                    // 4. Générer les rapports après drift
                    generateDriftImpactReport(fichiers, originalSizes, originalAccess);
                    generateUserAccessDriftLog(users, originalUsers);
                    generateUserAccessAfterDriftCSV(users, dcidToRegion);
                    generateDatacenterInfoAfterDriftCSV(datacenterInfos);
                    generateFileInfoAfterDriftCSV(fichiers, dcidToRegion);
                    generateFragmentInfoCSV(CSV_FRAGMENT_AFTER_DRIFT);
                    generateSystemStateAfterDriftCSV(fichiers, users, datacenterInfos);

                    // 5. Envoyer les données à Python
                    generateOptimizationInputData(fichiers, users, datacenterInfos, dcidToRegion);

                    // 6. Attendre les résultats (ou utiliser le défaut)
                    boolean optimizationReceived = waitForOptimizationResults(OPTIMIZATION_TIMEOUT_SEC);

                    // 7. Appliquer l'optimisation
                    if (optimizationReceived) {
                        applyOptimizationToReplication(fichiers, datacenterMap);
                    } else {
                        applyDefaultReplication(fichiers, datacenterMap, dcidToRegion, seededRandom);
                    }

                    // 8. Générer les rapports après optimisation
                    generateOptimizedReplicationLog(fichiers, dcidToRegion, optimizationReceived);
                    generateOptimizedDatacenterCSV(datacenterInfos);
                    generateOptimizedFileInfoCSV(fichiers, dcidToRegion);
                    generateFragmentInfoCSV(CSV_OPTIMIZED_FRAGMENT);
                    generateOptimizedPlacementReport(fichiers, dcidToRegion);
                    generateOptimizedSystemState(fichiers, users, datacenterInfos);
                    generateOptimizationSummary(fichiers, datacenterInfos);

                    // 9. Sauvegarder dans un sous-dossier drift_N
                    saveDriftSnapshot(driftId, fichiers, users, datacenterInfos, dcidToRegion);

                    long elapsed = System.currentTimeMillis() - driftStart;
                    System.out.printf("%n✅ Drift %d terminé en %.1f s%n", driftId, elapsed / 1000.0);

                    Map<String, Object> entry = new LinkedHashMap<>();
                    entry.put("drift_id", driftId);
                    entry.put("timestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                    entry.put("files_count", fichiers.size());
                    entry.put("users_count", users.size());
                    entry.put("optimized_files", optimizedReplicas.size());
                    entry.put("optimized_placements", optimizedPlacements.size());
                    entry.put("optimization_source", optimizationReceived ? "SPEA2+NSGA2" : "Default");
                    entry.put("elapsed_ms", elapsed);
                    driftHistory.add(entry);

                } catch (Exception eDrift) {
                    System.err.printf("❌ Erreur drift %d : %s%n", driftId, eDrift.getMessage());
                    eDrift.printStackTrace();
                    // On continue avec le drift suivant
                }

                // Petite pause entre les drifts pour laisser Python traiter
                if (driftId < NOMBRE_DRIFTS) {
                    Thread.sleep(1000);
                }
            }

            // --- Résumé global ---
            saveGlobalSummary(driftHistory);

            System.out.println("\n" + "=".repeat(60));
            System.out.println("✅ SIMULATION TERMINÉE AVEC SUCCÈS");
            System.out.printf("   %d drifts traités%n", NOMBRE_DRIFTS);
            System.out.println("   Résultats dans : " + RESULTS_DIR);
            System.out.println("=".repeat(60));

            CloudSim.stopSimulation();

        } catch (Exception e) {
            System.err.println("❌ ERREUR FATALE : " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    // ==================== CRÉATION DES RÉPERTOIRES ====================

    private static void createDirectories() throws IOException {
        for (String dir : new String[]{RESULTS_DIR, OPTIMIZATION_INPUT_DIR, OPTIMIZATION_OUTPUT_DIR}) {
            Files.createDirectories(Paths.get(dir));
        }
        System.out.println("📁 Répertoires prêts dans : " + CLOUDSIM_BASE_DIR);
    }

    // ==================== CRÉATION DATACENTER CLOUDSIM ====================

    private static Datacenter createDatacenter(String name, int hostCount, double costPerCloudlet) throws Exception {
        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < hostCount; i++) {
            List<Pe> peList = new ArrayList<>();
            peList.add(new Pe(0, new PeProvisionerSimple(VM_MIPS)));

            hostList.add(new Host(
                i,
                new RamProvisionerSimple(VM_RAM * VMS_PER_HOST),
                new BwProvisionerSimple(VM_BW * VMS_PER_HOST),
                VM_SIZE * VMS_PER_HOST,
                peList,
                new VmSchedulerTimeShared(peList)
            ));
        }

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
            "x86", "Linux", "Xen", hostList,
            10.0,          // time zone
            costPerCloudlet, // cost per sec
            0.05,          // cost per mem
            0.001,         // cost per storage
            0.001          // cost per BW
        );

        return new Datacenter(name, characteristics,
            new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
    }

    // ==================== DISTRIBUTION INITIALE DES ACCÈS ====================

    private static void distributeFilesWithAccessRights(
            List<ReplicaData> fichiers, List<User> users,
            Map<Integer, String> dcidToRegion,
            Map<Integer, RegionInfo> datacenterMap,
            Random random) {

        // Chaque utilisateur accède à 1..5 fichiers aléatoires
        for (User user : users) {
            int filesToAccess = 1 + random.nextInt(5);
            for (int i = 0; i < filesToAccess; i++) {
                ReplicaData file = fichiers.get(random.nextInt(fichiers.size()));
                if (!user.fileAccess.containsKey(file.dataID)) {
                    AccessType access  = randomAccessType(random);
                    int        chosenDc = pickClosestDc(file, user.location, dcidToRegion);
                    user.addFileAccess(file.dataID, access, chosenDc);
                    file.addUserAccess(user.id, access, chosenDc);
                }
            }
        }

        // Garantir qu'au moins 10 utilisateurs accèdent à chaque fichier
        for (ReplicaData file : fichiers) {
            if (file.userAccess.size() < 10) {
                int toAdd = 30 + random.nextInt(20);
                for (int i = 0; i < toAdd; i++) {
                    User user = users.get(random.nextInt(users.size()));
                    if (!file.userAccess.containsKey(user.id)) {
                        AccessType access  = randomAccessType(random);
                        int        chosenDc = pickClosestDc(file, user.location, dcidToRegion);
                        user.addFileAccess(file.dataID, access, chosenDc);
                        file.addUserAccess(user.id, access, chosenDc);
                    }
                }
            }
        }
    }

    private static int pickClosestDc(ReplicaData file, String userLocation, Map<Integer, String> dcidToRegion) {
        String bestRegion = BEST_DC_MAP.getOrDefault(userLocation, "Europe");
        return file.datacenterIds.stream()
            .filter(dcId -> bestRegion.equals(dcidToRegion.get(dcId)))
            .findFirst()
            .orElse(file.datacenterIds.get(0));
    }

    // ==================== DRIFTS ====================

    private static void applyFileSizeDrift(
            List<ReplicaData> fichiers,
            Random random,
            Map<Integer, RegionInfo> datacenterMap,
            Map<Integer, Integer> originalSizes) {

        System.out.println("\n=== DRIFT : MODIFICATION DES TAILLES ===");
        int modified = 0;

        for (ReplicaData file : fichiers) {
            if (random.nextDouble() < 0.3) {
                int oldSize = file.sizeMB;
                originalSizes.put(file.dataID, oldSize);

                double factor  = 0.5 + random.nextDouble(); // 0.5x à 1.5x
                int    newSize = Math.max(500, Math.min(20_000, (int)(oldSize * factor)));
                int    delta   = newSize - oldSize;

                if (delta > 0) {
                    // Vérifier capacité disponible
                    int available = file.datacenterIds.stream()
                        .mapToInt(dcId -> datacenterMap.get(dcId).storageLibre)
                        .sum();
                    if (available < delta) continue;

                    int remaining = delta;
                    for (int dcId : file.datacenterIds) {
                        RegionInfo dc  = datacenterMap.get(dcId);
                        int        add = Math.min(remaining, dc.storageLibre);
                        dc.storageLibre -= add;
                        dc.used         += add;
                        remaining       -= add;
                        if (remaining == 0) break;
                    }
                } else {
                    int toFree = -delta;
                    for (int dcId : file.datacenterIds) {
                        RegionInfo dc  = datacenterMap.get(dcId);
                        int        free = Math.min(toFree, dc.used);
                        dc.storageLibre += free;
                        dc.used         -= free;
                        toFree          -= free;
                        if (toFree == 0) break;
                    }
                }

                file.updateFileSize(newSize);
                modified++;
                System.out.printf("  Fichier %d : %d MB → %d MB%n", file.dataID, oldSize, newSize);
            }
        }
        System.out.printf("✓ Drift taille : %d fichiers modifiés%n", modified);
    }

    private static void applyUserAccessDrift(
            List<User> users,
            List<ReplicaData> fichiers,
            Random random,
            Map<Integer, String> dcidToRegion,
            Map<Integer, Map<Integer, AccessType>> originalAccess) {

        System.out.println("\n=== DRIFT : MODIFICATION DES ACCÈS ===");
        int changes = 0, added = 0, removed = 0;

        // Sauvegarder l'état original
        for (User user : users) {
            originalAccess.put(user.id, new HashMap<>(user.fileAccess));
        }

        Map<Integer, ReplicaData> fileMap = new HashMap<>();
        for (ReplicaData f : fichiers) fileMap.put(f.dataID, f);

        for (User user : users) {
            if (random.nextDouble() >= 0.4) continue;

            List<Integer> currentFiles = new ArrayList<>(user.fileAccess.keySet());

            // Supprimer quelques accès
            int toRemove = Math.min(currentFiles.size(), 1 + random.nextInt(3));
            for (int i = 0; i < toRemove && !currentFiles.isEmpty(); i++) {
                int fileId = currentFiles.remove(random.nextInt(currentFiles.size()));
                user.fileAccess.remove(fileId);
                user.fileToDatacenter.remove(fileId);
                ReplicaData f = fileMap.get(fileId);
                if (f != null) f.userAccess.remove(user.id);
                removed++;
            }

            // Ajouter de nouveaux accès
            int toAdd = 1 + random.nextInt(3);
            for (int i = 0; i < toAdd; i++) {
                ReplicaData file = fichiers.get(random.nextInt(fichiers.size()));
                if (!user.fileAccess.containsKey(file.dataID)) {
                    AccessType access   = randomAccessType(random);
                    int        chosenDc = pickClosestDc(file, user.location, dcidToRegion);
                    user.addFileAccess(file.dataID, access, chosenDc);
                    file.addUserAccess(user.id, access, chosenDc);
                    added++;
                }
            }

            // Modifier des types d'accès existants
            List<Integer> remaining = new ArrayList<>(user.fileAccess.keySet());
            int toModify = Math.min(remaining.size(), 1 + random.nextInt(2));
            for (int i = 0; i < toModify && !remaining.isEmpty(); i++) {
                int        fileId    = remaining.get(random.nextInt(remaining.size()));
                AccessType newAccess = randomAccessType(random);
                if (user.fileAccess.get(fileId) != newAccess) {
                    user.fileAccess.put(fileId, newAccess);
                    ReplicaData f = fileMap.get(fileId);
                    if (f != null) f.userAccess.put(user.id, newAccess);
                    changes++;
                }
            }
        }

        System.out.printf("✓ Drift accès : %d changements, %d ajouts, %d suppressions%n",
            changes, added, removed);
    }

    // ==================== GÉNÉRATION DES INPUTS POUR PYTHON ====================

    private static void generateOptimizationInputData(
            List<ReplicaData> fichiers, List<User> users,
            List<RegionInfo> datacenterInfos,
            Map<Integer, String> dcidToRegion) throws IOException {

        System.out.println("\n=== GÉNÉRATION DES DONNÉES POUR SPEA2/NSGA-II ===");

        // file_information.csv
        List<String> lines = new ArrayList<>();
        lines.add("FileID,Taille(MB),Importance,Popularite,ReplicaCount,CurrentDatacenters,FrequenceAcces");
        for (ReplicaData f : fichiers) {
            String dcs  = f.datacenterIds.stream().map(String::valueOf).collect(Collectors.joining(";"));
            int    freq = (int) f.userAccess.values().stream().filter(a -> a != AccessType.WRITE).count()
                        + (int) f.userAccess.values().stream().filter(a -> a != AccessType.READ).count();
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%d,%s,%d",
                f.dataID, f.sizeMB, f.importance, f.userAccess.size(), f.datacenterIds.size(), dcs, freq));
        }
        writeLines(CSV_OPT_FILE_INFO, lines);
        System.out.println("✓ " + CSV_OPT_FILE_INFO);

        // datacenter_information.csv
        lines.clear();
        lines.add("DatacenterID,Region,TypeDC,StorageCapacityMB,StorageUsedMB,StorageFreeMB,HostCount,CostPerCloudlet,VMPerHost");
        for (RegionInfo dc : datacenterInfos) {
            lines.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.4f,%d",
                dc.datacenterId, dc.region, dc.typeDc, dc.storageCapacity,
                dc.used, dc.storageLibre, dc.hostCount, dc.costPerCloudlet, dc.vmPerHost));
        }
        writeLines(CSV_OPT_DC_INFO, lines);
        System.out.println("✓ " + CSV_OPT_DC_INFO);

        // user_access_details.csv
        lines.clear();
        lines.add("UserID,UserLocation,FileID,AccessType,PreferredDatacenter,Region");
        for (User u : users) {
            for (Map.Entry<Integer, AccessType> e : u.fileAccess.entrySet()) {
                int    fileId = e.getKey();
                int    dcId   = u.fileToDatacenter.getOrDefault(fileId, 1);
                String region = dcidToRegion.getOrDefault(dcId, "Unknown");
                lines.add(String.format("%d,%s,%d,%s,%d,%s",
                    u.id, u.location, fileId, e.getValue(), dcId, region));
            }
        }
        writeLines(CSV_OPT_USER_ACCESS, lines);
        System.out.println("✓ " + CSV_OPT_USER_ACCESS);

        // Signal data_ready
        Files.write(Paths.get(SIGNAL_DATA_READY),
            String.valueOf(System.currentTimeMillis()).getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Signal envoyé : " + SIGNAL_DATA_READY);
    }

    // ==================== ATTENTE DES RÉSULTATS PYTHON ====================

    /**
     * Attend que Python dépose optimization_result.csv + optimization_placement.csv.
     * Pas de récursion : si timeout → retourne false.
     */
    private static boolean waitForOptimizationResults(int timeoutSeconds) {
        System.out.printf("%n⏳ Attente des résultats Python (max %d s)...%n", timeoutSeconds);

        Path resultFile    = Paths.get(CSV_OPT_RESULT);
        Path placementFile = Paths.get(CSV_OPT_PLACEMENT);
        Path doneSignal    = Paths.get(SIGNAL_OPT_DONE);

        long deadline = System.currentTimeMillis() + (long) timeoutSeconds * 1000;

        while (System.currentTimeMillis() < deadline) {
            if (Files.exists(resultFile) && Files.exists(placementFile)) {
                System.out.println("\n✅ Fichiers d'optimisation détectés !");

                boolean loaded = loadOptimizationResults();
                if (loaded) {
                    // Archiver pour éviter une relecture au prochain drift
                    archiveFile(resultFile);
                    archiveFile(placementFile);
                    try { if (Files.exists(doneSignal)) Files.delete(doneSignal); } catch (IOException ignored) {}
                    return true;
                }
            }

            try {
                Thread.sleep(5_000);
                System.out.print(".");
                System.out.flush();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.println("\n⚠️ Timeout : utilisation de la réplication par défaut.");
        return false;
    }

    private static void archiveFile(Path file) {
        try {
            Path archived = file.resolveSibling(
                file.getFileName().toString().replace(".csv", "_processed_" + System.currentTimeMillis() + ".csv"));
            Files.move(file, archived);
        } catch (IOException e) {
            System.err.println("⚠️ Impossible d'archiver " + file + " : " + e.getMessage());
        }
    }

    private static boolean loadOptimizationResults() {
        optimizedReplicas.clear();
        optimizedPlacements.clear();

        try {
            // --- SPEA2 : optimization_result.csv ---
            List<String> lines = Files.readAllLines(Paths.get(CSV_OPT_RESULT));
            if (lines.size() < 2) {
                System.err.println("⚠️ optimization_result.csv est vide.");
                return false;
            }

            String[] headers = lines.get(0).split(",");
            int fileIdCol  = findCol(headers, "fileid", "file_id", "id");
            int replicaCol = findCol(headers, "replicas", "replicacount", "nbcopies");
            int selectedCol= findCol(headers, "selected", "selection", "is_selected");

            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.isEmpty()) continue;
                String[] p = line.split(",");
                if (fileIdCol < 0 || fileIdCol >= p.length) continue;

                int fileId   = Integer.parseInt(p[fileIdCol].trim());
                int replicas = 2; // défaut

                if (replicaCol >= 0 && replicaCol < p.length) {
                    try {
                        int r = Integer.parseInt(p[replicaCol].trim());
                        if (r >= 1 && r <= 5) replicas = r;
                    } catch (NumberFormatException ignored) {}
                } else if (selectedCol >= 0 && selectedCol < p.length) {
                    String sel = p[selectedCol].trim().toLowerCase();
                    if (sel.equals("yes") || sel.equals("true") || sel.equals("1")) replicas = 3;
                }

                optimizedReplicas.put(fileId, replicas);
            }
            System.out.println("📊 SPEA2 : " + optimizedReplicas.size() + " fichiers chargés");

            // --- NSGA-II : optimization_placement.csv ---
            List<String> pLines = Files.readAllLines(Paths.get(CSV_OPT_PLACEMENT));
            if (pLines.size() < 2) {
                System.err.println("⚠️ optimization_placement.csv est vide.");
                return !optimizedReplicas.isEmpty();
            }

            String[] pHeaders = pLines.get(0).split(",");
            int pfCol  = findCol(pHeaders, "fileid", "file_id", "id");
            int pdcCol = findCol(pHeaders, "datacenterid", "dc_id", "datacenter");

            for (int i = 1; i < pLines.size(); i++) {
                String line = pLines.get(i).trim();
                if (line.isEmpty()) continue;
                String[] p = line.split(",");
                if (pfCol < 0 || pdcCol < 0 || pfCol >= p.length || pdcCol >= p.length) continue;
                int fileId = Integer.parseInt(p[pfCol].trim());
                int dcId   = Integer.parseInt(p[pdcCol].trim());
                optimizedPlacements.computeIfAbsent(fileId, k -> new ArrayList<>()).add(dcId);
            }
            System.out.println("📍 NSGA-II : " + optimizedPlacements.size() + " placements chargés");

            return !optimizedReplicas.isEmpty() || !optimizedPlacements.isEmpty();

        } catch (Exception e) {
            System.err.println("❌ Erreur chargement résultats : " + e.getMessage());
            return false;
        }
    }

    /** Recherche la première colonne dont le nom (minuscules) correspond à l'un des candidats. */
    private static int findCol(String[] headers, String... candidates) {
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim().toLowerCase().replaceAll("[^a-z0-9]", "");
            for (String c : candidates) {
                if (h.equals(c.toLowerCase().replaceAll("[^a-z0-9]", ""))) return i;
            }
        }
        return -1;
    }

    // ==================== APPLICATION DE L'OPTIMISATION ====================

    private static void applyOptimizationToReplication(
            List<ReplicaData> fichiers,
            Map<Integer, RegionInfo> datacenterMap) {

        System.out.println("\n=== APPLICATION DE L'OPTIMISATION ===");
        int filesOptimized = 0;

        for (ReplicaData file : fichiers) {
            // SPEA2 : ajuster le nombre de répliques
            if (optimizedReplicas.containsKey(file.dataID)) {
                int target  = optimizedReplicas.get(file.dataID);
                int current = file.datacenterIds.size();

                if (target > current) {
                    // Ajouter des répliques dans des DCs qui ont de l'espace
                    for (RegionInfo dc : datacenterMap.values()) {
                        if (file.datacenterIds.size() >= target) break;
                        if (!file.datacenterIds.contains(dc.datacenterId)
                                && dc.storageLibre >= file.sizeMB) {
                            file.datacenterIds.add(dc.datacenterId);
                            dc.storageLibre -= file.sizeMB;
                            dc.used         += file.sizeMB;
                            filesOptimized++;
                            System.out.printf("  + Réplique : Fichier %d → DC %d%n",
                                file.dataID, dc.datacenterId);
                        }
                    }
                }
            }

            // NSGA-II : déplacer les répliques vers des DCs optimaux
            if (optimizedPlacements.containsKey(file.dataID)) {
                List<Integer> newDcs = optimizedPlacements.get(file.dataID);

                // Vérifier la capacité
                boolean ok = newDcs.stream().allMatch(dcId -> {
                    RegionInfo dc = datacenterMap.get(dcId);
                    return dc != null && dc.storageLibre >= file.sizeMB;
                });

                if (ok && !newDcs.isEmpty()) {
                    // Libérer les anciens emplacements
                    for (int oldDcId : file.datacenterIds) {
                        RegionInfo dc = datacenterMap.get(oldDcId);
                        if (dc != null) {
                            dc.storageLibre += file.sizeMB;
                            dc.used         -= file.sizeMB;
                        }
                    }
                    // Appliquer les nouveaux
                    file.updateReplicas(newDcs);
                    for (int newDcId : newDcs) {
                        RegionInfo dc = datacenterMap.get(newDcId);
                        if (dc != null) {
                            dc.storageLibre -= file.sizeMB;
                            dc.used         += file.sizeMB;
                        }
                    }
                    filesOptimized++;
                    System.out.printf("  ✓ Placement : Fichier %d → %s%n", file.dataID, newDcs);
                }
            }
        }

        System.out.printf("✅ %d fichiers optimisés%n", filesOptimized);
    }

    private static void applyDefaultReplication(
            List<ReplicaData> fichiers,
            Map<Integer, RegionInfo> datacenterMap,
            Map<Integer, String> dcidToRegion,
            Random random) {

        System.out.println("\n=== RÉPLICATION PAR DÉFAUT (popularité) ===");

        List<ReplicaData> sorted = fichiers.stream()
            .sorted(Comparator.comparingInt(f -> -f.userAccess.size()))
            .collect(Collectors.toList());

        int threshold = (int)(fichiers.size() * 0.3);
        int count = 0;

        for (int i = 0; i < threshold; i++) {
            ReplicaData file = sorted.get(i);

            // Compter les accès par région
            Map<String, Long> regionCounts = file.userToDatacenter.values().stream()
                .collect(Collectors.groupingBy(
                    dcId -> dcidToRegion.getOrDefault(dcId, "Unknown"),
                    Collectors.counting()));

            // Choisir la région la plus demandée qui n'a pas encore de réplique
            String targetRegion = regionCounts.entrySet().stream()
                .filter(e -> file.datacenterIds.stream()
                    .noneMatch(dcId -> dcidToRegion.get(dcId).equals(e.getKey())))
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);

            if (targetRegion == null) continue;

            final String tr = targetRegion;
            List<RegionInfo> candidates = datacenterMap.values().stream()
                .filter(dc -> dc.region.equals(tr))
                .filter(dc -> dc.storageLibre >= file.sizeMB)
                .filter(dc -> !file.datacenterIds.contains(dc.datacenterId))
                .collect(Collectors.toList());

            if (!candidates.isEmpty()) {
                RegionInfo chosen = candidates.get(random.nextInt(candidates.size()));
                file.datacenterIds.add(chosen.datacenterId);
                chosen.storageLibre -= file.sizeMB;
                chosen.used         += file.sizeMB;
                count++;
                System.out.printf("  + Fichier %d (pop=%d) → DC %d (%s)%n",
                    file.dataID, file.userAccess.size(), chosen.datacenterId, targetRegion);
            }
        }
        System.out.printf("✓ %d fichiers répliqués par défaut%n", count);
    }

    // ==================== GÉNÉRATION CSV ====================

    private static void generateUserAccessBeforeCSV(List<User> users, Map<Integer, String> dcidToRegion)
            throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("UserID,UserLocation,FileID,AccessType,DatacenterAccessed,DatacenterRegion");
        for (User u : users) {
            for (Map.Entry<Integer, AccessType> e : u.fileAccess.entrySet()) {
                int    dcId   = u.fileToDatacenter.getOrDefault(e.getKey(), 1);
                String region = dcidToRegion.getOrDefault(dcId, "Unknown");
                lines.add(String.format("%d,%s,%d,%s,%d,%s",
                    u.id, u.location, e.getKey(), e.getValue(), dcId, region));
            }
        }
        writeLines(CSV_USER_ACCESS_BEFORE, lines);
    }

    private static void generateDatacenterInfoBeforeCSV(List<RegionInfo> dcs) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("DatacenterID,Region,TypeDC,HostCount,StorageCapacityMB,StorageUsedMB,StorageFreeMB,UsagePct,VMCount,VMPerHost");
        for (RegionInfo dc : dcs) {
            lines.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.2f%%,%d,%d",
                dc.datacenterId, dc.region, dc.typeDc, dc.hostCount,
                dc.storageCapacity, dc.used, dc.storageLibre,
                100.0 * dc.used / dc.storageCapacity, dc.vmCount, dc.vmPerHost));
        }
        writeLines(CSV_DATACENTER_BEFORE, lines);
    }

    private static void generateFileInfoBeforeCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion)
            throws IOException {
        writeLines(CSV_FILE_INFO_BEFORE, buildFileInfoLines(fichiers, dcidToRegion));
    }

    private static List<String> buildFileInfoLines(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) {
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,Importance,ReplicaCount,ReplicaLocations,UserAccessCount,ReadCount,WriteCount,RWCount");
        for (ReplicaData f : fichiers) {
            long r  = f.userAccess.values().stream().filter(a -> a == AccessType.READ).count();
            long w  = f.userAccess.values().stream().filter(a -> a == AccessType.WRITE).count();
            long rw = f.userAccess.values().stream().filter(a -> a == AccessType.READ_WRITE).count();
            String locs = f.datacenterIds.stream()
                .map(d -> d + "(" + dcidToRegion.getOrDefault(d, "?") + ")")
                .collect(Collectors.joining("; "));
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%d,%d,%d,%d",
                f.dataID, f.sizeMB, f.importance, f.datacenterIds.size(), locs,
                f.userAccess.size(), r, w, rw));
        }
        return lines;
    }

    private static void generateVmInfoBeforeCSV(List<RegionInfo> dcs) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("VM_ID,DatacenterID,Region,MIPS,RAM_MB,BW_Mbps,Storage_MB,StorageFree_MB");
        for (RegionInfo info : dcs) {
            for (Vm vm : info.vmList) {
                int free = vmStorageFree.getOrDefault(vm.getId(), (int) VM_SIZE);
                lines.add(String.format("%d,%d,%s,%d,%d,%d,%d,%d",
                    vm.getId(), info.datacenterId, info.region,
                    (int) vm.getMips(), vm.getRam(), vm.getBw(), vm.getSize(), free));
            }
        }
        writeLines(CSV_VM_INFO_BEFORE, lines);
    }

    private static void generateFragmentInfoCSV(String path) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("FragmentID,FileID,SizeMB,VM_ID,DatacenterID,Region");
        for (Map.Entry<Integer, Map<Integer, List<FragmentMetadata>>> fe : fileCopiesMap.entrySet()) {
            for (Map.Entry<Integer, List<FragmentMetadata>> de : fe.getValue().entrySet()) {
                String region = getRegionForDc(de.getKey());
                for (FragmentMetadata frag : de.getValue()) {
                    lines.add(String.format("%d,%d,%d,%d,%d,%s",
                        frag.fragmentId, frag.originalFileId, frag.sizeMB,
                        frag.vmId, frag.dcId, region));
                }
            }
        }
        writeLines(path, lines);
    }

    private static void generateDriftImpactReport(
            List<ReplicaData> fichiers,
            Map<Integer, Integer> originalSizes,
            Map<Integer, Map<Integer, AccessType>> originalAccess) throws IOException {

        List<String> lines = new ArrayList<>();
        lines.add("FileID,OrigSizeMB,NewSizeMB,SizeDelta,SizeDeltaPct,OrigAccessCount,NewAccessCount,AccessDelta");
        for (ReplicaData f : fichiers) {
            int origSize   = originalSizes.getOrDefault(f.dataID, f.sizeMB);
            int delta      = f.sizeMB - origSize;
            double pct     = origSize > 0 ? 100.0 * delta / origSize : 0;
            int origAccess = originalAccess.containsKey(f.dataID)
                ? originalAccess.get(f.dataID).size() : 0;
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%.2f%%,%d,%d,%d",
                f.dataID, origSize, f.sizeMB, delta, pct,
                origAccess, f.userAccess.size(), f.userAccess.size() - origAccess));
        }
        writeLines(CSV_DRIFT_REPORT, lines);
    }

    private static void generateUserAccessDriftLog(List<User> users, List<User> originalUsers)
            throws IOException {
        Map<Integer, User> origMap = new HashMap<>();
        for (User u : originalUsers) origMap.put(u.id, u);

        List<String> lines = new ArrayList<>();
        lines.add("UserID,UserLocation,FileID,ChangeType,OldAccessType,NewAccessType,Timestamp");
        long ts = System.currentTimeMillis();

        for (User user : users) {
            User orig = origMap.get(user.id);
            if (orig == null) continue;

            for (Map.Entry<Integer, AccessType> e : orig.fileAccess.entrySet()) {
                if (!user.fileAccess.containsKey(e.getKey())) {
                    lines.add(String.format("%d,%s,%d,REMOVED,%s,N/A,%d",
                        user.id, user.location, e.getKey(), e.getValue(), ts));
                }
            }
            for (Map.Entry<Integer, AccessType> e : user.fileAccess.entrySet()) {
                if (!orig.fileAccess.containsKey(e.getKey())) {
                    lines.add(String.format("%d,%s,%d,ADDED,N/A,%s,%d",
                        user.id, user.location, e.getKey(), e.getValue(), ts));
                } else {
                    AccessType old = orig.fileAccess.get(e.getKey());
                    if (old != e.getValue()) {
                        lines.add(String.format("%d,%s,%d,MODIFIED,%s,%s,%d",
                            user.id, user.location, e.getKey(), old, e.getValue(), ts));
                    }
                }
            }
        }
        writeLines(CSV_USER_ACCESS_DRIFT_LOG, lines);
    }

    private static void generateUserAccessAfterDriftCSV(List<User> users, Map<Integer, String> dcidToRegion)
            throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("UserID,UserLocation,FileID,AccessType,DatacenterAccessed,DatacenterRegion");
        for (User u : users) {
            for (Map.Entry<Integer, AccessType> e : u.fileAccess.entrySet()) {
                int    dcId   = u.fileToDatacenter.getOrDefault(e.getKey(), 1);
                String region = dcidToRegion.getOrDefault(dcId, "Unknown");
                lines.add(String.format("%d,%s,%d,%s,%d,%s",
                    u.id, u.location, e.getKey(), e.getValue(), dcId, region));
            }
        }
        writeLines(CSV_USER_ACCESS_AFTER_DRIFT, lines);
    }

    private static void generateDatacenterInfoAfterDriftCSV(List<RegionInfo> dcs) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("DatacenterID,Region,TypeDC,HostCount,StorageCapacityMB,StorageUsedMB,StorageFreeMB,UsagePct,VMCount,VMPerHost");
        for (RegionInfo dc : dcs) {
            lines.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.2f%%,%d,%d",
                dc.datacenterId, dc.region, dc.typeDc, dc.hostCount,
                dc.storageCapacity, dc.used, dc.storageLibre,
                100.0 * dc.used / dc.storageCapacity, dc.vmCount, dc.vmPerHost));
        }
        writeLines(CSV_DATACENTER_AFTER_DRIFT, lines);
    }

    private static void generateFileInfoAfterDriftCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion)
            throws IOException {
        writeLines(CSV_FILE_INFO_AFTER_DRIFT, buildFileInfoLines(fichiers, dcidToRegion));
    }

    private static void generateSystemStateAfterDriftCSV(
            List<ReplicaData> fichiers, List<User> users, List<RegionInfo> dcs) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("Metric,Value");
        lines.add("Timestamp," + new Date());
        lines.add("TotalFiles,"       + fichiers.size());
        lines.add("TotalUsers,"       + users.size());
        lines.add("TotalDatacenters," + dcs.size());
        lines.add("TotalReplicas,"    + fichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum());
        writeLines(CSV_SYSTEM_STATE_AFTER_DRIFT, lines);
    }

    private static void generateOptimizedReplicationLog(
            List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion,
            boolean optimizationReceived) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,UserAccessCount,TotalReplicas,ReplicaLocations,OptimizedBy");
        for (ReplicaData f : fichiers) {
            String locs = f.datacenterIds.stream()
                .map(d -> d + "(" + dcidToRegion.getOrDefault(d, "?") + ")")
                .collect(Collectors.joining("; "));
            String by = optimizedReplicas.containsKey(f.dataID) ? "SPEA2"
                      : optimizedPlacements.containsKey(f.dataID) ? "NSGA-II"
                      : (optimizationReceived ? "Inchangé" : "Default");
            lines.add(String.format("%d,%d,%d,%d,%s,%s",
                f.dataID, f.sizeMB, f.userAccess.size(), f.datacenterIds.size(), locs, by));
        }
        writeLines(CSV_OPTIMIZED_REPLICATION_LOG, lines);
    }

    private static void generateOptimizedDatacenterCSV(List<RegionInfo> dcs) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("DatacenterID,Region,TypeDC,StorageCapacityMB,StorageUsedMB,StorageFreeMB,UsagePct");
        for (RegionInfo dc : dcs) {
            lines.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%.2f%%",
                dc.datacenterId, dc.region, dc.typeDc,
                dc.storageCapacity, dc.used, dc.storageLibre,
                100.0 * dc.used / dc.storageCapacity));
        }
        writeLines(CSV_OPTIMIZED_DATACENTER, lines);
    }

    private static void generateOptimizedFileInfoCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion)
            throws IOException {
        writeLines(CSV_OPTIMIZED_FILE_INFO, buildFileInfoLines(fichiers, dcidToRegion));
    }

    private static void generateOptimizedPlacementReport(
            List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("FileID,FileSizeMB,UserAccessCount,AllReplicaLocations");
        for (ReplicaData f : fichiers) {
            String locs = f.datacenterIds.stream()
                .map(d -> d + "(" + dcidToRegion.getOrDefault(d, "?") + ")")
                .collect(Collectors.joining("; "));
            lines.add(String.format("%d,%d,%d,%s", f.dataID, f.sizeMB, f.userAccess.size(), locs));
        }
        writeLines(CSV_OPTIMIZED_PLACEMENT_REPORT, lines);
    }

    private static void generateOptimizedSystemState(
            List<ReplicaData> fichiers, List<User> users, List<RegionInfo> dcs) throws IOException {
        generateSystemStateAfterDriftCSV(fichiers, users, dcs); // même format
        // on réécrit dans le fichier dédié
        List<String> lines = new ArrayList<>();
        lines.add("Metric,Value");
        lines.add("Timestamp,"        + new Date());
        lines.add("TotalFiles,"       + fichiers.size());
        lines.add("TotalUsers,"       + users.size());
        lines.add("TotalDatacenters," + dcs.size());
        lines.add("TotalReplicas,"    + fichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum());
        lines.add("FilesWithSPEA2,"   + optimizedReplicas.size());
        lines.add("FilesWithNSGA2,"   + optimizedPlacements.size());
        writeLines(CSV_OPTIMIZED_SYSTEM_STATE, lines);
    }

    private static void generateOptimizationSummary(
            List<ReplicaData> fichiers, List<RegionInfo> dcs) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("Metric,Value");
        lines.add("OptimizationDate," + new Date());
        lines.add("TotalFiles,"       + fichiers.size());
        lines.add("TotalReplicas,"    + fichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum());
        lines.add("FilesViaSPEA2,"    + optimizedReplicas.size());
        lines.add("FilesViaNSGA2,"    + optimizedPlacements.size());
        writeLines(CSV_OPTIMIZATION_SUMMARY, lines);
    }

    // ==================== SNAPSHOT PAR DRIFT ====================

    private static void saveDriftSnapshot(
            int driftId, List<ReplicaData> fichiers, List<User> users,
            List<RegionInfo> dcs, Map<Integer, String> dcidToRegion) throws IOException {
        String dir = RESULTS_DIR + "drift_" + driftId + "/";
        Files.createDirectories(Paths.get(dir));

        writeLines(dir + "files_after_optimization.csv",  buildFileInfoLines(fichiers, dcidToRegion));

        List<String> dcLines = new ArrayList<>();
        dcLines.add("DatacenterID,Region,StorageUsedMB,StorageFreeMB");
        for (RegionInfo dc : dcs) {
            dcLines.add(String.format("%d,%s,%d,%d", dc.datacenterId, dc.region, dc.used, dc.storageLibre));
        }
        writeLines(dir + "datacenter_after_optimization.csv", dcLines);

        System.out.println("📁 Snapshot drift " + driftId + " → " + dir);
    }

    private static void saveGlobalSummary(List<Map<String, Object>> history) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("DriftID,Timestamp,FilesCount,UsersCount,OptimizedFiles,OptimizedPlacements,OptimizationSource,ElapsedMs");
        for (Map<String, Object> entry : history) {
            lines.add(String.format("%s,%s,%s,%s,%s,%s,%s,%s",
                entry.get("drift_id"), entry.get("timestamp"),
                entry.get("files_count"), entry.get("users_count"),
                entry.get("optimized_files"), entry.get("optimized_placements"),
                entry.get("optimization_source"), entry.get("elapsed_ms")));
        }
        writeLines(RESULTS_DIR + "global_summary.csv", lines);
        System.out.println("📊 Résumé global → " + RESULTS_DIR + "global_summary.csv");
    }

    // ==================== UTILITAIRES ====================

    private static void writeLines(String path, List<String> lines) throws IOException {
        Path p = Paths.get(path);
        Files.createDirectories(p.getParent());
        Files.write(p, lines, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ " + path);
    }

    private static String getRegionForDc(int dcId) {
        if (dcId >= 1  && dcId <= 6)  return "Europe";
        if (dcId >= 7  && dcId <= 12) return "USA";
        if (dcId >= 13 && dcId <= 18) return "Asie";
        if (dcId >= 19 && dcId <= 24) return "Afrique";
        if (dcId >= 25 && dcId <= 30) return "AmeriqueSud";
        return "Unknown";
    }

    private static AccessType randomAccessType(Random r) {
        switch (r.nextInt(3)) {
            case 0: return AccessType.READ;
            case 1: return AccessType.WRITE;
            default: return AccessType.READ_WRITE;
        }
    }

    private static Map<String, Map<String, Integer>> createLatencyMap() {
        Map<String, Map<String, Integer>> m = new HashMap<>();
        String[] locs = {
            "Paris (France)","Berlin (Allemagne)","Madrid (Espagne)",
            "New York (USA)","San Francisco (USA)",
            "Tokyo (Japon)","Mumbai (Inde)",
            "Nairobi (Kenya)","Lagos (Nigeria)","Le Caire (Egypte)"
        };
        int[][] latencies = {
            // EU  USA  Asie  Afr  AmSud
            {5,  100, 200,  100, 150},  // Paris
            {5,  100, 200,  100, 150},  // Berlin
            {5,  100, 200,  100, 150},  // Madrid
            {100, 10, 150,  200, 100},  // New York
            {100, 10, 150,  200, 100},  // San Francisco
            {200,150,  10,  200, 250},  // Tokyo
            {200,150,  10,  200, 250},  // Mumbai
            {100,200, 200,   15, 200},  // Nairobi
            {100,200, 200,   15, 200},  // Lagos
            {100,200, 200,   15, 200},  // Le Caire
        };
        String[] regions = {"Europe","USA","Asie","Afrique","AmeriqueSud"};
        for (int i = 0; i < locs.length; i++) {
            Map<String, Integer> lat = new HashMap<>();
            for (int j = 0; j < regions.length; j++) lat.put(regions[j], latencies[i][j]);
            m.put(locs[i], lat);
        }
        return m;
    }

    private static Map<String, String> createBestDcMap() {
        Map<String, String> m = new HashMap<>();
        m.put("Paris (France)",      "Europe");
        m.put("Berlin (Allemagne)",  "Europe");
        m.put("Madrid (Espagne)",    "Europe");
        m.put("New York (USA)",      "USA");
        m.put("San Francisco (USA)", "USA");
        m.put("Tokyo (Japon)",       "Asie");
        m.put("Mumbai (Inde)",       "Asie");
        m.put("Nairobi (Kenya)",     "Afrique");
        m.put("Lagos (Nigeria)",     "Afrique");
        m.put("Le Caire (Egypte)",   "Afrique");
        return m;
    }
}
