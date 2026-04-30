import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public class ReplicaPlacementSimulationOptimized2 {
    private static final long RANDOM_SEED = 12345L;
    private static final int VMS_PER_HOST = 5;
    private static final Map<String, Map<String, Integer>> LATENCY_MAP = createLatencyMap();
    private static final Map<String, String> BEST_DC_MAP = createBestDcMap();
   
    private static final int VM_MIPS = 1000;
    private static final int VM_PES = 1;
    private static final int VM_RAM = 2048;
    private static final long VM_BW = 1000;
    private static final long VM_SIZE = 10000;
    private static final String VMM = "Xen";
   
    // Configuration des drifts
    private static final int NOMBRE_DRIFTS = 720;  // 24 scénarios × 30 exécutions
    
    // Chemins pour GitHub Actions
    private static final String CSV_OUTPUT_DIR = System.getProperty("user.home") + "/results/";
    private static final String OPTIMIZATION_INPUT_DIR = System.getProperty("user.home") + "/optimization_input/";
    private static final String OPTIMIZATION_OUTPUT_DIR = System.getProperty("user.home") + "/optimization_output/";
    
    // Compteur de drift courant
    private static int currentDriftId = 0;
    
    // FICHIERS AVANT DRIFT
    private static final String CSV_USER_ACCESS_BEFORE_PATH = CSV_OUTPUT_DIR + "1_user_access_details_before.csv";
    private static final String CSV_DATACENTER_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "2_datacenter_information_before.csv";
    private static final String CSV_FILE_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "3_file_information_before.csv";
    private static final String CSV_VM_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "4_vm_information_before.csv";
    private static final String CSV_FRAGMENT_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "5_fragment_information_before.csv";
    
    // FICHIERS APRES DRIFT (AVANT OPTIMISATION)
    private static final String CSV_DRIFT_REPORT_PATH = CSV_OUTPUT_DIR + "6_drift_impact_report.csv";
    private static final String CSV_USER_ACCESS_DRIFT_LOG_PATH = CSV_OUTPUT_DIR + "7_user_access_drift_log.csv";
    private static final String CSV_USER_ACCESS_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "8_user_access_details_after_drift.csv";
    private static final String CSV_DATACENTER_INFO_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "9_datacenter_information_after_drift.csv";
    private static final String CSV_FILE_INFO_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "10_file_information_after_drift.csv";
    private static final String CSV_FRAGMENT_INFO_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "11_fragment_information_after_drift.csv";
    private static final String CSV_SYSTEM_STATE_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "12_system_state_after_drift.csv";
    
    // FICHIERS DE DEMANDE D'OPTIMISATION
    private static final String CSV_OPTIMIZATION_REQUEST_PATH = OPTIMIZATION_INPUT_DIR + "optimization_request.csv";
    private static final String CSV_OPTIMIZATION_REQUEST_DC_PATH = OPTIMIZATION_INPUT_DIR + "optimization_request_datacenters.csv";
    private static final String CSV_OPTIMIZATION_REQUEST_USERS_PATH = OPTIMIZATION_INPUT_DIR + "optimization_request_users.csv";
    
    // FICHIERS DE RESULTATS D'OPTIMISATION (REÇUS DE SPEA2/NSGA-II)
    private static final String CSV_OPTIMIZATION_RESULT_PATH = OPTIMIZATION_OUTPUT_DIR + "optimization_result.csv";
    private static final String CSV_OPTIMIZATION_PLACEMENT_PATH = OPTIMIZATION_OUTPUT_DIR + "optimization_placement.csv";
    
    // FICHIERS APRES OPTIMISATION
    private static final String CSV_OPTIMIZED_REPLICATION_LOG_PATH = CSV_OUTPUT_DIR + "13_optimized_replication_log.csv";
    private static final String CSV_OPTIMIZED_DATACENTER_INFO_PATH = CSV_OUTPUT_DIR + "14_optimized_datacenter_information.csv";
    private static final String CSV_OPTIMIZED_FILE_INFO_PATH = CSV_OUTPUT_DIR + "15_optimized_file_information.csv";
    private static final String CSV_OPTIMIZED_FRAGMENT_INFO_PATH = CSV_OUTPUT_DIR + "16_optimized_fragment_information.csv";
    private static final String CSV_OPTIMIZED_PLACEMENT_REPORT_PATH = CSV_OUTPUT_DIR + "17_optimized_placement_report.csv";
    private static final String CSV_OPTIMIZED_SYSTEM_STATE_PATH = CSV_OUTPUT_DIR + "18_optimized_system_state.csv";
    private static final String CSV_OPTIMIZATION_SUMMARY_PATH = CSV_OUTPUT_DIR + "19_optimization_summary.csv";
    
    // Variables globales
    private static boolean googleDriveEnabled = false;
    private static final int MAX_FRAGMENT_SIZE = 2000;
    private static int fragmentIdCounter = 1;
    private static Map<Integer, Map<Integer, List<FragmentMetadata>>> fileCopiesMap = new HashMap<>();
    private static Map<Integer, Integer> vmStorageFree = new HashMap<>();
    private static Map<Integer, Integer> vmToDcMap = new HashMap<>();
    
    // Structure pour stocker les résultats d'optimisation
    private static Map<Integer, Integer> optimizedReplicas = new HashMap<>();
    private static Map<Integer, List<Integer>> optimizedPlacements = new HashMap<>();
   
    enum AccessType { READ, WRITE, READ_WRITE }
    
    static class FragmentMetadata {
        int fragmentId, originalFileId, sizeMB, vmId, dcId;
        public FragmentMetadata(int fragmentId, int originalFileId, int sizeMB, int vmId, int dcId) {
            this.fragmentId = fragmentId; this.originalFileId = originalFileId;
            this.sizeMB = sizeMB; this.vmId = vmId; this.dcId = dcId;
        }
    }

    static class User {
        int id; String location;
        Map<Integer, AccessType> fileAccess = new HashMap<>();
        Map<Integer, Integer> fileToDatacenter = new HashMap<>();
        public User(int id, String location) { this.id = id; this.location = location; }
        public void addFileAccess(int fileId, AccessType access, int datacenterId) {
            this.fileAccess.put(fileId, access);
            this.fileToDatacenter.put(fileId, datacenterId);
        }
    }

    static class ReplicaData {
        int dataID, sizeMB, importance, popularite;
        List<Integer> datacenterIds;
        Map<Integer, AccessType> userAccess = new HashMap<>();
        Map<Integer, Integer> userToDatacenter = new HashMap<>();
        Map<Integer, Integer> accessCountByDatacenter = new HashMap<>();
        public ReplicaData(int dataID, int sizeMB, int importance, int popularite, List<Integer> datacenterIds) {
            this.dataID = dataID; this.sizeMB = sizeMB; this.importance = importance;
            this.popularite = popularite; this.datacenterIds = datacenterIds;
            for (int dcId : datacenterIds) this.accessCountByDatacenter.put(dcId, 0);
        }
        public void addUserAccess(int userId, AccessType access, int datacenterId) {
            this.userAccess.put(userId, access);
            this.userToDatacenter.put(userId, datacenterId);
            this.accessCountByDatacenter.merge(datacenterId, 1, Integer::sum);
        }
        public void updateFileSize(int newSizeMB) { this.sizeMB = newSizeMB; }
        public void updateReplicas(List<Integer> newDatacenterIds) { this.datacenterIds = newDatacenterIds; }
    }

    static class RegionInfo {
        int datacenterId, hostCount, storageCapacity, storageLibre, used, vmCount, vmPerHost;
        String region, typeDc;
        double costPerCloudlet;
        List<Vm> vmList = new ArrayList<>();
        public RegionInfo(int datacenterId, String region, double costPerCloudlet, String typeDc, int hostCount, int storageCapacity) {
            this.datacenterId = datacenterId; this.region = region; this.costPerCloudlet = costPerCloudlet;
            this.typeDc = typeDc; this.hostCount = hostCount; this.storageCapacity = storageCapacity;
            this.storageLibre = storageCapacity; this.used = 0;
            this.vmCount = hostCount * VMS_PER_HOST; this.vmPerHost = VMS_PER_HOST;
        }
    }

    // ==================== GESTION DES DRIFTS ====================
    
    private static void setupDriftDirectory(int driftId) throws IOException {
        Path driftDir = Paths.get(CSV_OUTPUT_DIR + "drift_" + driftId + "/");
        Files.createDirectories(driftDir);
        Files.createDirectories(Paths.get(OPTIMIZATION_INPUT_DIR));
        Files.createDirectories(Paths.get(OPTIMIZATION_OUTPUT_DIR));
    }
    
    private static void saveDriftResults(int driftId, List<ReplicaData> fichiers, List<User> users, 
                                          List<RegionInfo> datacenterInfos, Map<Integer, String> dcidToRegion) throws IOException {
        String driftDir = CSV_OUTPUT_DIR + "drift_" + driftId + "/";
        
        // Sauvegarder les fichiers avant optimisation pour ce drift
        saveDatacenterInfoToFile(datacenterInfos, driftDir + "datacenter_before_drift_" + driftId + ".csv");
        saveFileInfoToFile(fichiers, dcidToRegion, driftDir + "files_before_drift_" + driftId + ".csv");
        saveUserAccessToFile(users, driftDir + "users_before_drift_" + driftId + ".csv");
        
        System.out.println("📁 Résultats du drift " + driftId + " sauvegardés");
    }
    
    private static void saveAfterOptimizationResults(int driftId, List<ReplicaData> fichiers, 
                                                       List<RegionInfo> datacenterInfos, 
                                                       Map<Integer, String> dcidToRegion) throws IOException {
        String driftDir = CSV_OUTPUT_DIR + "drift_" + driftId + "/";
        
        // Sauvegarder les fichiers après optimisation
        saveDatacenterInfoToFile(datacenterInfos, driftDir + "datacenter_after_optimization_" + driftId + ".csv");
        saveFileInfoToFile(fichiers, dcidToRegion, driftDir + "files_after_optimization_" + driftId + ".csv");
        
        System.out.println("📁 Résultats optimisés du drift " + driftId + " sauvegardés");
    }
    
    private static void saveDatacenterInfoToFile(List<RegionInfo> datacenterInfos, String filename) throws IOException {
        Path path = Paths.get(filename);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("DatacenterID,Region,TypeDC,HostCount,StorageCapacityMB,StorageUsedMB,StorageFreeMB,UsagePercentage,VMCount");
        for (RegionInfo info : datacenterInfos) {
            double usagePercentage = (double) info.used / info.storageCapacity * 100;
            lines.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.2f%%,%d",
                    info.datacenterId, info.region, info.typeDc, info.hostCount,
                    info.storageCapacity, info.used, info.storageLibre, usagePercentage, info.vmCount));
        }
        Files.write(path, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
    
    private static void saveFileInfoToFile(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion, String filename) throws IOException {
        Path path = Paths.get(filename);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,Importance,ReplicaCount,ReplicaLocations,UserAccessCount");
        for (ReplicaData fichier : fichiers) {
            String replicaLocations = fichier.datacenterIds.stream()
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining("; "));
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%d",
                fichier.dataID, fichier.sizeMB, fichier.importance, 
                fichier.datacenterIds.size(), replicaLocations, fichier.userAccess.size()));
        }
        Files.write(path, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
    
    private static void saveUserAccessToFile(List<User> users, String filename) throws IOException {
        Path path = Paths.get(filename);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("UserID,UserLocation,FileID,AccessType");
        for (User user : users) {
            for (Map.Entry<Integer, AccessType> entry : user.fileAccess.entrySet()) {
                lines.add(String.format("%d,%s,%d,%s", user.id, user.location, entry.getKey(), entry.getValue()));
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
    
    // ==================== RÉCEPTION DES RÉSULTATS D'OPTIMISATION ====================
    
    private static boolean waitForOptimizationResults(int timeoutSeconds) {
        System.out.println("⏳ Attente des résultats d'optimisation (SPEA2 + NSGA-II)...");
        
        long startTime = System.currentTimeMillis();
        long timeout = timeoutSeconds * 1000L;
        
        while (System.currentTimeMillis() - startTime < timeout) {
            try {
                Path resultFile = Paths.get(CSV_OPTIMIZATION_RESULT_PATH);
                Path placementFile = Paths.get(CSV_OPTIMIZATION_PLACEMENT_PATH);
                
                if (Files.exists(resultFile) && Files.exists(placementFile)) {
                    System.out.println("✅ Résultats d'optimisation reçus !");
                    return loadOptimizationResults();
                }
                
                Thread.sleep(5000); // Attendre 5 secondes
                System.out.print(".");
                
            } catch (Exception e) {
                System.err.println("Erreur: " + e.getMessage());
            }
        }
        
        System.out.println("\n⚠️ Délai d'attente dépassé. Utilisation de la réplication par défaut.");
        return false;
    }
    
    private static boolean loadOptimizationResults() {
        try {
            // Charger les résultats SPEA2 - sélection des fichiers à répliquer
            Path resultPath = Paths.get(CSV_OPTIMIZATION_RESULT_PATH);
            if (Files.exists(resultPath)) {
                List<String> lines = Files.readAllLines(resultPath);
                for (int i = 1; i < lines.size(); i++) {
                    String[] parts = lines.get(i).split(",");
                    if (parts.length >= 5) {
                        int fileId = Integer.parseInt(parts[0].trim());
                        int replicas = Integer.parseInt(parts[4].trim());
                        optimizedReplicas.put(fileId, replicas);
                    }
                }
                System.out.println("📊 " + optimizedReplicas.size() + " fichiers sélectionnés par SPEA2");
            }
            
            // Charger les placements NSGA-II
            Path placementPath = Paths.get(CSV_OPTIMIZATION_PLACEMENT_PATH);
            if (Files.exists(placementPath)) {
                List<String> lines = Files.readAllLines(placementPath);
                for (int i = 1; i < lines.size(); i++) {
                    String[] parts = lines.get(i).split(",");
                    if (parts.length >= 3) {
                        int fileId = Integer.parseInt(parts[0].trim());
                        int datacenterId = Integer.parseInt(parts[2].trim());
                        optimizedPlacements.computeIfAbsent(fileId, k -> new ArrayList<>()).add(datacenterId);
                    }
                }
                System.out.println("📍 " + optimizedPlacements.size() + " placements optimisés par NSGA-II");
            }
            
            return !optimizedReplicas.isEmpty() || !optimizedPlacements.isEmpty();
            
        } catch (Exception e) {
            System.err.println("❌ Erreur chargement optimisation: " + e.getMessage());
            return false;
        }
    }
    
    // ==================== APPLICATION DE L'OPTIMISATION ====================
    
    private static void applyOptimizationToReplication(List<ReplicaData> fichiers, 
                                                        Map<Integer, RegionInfo> datacenterMap) {
        System.out.println("\n=== APPLICATION DE L'OPTIMISATION ===");
        
        int filesOptimized = 0;
        
        for (ReplicaData file : fichiers) {
            // Appliquer SPEA2 : ajuster le nombre de répliques
            if (optimizedReplicas.containsKey(file.dataID)) {
                int targetReplicas = optimizedReplicas.get(file.dataID);
                int currentReplicas = file.datacenterIds.size();
                
                if (targetReplicas > currentReplicas) {
                    // Ajouter des répliques
                    int toAdd = targetReplicas - currentReplicas;
                    for (int i = 0; i < toAdd; i++) {
                        // Choisir un datacenter avec de l'espace
                        for (RegionInfo dc : datacenterMap.values()) {
                            if (!file.datacenterIds.contains(dc.datacenterId) && dc.storageLibre >= file.sizeMB) {
                                file.datacenterIds.add(dc.datacenterId);
                                dc.storageLibre -= file.sizeMB;
                                dc.used += file.sizeMB;
                                filesOptimized++;
                                System.out.printf("  + Ajout réplique Fichier %d → DC %d\n", file.dataID, dc.datacenterId);
                                break;
                            }
                        }
                    }
                }
            }
            
            // Appliquer NSGA-II : placements spécifiques
            if (optimizedPlacements.containsKey(file.dataID)) {
                List<Integer> newPlacements = optimizedPlacements.get(file.dataID);
                
                // Vérifier la capacité avant de remplacer
                boolean hasCapacity = true;
                for (int dcId : newPlacements) {
                    RegionInfo dc = datacenterMap.get(dcId);
                    if (dc.storageLibre < file.sizeMB) {
                        hasCapacity = false;
                        break;
                    }
                }
                
                if (hasCapacity && !newPlacements.isEmpty()) {
                    // Libérer l'ancien espace
                    for (int oldDcId : file.datacenterIds) {
                        RegionInfo dc = datacenterMap.get(oldDcId);
                        dc.storageLibre += file.sizeMB;
                        dc.used -= file.sizeMB;
                    }
                    
                    // Appliquer les nouveaux placements
                    file.updateReplicas(new ArrayList<>(newPlacements));
                    for (int newDcId : newPlacements) {
                        RegionInfo dc = datacenterMap.get(newDcId);
                        dc.storageLibre -= file.sizeMB;
                        dc.used += file.sizeMB;
                    }
                    filesOptimized++;
                    System.out.printf("  ✓ Placement optimisé Fichier %d → %s\n", file.dataID, newPlacements);
                }
            }
        }
        
        System.out.printf("✅ Optimisation appliquée: %d fichiers modifiés\n", filesOptimized);
    }
    
    // ==================== MÉTHODES DE GÉNÉRATION CSV EXISTANTES ====================
    // (Toutes les méthodes de génération CSV doivent être conservées ici)
    // generateUserAccessBeforeCSV, generateDatacenterInfoBeforeCSV, etc.
    // Je les ai conservées mais omises pour la lisibilité - à recopier de votre code existant
    
    private static void generateUserAccessBeforeCSV(List<User> users) throws IOException {
        Path path = Paths.get(CSV_USER_ACCESS_BEFORE_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("UserID,UserLocation,FileID,AccessType,DatacenterAccessed,DatacenterRegion");
        for (User user : users) {
            for (Map.Entry<Integer, AccessType> entry : user.fileAccess.entrySet()) {
                int fileId = entry.getKey();
                int dcId = user.fileToDatacenter.get(fileId);
                String region = getRegionForDatacenter(dcId);
                lines.add(String.format("%d,%s,%d,%s,%d,%s",
                        user.id, user.location, fileId, entry.getValue().toString(), dcId, region));
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_USER_ACCESS_BEFORE_PATH);
    }

    private static void generateDatacenterInfoBeforeCSV(List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_DATACENTER_INFO_BEFORE_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("DatacenterID,Region,TypeDC,HostCount,StorageCapacityMB,StorageUsedMB,StorageFreeMB,UsagePercentage,VMCount,VMPerHost");
        for (RegionInfo info : datacenterInfos) {
            double usagePercentage = (double) info.used / info.storageCapacity * 100;
            lines.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.2f%%,%d,%d",
                    info.datacenterId, info.region, info.typeDc, info.hostCount,
                    info.storageCapacity, info.used, info.storageLibre, usagePercentage,
                    info.vmCount, info.vmPerHost));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_DATACENTER_INFO_BEFORE_PATH);
    }

    private static void generateFileInfoBeforeCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_FILE_INFO_BEFORE_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,Importance,ReplicaCount,ReplicaLocations,UserAccessCount,ReadCount,WriteCount,ReadWriteCount");
        for (ReplicaData fichier : fichiers) {
            String replicaLocations = fichier.datacenterIds.stream()
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining("; "));
            
            int readCount = 0, writeCount = 0, readWriteCount = 0;
            for (AccessType access : fichier.userAccess.values()) {
                if (access == AccessType.READ) readCount++;
                else if (access == AccessType.WRITE) writeCount++;
                else if (access == AccessType.READ_WRITE) readWriteCount++;
            }
            
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%d,%d,%d,%d",
                fichier.dataID, fichier.sizeMB, fichier.importance, 
                fichier.datacenterIds.size(), replicaLocations,
                fichier.userAccess.size(), readCount, writeCount, readWriteCount));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_FILE_INFO_BEFORE_PATH);
    }

    private static void generateVmInfoBeforeCSV(List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_VM_INFO_BEFORE_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("VM_ID,DatacenterID,Region,MIPS,RAM_MB,Bandwidth_Mbps,Storage_MB,StorageFree_MB");
        for (RegionInfo info : datacenterInfos) {
            for (Vm vm : info.vmList) {
                int storageFree = vmStorageFree.getOrDefault(vm.getId(), (int) VM_SIZE);
                lines.add(String.format("%d,%d,%s,%d,%d,%d,%d,%d",
                        vm.getId(), info.datacenterId, info.region,
                        (int)vm.getMips(), vm.getRam(), vm.getBw(), 
                        vm.getSize(), storageFree));
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_VM_INFO_BEFORE_PATH);
    }

    private static void generateFragmentInfoBeforeCSV() throws IOException {
        Path path = Paths.get(CSV_FRAGMENT_INFO_BEFORE_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FragmentID,FileID,SizeMB,VM_ID,DatacenterID,Region");
        for (Map.Entry<Integer, Map<Integer, List<FragmentMetadata>>> fileEntry : fileCopiesMap.entrySet()) {
            int fileId = fileEntry.getKey();
            for (Map.Entry<Integer, List<FragmentMetadata>> dcEntry : fileEntry.getValue().entrySet()) {
                int dcId = dcEntry.getKey();
                String region = getRegionForDatacenter(dcId);
                for (FragmentMetadata fragment : dcEntry.getValue()) {
                    lines.add(String.format("%d,%d,%d,%d,%d,%s",
                            fragment.fragmentId, fragment.originalFileId, fragment.sizeMB,
                            fragment.vmId, fragment.dcId, region));
                }
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_FRAGMENT_INFO_BEFORE_PATH);
    }

    private static void generateDriftImpactReport(List<ReplicaData> fichiers, 
                                                   Map<Integer, Integer> originalSizes,
                                                   Map<Integer, Map<Integer, AccessType>> originalAccess) throws IOException {
        Path path = Paths.get(CSV_DRIFT_REPORT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,OriginalSizeMB,NewSizeMB,SizeChangeMB,SizeChangePercentage,OriginalAccessCount,NewAccessCount,AccessChangeCount");
        
        for (ReplicaData file : fichiers) {
            int originalSize = originalSizes.getOrDefault(file.dataID, file.sizeMB);
            int sizeChange = file.sizeMB - originalSize;
            double sizeChangePercent = originalSize > 0 ? (double) sizeChange / originalSize * 100 : 0;
            
            int originalAccessCount = originalAccess.containsKey(file.dataID) ? originalAccess.get(file.dataID).size() : 0;
            int newAccessCount = file.userAccess.size();
            int accessChange = newAccessCount - originalAccessCount;
            
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%.2f%%,%d,%d,%d",
                file.dataID, originalSize, file.sizeMB, sizeChange, sizeChangePercent,
                originalAccessCount, newAccessCount, accessChange));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_DRIFT_REPORT_PATH);
    }

    private static void generateUserAccessDriftLog(List<User> users, List<User> originalUsers) throws IOException {
        Path path = Paths.get(CSV_USER_ACCESS_DRIFT_LOG_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("UserID,UserLocation,FileID,ChangeType,OldAccessType,NewAccessType,Timestamp");
        
        for (User user : users) {
            User originalUser = originalUsers.stream().filter(u -> u.id == user.id).findFirst().orElse(null);
            if (originalUser != null) {
                for (Map.Entry<Integer, AccessType> originalEntry : originalUser.fileAccess.entrySet()) {
                    if (!user.fileAccess.containsKey(originalEntry.getKey())) {
                        lines.add(String.format("%d,%s,%d,REMOVED,%s,N/A,%d",
                            user.id, user.location, originalEntry.getKey(), 
                            originalEntry.getValue(), System.currentTimeMillis()));
                    }
                }
                for (Map.Entry<Integer, AccessType> newEntry : user.fileAccess.entrySet()) {
                    if (!originalUser.fileAccess.containsKey(newEntry.getKey())) {
                        lines.add(String.format("%d,%s,%d,ADDED,N/A,%s,%d",
                            user.id, user.location, newEntry.getKey(),
                            newEntry.getValue(), System.currentTimeMillis()));
                    } else {
                        AccessType oldAccess = originalUser.fileAccess.get(newEntry.getKey());
                        if (oldAccess != newEntry.getValue()) {
                            lines.add(String.format("%d,%s,%d,MODIFIED,%s,%s,%d",
                                user.id, user.location, newEntry.getKey(),
                                oldAccess, newEntry.getValue(), System.currentTimeMillis()));
                        }
                    }
                }
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_USER_ACCESS_DRIFT_LOG_PATH);
    }

    private static void generateUserAccessAfterDriftCSV(List<User> users) throws IOException {
        Path path = Paths.get(CSV_USER_ACCESS_AFTER_DRIFT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("UserID,UserLocation,FileID,AccessType,DatacenterAccessed,DatacenterRegion");
        for (User user : users) {
            for (Map.Entry<Integer, AccessType> entry : user.fileAccess.entrySet()) {
                int fileId = entry.getKey();
                int dcId = user.fileToDatacenter.get(fileId);
                String region = getRegionForDatacenter(dcId);
                lines.add(String.format("%d,%s,%d,%s,%d,%s",
                        user.id, user.location, fileId, entry.getValue().toString(), dcId, region));
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_USER_ACCESS_AFTER_DRIFT_PATH);
    }

    private static void generateDatacenterInfoAfterDriftCSV(List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_DATACENTER_INFO_AFTER_DRIFT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("DatacenterID,Region,TypeDC,HostCount,StorageCapacityMB,StorageUsedMB,StorageFreeMB,UsagePercentage,VMCount,VMPerHost");
        for (RegionInfo info : datacenterInfos) {
            double usagePercentage = (double) info.used / info.storageCapacity * 100;
            lines.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.2f%%,%d,%d",
                    info.datacenterId, info.region, info.typeDc, info.hostCount,
                    info.storageCapacity, info.used, info.storageLibre, usagePercentage,
                    info.vmCount, info.vmPerHost));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_DATACENTER_INFO_AFTER_DRIFT_PATH);
    }

    private static void generateFileInfoAfterDriftCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_FILE_INFO_AFTER_DRIFT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,Importance,ReplicaCount,ReplicaLocations,UserAccessCount,ReadCount,WriteCount,ReadWriteCount");
        for (ReplicaData fichier : fichiers) {
            String replicaLocations = fichier.datacenterIds.stream()
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining("; "));
            
            int readCount = 0, writeCount = 0, readWriteCount = 0;
            for (AccessType access : fichier.userAccess.values()) {
                if (access == AccessType.READ) readCount++;
                else if (access == AccessType.WRITE) writeCount++;
                else if (access == AccessType.READ_WRITE) readWriteCount++;
            }
            
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%d,%d,%d,%d",
                fichier.dataID, fichier.sizeMB, fichier.importance, 
                fichier.datacenterIds.size(), replicaLocations,
                fichier.userAccess.size(), readCount, writeCount, readWriteCount));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_FILE_INFO_AFTER_DRIFT_PATH);
    }

    private static void generateFragmentInfoAfterDriftCSV() throws IOException {
        Path path = Paths.get(CSV_FRAGMENT_INFO_AFTER_DRIFT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FragmentID,FileID,SizeMB,VM_ID,DatacenterID,Region");
        for (Map.Entry<Integer, Map<Integer, List<FragmentMetadata>>> fileEntry : fileCopiesMap.entrySet()) {
            int fileId = fileEntry.getKey();
            for (Map.Entry<Integer, List<FragmentMetadata>> dcEntry : fileEntry.getValue().entrySet()) {
                int dcId = dcEntry.getKey();
                String region = getRegionForDatacenter(dcId);
                for (FragmentMetadata fragment : dcEntry.getValue()) {
                    lines.add(String.format("%d,%d,%d,%d,%d,%s",
                            fragment.fragmentId, fragment.originalFileId, fragment.sizeMB,
                            fragment.vmId, fragment.dcId, region));
                }
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_FRAGMENT_INFO_AFTER_DRIFT_PATH);
    }

    private static void generateSystemStateAfterDriftCSV(List<ReplicaData> fichiers, List<User> users,
                                                         List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_SYSTEM_STATE_AFTER_DRIFT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("Timestamp," + System.currentTimeMillis());
        lines.add("TotalFiles," + fichiers.size());
        lines.add("TotalUsers," + users.size());
        lines.add("TotalDatacenters," + datacenterInfos.size());
        lines.add("");
        lines.add("FileID,SizeMB,ReplicaCount,UserAccessCount");
        for (ReplicaData file : fichiers) {
            lines.add(String.format("%d,%d,%d,%d", file.dataID, file.sizeMB, 
                    file.datacenterIds.size(), file.userAccess.size()));
        }
        lines.add("");
        lines.add("DatacenterID,Region,StorageUsedMB,StorageFreeMB,UsagePercentage");
        for (RegionInfo dc : datacenterInfos) {
            double usagePercentage = (double) dc.used / dc.storageCapacity * 100;
            lines.add(String.format(Locale.US, "%d,%s,%d,%d,%.2f%%",
                dc.datacenterId, dc.region, dc.used, dc.storageLibre, usagePercentage));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_SYSTEM_STATE_AFTER_DRIFT_PATH);
    }

    private static void generateOptimizationInputData(List<ReplicaData> fichiers, List<User> users, 
                                                       List<RegionInfo> datacenterInfos,
                                                       Map<Integer, String> dcidToRegion) throws IOException {
        System.out.println("\n=== GÉNÉRATION DES DONNÉES POUR OPTIMISATION SPEA2/NSGA-II ===");
        
        // Sauvegarder la demande d'optimisation
        Path path = Paths.get(CSV_OPTIMIZATION_REQUEST_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,UserAccessCount,CurrentReplicaCount,CurrentDatacenters,AccessPattern,Importance,PopularityScore,ReadCount,WriteCount");
        
        for (ReplicaData file : fichiers) {
            String currentDatacenters = file.datacenterIds.stream()
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining(";"));
            
            int readCount = 0, writeCount = 0;
            for (AccessType access : file.userAccess.values()) {
                if (access == AccessType.READ) readCount++;
                else if (access == AccessType.WRITE) writeCount++;
            }
            String accessPattern = (readCount > writeCount) ? "READ_HEAVY" : (writeCount > readCount) ? "WRITE_HEAVY" : "BALANCED";
            
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%s,%d,%d,%d,%d",
                file.dataID, file.sizeMB, file.userAccess.size(),
                file.datacenterIds.size(), currentDatacenters, accessPattern,
                file.importance, file.userAccess.size(), readCount, writeCount));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré demande optimisation: " + CSV_OPTIMIZATION_REQUEST_PATH);
        
        // Sauvegarder aussi les données datacenters pour l'optimisation
        saveDatacenterInfoToFile(datacenterInfos, OPTIMIZATION_INPUT_DIR + "datacenter_for_optimization.csv");
        saveUserAccessToFile(users, OPTIMIZATION_INPUT_DIR + "users_for_optimization.csv");
    }
    
    private static void generateOptimizationSummary(List<ReplicaData> fichiers, 
                                                     List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_OPTIMIZATION_SUMMARY_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("OptimizationDate," + new Date());
        lines.add("TotalFiles," + fichiers.size());
        lines.add("TotalReplicas," + fichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum());
        lines.add("FillesOptimized," + optimizedReplicas.size());
        lines.add("OptimizedPlacements," + optimizedPlacements.size());
        
        // Ajouter les détails des fichiers optimisés
        lines.add("");
        lines.add("FileID,OptimizedReplicas,OptimizedPlacements");
        for (Map.Entry<Integer, Integer> entry : optimizedReplicas.entrySet()) {
            String placements = optimizedPlacements.containsKey(entry.getKey()) 
                ? optimizedPlacements.get(entry.getKey()).toString() : "none";
            lines.add(String.format("%d,%d,%s", entry.getKey(), entry.getValue(), placements));
        }
        
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_OPTIMIZATION_SUMMARY_PATH);
    }

    private static void generateOptimizedReplicationLog(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_OPTIMIZED_REPLICATION_LOG_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,UserAccessCount,TotalReplicas,ReplicaLocations,OptimizedBy");
        for (ReplicaData file : fichiers) {
            String replicaLocations = file.datacenterIds.stream()
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining("; "));
            String optimizedBy = optimizedReplicas.containsKey(file.dataID) ? "SPEA2" : 
                                 (optimizedPlacements.containsKey(file.dataID) ? "NSGA-II" : "Default");
            lines.add(String.format("%d,%d,%d,%d,%s,%s",
                file.dataID, file.sizeMB, file.userAccess.size(),
                file.datacenterIds.size(), replicaLocations, optimizedBy));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_OPTIMIZED_REPLICATION_LOG_PATH);
    }

    private static void generateOptimizedPlacementReport(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_OPTIMIZED_PLACEMENT_REPORT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,FileSizeMB,UserAccessCount,AllReplicaLocations");
        for (ReplicaData file : fichiers) {
            String allReplicas = file.datacenterIds.stream()
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining("; "));
            lines.add(String.format("%d,%d,%d,%s", 
                file.dataID, file.sizeMB, file.userAccess.size(), allReplicas));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_OPTIMIZED_PLACEMENT_REPORT_PATH);
    }

    private static String getRegionForDatacenter(int dcId) {
        if (dcId >= 1 && dcId <= 6) return "Europe";
        if (dcId >= 7 && dcId <= 12) return "USA";
        if (dcId >= 13 && dcId <= 18) return "Asie";
        if (dcId >= 19 && dcId <= 24) return "Afrique";
        if (dcId >= 25 && dcId <= 30) return "AmeriqueSud";
        return "Unknown";
    }

    // ==================== MÉTHODES DES DRIFTS ====================
    
    private static void applyFileSizeDrift(List<ReplicaData> fichiers, Random random, 
                                          Map<Integer, RegionInfo> datacenterMap,
                                          Map<Integer, Integer> originalSizes) {
        System.out.println("\n=== DRIFT: MODIFICATION DES TAILLES DE FICHIERS ===");
        int filesModified = 0;
        for (ReplicaData file : fichiers) {
            if (random.nextDouble() < 0.3) {
                int originalSize = file.sizeMB;
                originalSizes.put(file.dataID, originalSize);
                double changeFactor = 0.5 + random.nextDouble() * 1.0;
                int newSize = (int)(originalSize * changeFactor);
                newSize = Math.max(500, Math.min(20000, newSize));
                int storageDifference = newSize - originalSize;
                
                if (storageDifference > 0) {
                    int totalAvailableSpace = 0;
                    for (int dcId : file.datacenterIds) {
                        totalAvailableSpace += datacenterMap.get(dcId).storageLibre;
                    }
                    if (totalAvailableSpace >= storageDifference) {
                        int remainingToAdd = storageDifference;
                        for (int dcId : file.datacenterIds) {
                            RegionInfo dc = datacenterMap.get(dcId);
                            int addToThisDc = Math.min(remainingToAdd, dc.storageLibre);
                            dc.storageLibre -= addToThisDc;
                            dc.used += addToThisDc;
                            remainingToAdd -= addToThisDc;
                            if (remainingToAdd == 0) break;
                        }
                        file.updateFileSize(newSize);
                        filesModified++;
                        System.out.printf("  Fichier %d: %d MB → %d MB\n", file.dataID, originalSize, newSize);
                    }
                } else if (storageDifference < 0) {
                    int spaceToFree = -storageDifference;
                    for (int dcId : file.datacenterIds) {
                        RegionInfo dc = datacenterMap.get(dcId);
                        dc.storageLibre += Math.min(spaceToFree, dc.used);
                        dc.used -= Math.min(spaceToFree, dc.used);
                    }
                    file.updateFileSize(newSize);
                    filesModified++;
                    System.out.printf("  Fichier %d: %d MB → %d MB\n", file.dataID, originalSize, newSize);
                }
            }
        }
        System.out.printf("✓ Drift taille: %d fichiers modifiés\n", filesModified);
    }

    private static void applyUserAccessDrift(List<User> users, List<ReplicaData> fichiers, Random random,
                                           Map<Integer, String> dcidToRegion,
                                           Map<Integer, Map<Integer, AccessType>> originalAccess) {
        System.out.println("\n=== DRIFT: MODIFICATION DES ACCÈS UTILISATEURS ===");
        
        for (User user : users) {
            originalAccess.put(user.id, new HashMap<>(user.fileAccess));
        }
        
        int accessChanges = 0, newAccessesAdded = 0, accessesRemoved = 0;
        
        for (User user : users) {
            if (random.nextDouble() < 0.4) {
                List<Integer> currentFiles = new ArrayList<>(user.fileAccess.keySet());
                
                int toRemove = Math.min(currentFiles.size(), 1 + random.nextInt(3));
                for (int i = 0; i < toRemove && !currentFiles.isEmpty(); i++) {
                    int fileId = currentFiles.get(random.nextInt(currentFiles.size()));
                    user.fileAccess.remove(fileId);
                    accessesRemoved++;
                    ReplicaData file = fichiers.stream().filter(f -> f.dataID == fileId).findFirst().orElse(null);
                    if (file != null) file.userAccess.remove(user.id);
                    currentFiles.remove(Integer.valueOf(fileId));
                }
                
                int toAdd = 1 + random.nextInt(3);
                for (int i = 0; i < toAdd; i++) {
                    int fileIndex = random.nextInt(fichiers.size());
                    ReplicaData file = fichiers.get(fileIndex);
                    if (!user.fileAccess.containsKey(file.dataID)) {
                        AccessType newAccess = getRandomAccessType(random);
                        String bestRegion = BEST_DC_MAP.get(user.location);
                        int chosenDc = file.datacenterIds.stream()
                            .filter(dcId -> dcidToRegion.get(dcId).equals(bestRegion))
                            .findFirst().orElse(file.datacenterIds.get(0));
                        user.addFileAccess(file.dataID, newAccess, chosenDc);
                        file.addUserAccess(user.id, newAccess, chosenDc);
                        newAccessesAdded++;
                    }
                }
                
                List<Integer> remainingFiles = new ArrayList<>(user.fileAccess.keySet());
                int toModify = Math.min(remainingFiles.size(), 1 + random.nextInt(2));
                for (int i = 0; i < toModify && !remainingFiles.isEmpty(); i++) {
                    int fileId = remainingFiles.get(random.nextInt(remainingFiles.size()));
                    AccessType newAccess = getRandomAccessType(random);
                    if (user.fileAccess.get(fileId) != newAccess) {
                        user.fileAccess.put(fileId, newAccess);
                        ReplicaData file = fichiers.stream().filter(f -> f.dataID == fileId).findFirst().orElse(null);
                        if (file != null) file.userAccess.put(user.id, newAccess);
                        accessChanges++;
                    }
                }
            }
        }
        
        System.out.printf("✓ Drift accès: %d changements, %d ajouts, %d suppressions\n", 
            accessChanges, newAccessesAdded, accessesRemoved);
    }

    private static AccessType getRandomAccessType(Random random) {
        int val = random.nextInt(3);
        return val == 0 ? AccessType.READ : (val == 1 ? AccessType.WRITE : AccessType.READ_WRITE);
    }

    private static void createOutputDirectoryIfNotExists() throws IOException {
        Path path = Paths.get(CSV_OUTPUT_DIR);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
            System.out.println("✓ Dossier créé: " + CSV_OUTPUT_DIR);
        }
        Files.createDirectories(Paths.get(OPTIMIZATION_INPUT_DIR));
        Files.createDirectories(Paths.get(OPTIMIZATION_OUTPUT_DIR));
    }

    // ==================== MÉTHODES STATIQUES INITIALES ====================
    
    private static Map<String, Map<String, Integer>> createLatencyMap() {
        Map<String, Map<String, Integer>> latencyMap = new HashMap<>();
        Map<String, Integer> europeLatency = new HashMap<>();
        europeLatency.put("Europe", 5); europeLatency.put("USA", 100); 
        europeLatency.put("Asie", 200); europeLatency.put("Afrique", 100); europeLatency.put("AmeriqueSud", 150);
        latencyMap.put("Paris (France)", europeLatency);
        latencyMap.put("Berlin (Allemagne)", europeLatency);
        latencyMap.put("Madrid (Espagne)", europeLatency);
        
        Map<String, Integer> usaLatency = new HashMap<>();
        usaLatency.put("Europe", 100); usaLatency.put("USA", 10); 
        usaLatency.put("Asie", 150); usaLatency.put("Afrique", 200); usaLatency.put("AmeriqueSud", 100);
        latencyMap.put("New York (USA)", usaLatency);
        latencyMap.put("San Francisco (USA)", usaLatency);
        
        Map<String, Integer> asiaLatency = new HashMap<>();
        asiaLatency.put("Europe", 200); asiaLatency.put("USA", 150); 
        asiaLatency.put("Asie", 10); asiaLatency.put("Afrique", 200); asiaLatency.put("AmeriqueSud", 250);
        latencyMap.put("Tokyo (Japon)", asiaLatency);
        latencyMap.put("Mumbai (Inde)", asiaLatency);
        
        Map<String, Integer> africaLatency = new HashMap<>();
        africaLatency.put("Europe", 100); africaLatency.put("USA", 200); 
        africaLatency.put("Asie", 200); africaLatency.put("Afrique", 15); africaLatency.put("AmeriqueSud", 200);
        latencyMap.put("Nairobi (Kenya)", africaLatency);
        latencyMap.put("Lagos (Nigeria)", africaLatency);
        latencyMap.put("Le Caire (Egypte)", africaLatency);
        
        return latencyMap;
    }

    private static Map<String, String> createBestDcMap() {
        Map<String, String> bestDcMap = new HashMap<>();
        bestDcMap.put("Paris (France)", "Europe"); bestDcMap.put("Berlin (Allemagne)", "Europe"); 
        bestDcMap.put("Madrid (Espagne)", "Europe"); bestDcMap.put("New York (USA)", "USA");
        bestDcMap.put("San Francisco (USA)", "USA"); bestDcMap.put("Tokyo (Japon)", "Asie");
        bestDcMap.put("Mumbai (Inde)", "Asie"); bestDcMap.put("Nairobi (Kenya)", "Afrique");
        bestDcMap.put("Lagos (Nigeria)", "Afrique"); bestDcMap.put("Le Caire (Egypte)", "Afrique");
        return bestDcMap;
    }

    private static void distributeFilesWithAccessRights(List<ReplicaData> fichiers, List<User> users,
                                                     Map<Integer, String> dcidToRegion,
                                                     Map<Integer, RegionInfo> datacenterMap,
                                                     Random random) {
        for (User user : users) {
            int filesToAccess = 1 + random.nextInt(5);
            for (int i = 0; i < filesToAccess; i++) {
                int fileIndex = random.nextInt(fichiers.size());
                ReplicaData file = fichiers.get(fileIndex);
                if (!user.fileAccess.containsKey(file.dataID)) {
                    AccessType access = getRandomAccessType(random);
                    String bestRegion = BEST_DC_MAP.get(user.location);
                    int chosenDc = file.datacenterIds.stream()
                        .filter(dcId -> dcidToRegion.get(dcId).equals(bestRegion))
                        .findFirst().orElse(file.datacenterIds.get(0));
                    user.addFileAccess(file.dataID, access, chosenDc);
                    file.addUserAccess(user.id, access, chosenDc);
                }
            }
        }
        
        for (ReplicaData file : fichiers) {
            if (file.userAccess.size() < 10) {
                int usersToAdd = 31 + random.nextInt(500) - file.userAccess.size();
                for (int i = 0; i < usersToAdd; i++) {
                    int userIndex = random.nextInt(users.size());
                    User user = users.get(userIndex);
                    if (!file.userAccess.containsKey(user.id)) {
                        AccessType access = getRandomAccessType(random);
                        String bestRegion = BEST_DC_MAP.get(user.location);
                        int chosenDc = file.datacenterIds.stream()
                            .filter(dcId -> dcidToRegion.get(dcId).equals(bestRegion))
                            .findFirst().orElse(file.datacenterIds.get(0));
                        user.addFileAccess(file.dataID, access, chosenDc);
                        file.addUserAccess(user.id, access, chosenDc);
                    }
                }
            }
        }
    }

    // ==================== MAIN PRINCIPALE AVEC BOUCLE DE DRIFTS ====================
    
    public static void main(String[] args) {
        try {
            createOutputDirectoryIfNotExists();
            
            System.out.println("=========================================");
            System.out.println("SIMULATION DE RÉPLICATION DE DONNÉES");
            System.out.println("Avec boucle de " + NOMBRE_DRIFTS + " drifts");
            System.out.println("=========================================");
            System.out.println("Démarrage: " + new Date());
            System.out.println("Dossier local: " + CSV_OUTPUT_DIR);
            System.out.println();
            
            // Configuration de base (datacenters, fichiers, utilisateurs)
            Random seededRandom = new Random(RANDOM_SEED);
            CloudSim.init(1, Calendar.getInstance(), false);

            // Configuration des datacenters
            List<RegionInfo> datacenterInfos = new ArrayList<>();
            Map<Integer, String> dcidToRegion = new HashMap<>();
            Map<Integer, RegionInfo> datacenterMap = new HashMap<>();
            
            String[] regions = {"Europe", "Europe", "Europe", "Europe", "Europe", "Europe",
                                "USA", "USA", "USA", "USA", "USA", "USA",
                                "Asie", "Asie", "Asie", "Asie", "Asie", "Asie",
                                "Afrique", "Afrique", "Afrique", "Afrique", "Afrique", "Afrique",
                                "AmeriqueSud", "AmeriqueSud", "AmeriqueSud", "AmeriqueSud", "AmeriqueSud", "AmeriqueSud"};
            String[] types = {"grand", "grand", "moyen", "moyen", "mini", "mini",
                              "grand", "grand", "grand", "moyen", "moyen", "mini",
                              "grand", "grand", "moyen", "moyen", "mini", "mini",
                              "grand", "moyen", "moyen", "mini", "mini", "mini",
                              "grand", "moyen", "moyen", "mini", "mini", "mini"};
            double[] costs = {0.06, 0.06, 0.06, 0.06, 0.06, 0.06,
                              0.05, 0.05, 0.05, 0.05, 0.05, 0.05,
                              0.045, 0.045, 0.045, 0.045, 0.045, 0.045,
                              0.035, 0.035, 0.035, 0.035, 0.035, 0.035,
                              0.04, 0.04, 0.04, 0.04, 0.04, 0.04};
            
            for (int i = 1; i <= 30; i++) {
                int hostCount = types[i-1].equals("grand") ? 100 : (types[i-1].equals("moyen") ? 60 : 30);
                int storageCapacity = hostCount * 100000;
                RegionInfo info = new RegionInfo(i, regions[i-1], costs[i-1], types[i-1], hostCount, storageCapacity);
                datacenterInfos.add(info);
                datacenterMap.put(i, info);
                dcidToRegion.put(i, regions[i-1]);
            }
            
            // Création des VMs
            vmStorageFree = new HashMap<>();
            vmToDcMap = new HashMap<>();
            int vmId = 1;
            for (RegionInfo info : datacenterInfos) {
                for (int j = 0; j < info.vmCount; j++) {
                    vmStorageFree.put(vmId, (int) VM_SIZE);
                    vmToDcMap.put(vmId, info.datacenterId);
                    vmId++;
                }
            }
            
            // Création des fichiers (500 fichiers)
            List<ReplicaData> fichiers = new ArrayList<>();
            List<Integer> taillesFixes = Arrays.asList(1500, 2500, 5000, 7500, 10000);

            System.out.println("Création de 500 fichiers...");
            for (int i = 1; i <= 500; i++) {
                int taille = taillesFixes.get((i-1) % taillesFixes.size());
                List<Integer> selectedDCs = new ArrayList<>();
                selectedDCs.add(((i-1) % 30) + 1);
                selectedDCs.add(((i-1) + 15) % 30 + 1);
                ReplicaData newFile = new ReplicaData(i, taille, ((i-1) % 5) + 1, 0, new ArrayList<>(selectedDCs));
                fichiers.add(newFile);
            }

            // Mise à jour stockage datacenters
            Map<Integer, Integer> usedStorage = new HashMap<>();
            for (ReplicaData f : fichiers) {
                for (int dcId : f.datacenterIds) {
                    usedStorage.put(dcId, usedStorage.getOrDefault(dcId, 0) + f.sizeMB);
                }
            }
            for (RegionInfo info : datacenterInfos) {
                info.used = usedStorage.getOrDefault(info.datacenterId, 0);
                info.storageLibre = info.storageCapacity - info.used;
            }

            // Création utilisateurs (1000 utilisateurs)
            List<User> users = new ArrayList<>();
            String[] locations = {"Paris (France)", "Berlin (Allemagne)", "Madrid (Espagne)", "New York (USA)", 
                "San Francisco (USA)", "Tokyo (Japon)", "Mumbai (Inde)", "Nairobi (Kenya)", "Lagos (Nigeria)", "Le Caire (Egypte)"};
            int[] userCounts = {80, 160, 100, 120, 80, 100, 120, 60, 100, 80};
            int currentId = 1;
            for (int i = 0; i < locations.length; i++) {
                for (int j = 0; j < userCounts[i]; j++) {
                    users.add(new User(currentId++, locations[i]));
                }
            }
            System.out.println("Utilisateurs: " + users.size());

            // Distribution initiale des droits d'accès
            distributeFilesWithAccessRights(fichiers, users, dcidToRegion, datacenterMap, seededRandom);
            
            // ==================== BOUCLE PRINCIPALE DES DRIFTS ====================
            for (int driftId = 1; driftId <= NOMBRE_DRIFTS; driftId++) {
                System.out.println("\n" + "=".repeat(60));
                System.out.println("🚀 DRIFT " + driftId + "/" + NOMBRE_DRIFTS);
                System.out.println("=".repeat(60));
                
                // Sauvegarder l'état AVANT ce drift
                saveDriftResults(driftId, fichiers, users, datacenterInfos, dcidToRegion);
                
                // Générer les CSV avant drift (pour GitHub Actions)
                generateUserAccessBeforeCSV(users);
                generateDatacenterInfoBeforeCSV(datacenterInfos);
                generateFileInfoBeforeCSV(fichiers, dcidToRegion);
                generateVmInfoBeforeCSV(datacenterInfos);
                generateFragmentInfoBeforeCSV();
                
                // Phase 1: Sauvegarder l'état original (avant drift)
                Map<Integer, Integer> originalSizes = new HashMap<>();
                Map<Integer, Map<Integer, AccessType>> originalAccess = new HashMap<>();
                List<User> originalUsers = new ArrayList<>();
                for (User user : users) {
                    User copy = new User(user.id, user.location);
                    copy.fileAccess.putAll(user.fileAccess);
                    originalUsers.add(copy);
                }
                
                // Phase 2: Appliquer les drifts
                applyFileSizeDrift(fichiers, seededRandom, datacenterMap, originalSizes);
                applyUserAccessDrift(users, fichiers, seededRandom, dcidToRegion, originalAccess);
                
                // Générer les rapports après drift
                generateDriftImpactReport(fichiers, originalSizes, originalAccess);
                generateUserAccessDriftLog(users, originalUsers);
                generateUserAccessAfterDriftCSV(users);
                generateDatacenterInfoAfterDriftCSV(datacenterInfos);
                generateFileInfoAfterDriftCSV(fichiers, dcidToRegion);
                generateFragmentInfoAfterDriftCSV();
                generateSystemStateAfterDriftCSV(fichiers, users, datacenterInfos);
                
                // Phase 3: Générer les données pour l'optimisation (SPEA2/NSGA-II)
                generateOptimizationInputData(fichiers, users, datacenterInfos, dcidToRegion);
                
                // Phase 4: Attendre les résultats d'optimisation de SPEA2/NSGA-II
                System.out.println("\n📤 Données envoyées pour optimisation SPEA2/NSGA-II");
                System.out.println("📁 Fichiers dans: " + OPTIMIZATION_INPUT_DIR);
                
                boolean optimizationReceived = waitForOptimizationResults(300); // 5 minutes max
                
                // Phase 5: Appliquer l'optimisation si reçue
                if (optimizationReceived) {
                    applyOptimizationToReplication(fichiers, datacenterMap);
                } else {
                    // Utiliser la réplication par popularité par défaut
                    System.out.println("\n=== RÉPLICATION BASÉE SUR LA POPULARITÉ (DÉFAUT) ===");
                    applyDefaultReplication(fichiers, datacenterMap, dcidToRegion, seededRandom);
                }
                
                // Phase 6: Générer les rapports après optimisation
                generateOptimizedReplicationLog(fichiers, dcidToRegion);
                generateOptimizedPlacementReport(fichiers, dcidToRegion);
                generateDatacenterInfoAfterDriftCSV(datacenterInfos);
                generateFileInfoAfterDriftCSV(fichiers, dcidToRegion);
                generateFragmentInfoAfterDriftCSV();
                generateSystemStateAfterDriftCSV(fichiers, users, datacenterInfos);
                generateOptimizationSummary(fichiers, datacenterInfos);
                
                // Phase 7: Sauvegarder les résultats après optimisation
                saveAfterOptimizationResults(driftId, fichiers, datacenterInfos, dcidToRegion);
                
                System.out.println("\n✅ Drift " + driftId + " terminé avec succès");
                
                // Petite pause entre les drifts
                Thread.sleep(1000);
            }
            
            // ==================== RAPPORT FINAL ====================
            System.out.println("\n" + "=".repeat(60));
            System.out.println("✅ SIMULATION TERMINÉE AVEC SUCCÈS");
            System.out.println("=".repeat(60));
            System.out.println("📁 " + NOMBRE_DRIFTS + " drifts traités");
            System.out.println("📁 Dossier local: " + CSV_OUTPUT_DIR);
            System.out.println("=".repeat(60));

            CloudSim.stopSimulation();
            
        } catch (Exception e) {
            System.err.println("❌ ERREUR: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void applyDefaultReplication(List<ReplicaData> fichiers,
                                                Map<Integer, RegionInfo> datacenterMap,
                                                Map<Integer, String> dcidToRegion,
                                                Random random) {
        System.out.println("\n=== RÉPLICATION BASÉE SUR LA POPULARITÉ ===");
        
        List<ReplicaData> sortedFiles = fichiers.stream()
            .sorted(Comparator.comparingInt(f -> -f.userAccess.size()))
            .collect(Collectors.toList());
        
        int replicationThreshold = (int) (fichiers.size() * 0.3);
        int replicationCount = 0;
        
        for (int i = 0; i < replicationThreshold; i++) {
            ReplicaData file = sortedFiles.get(i);
            
            Map<String, Integer> regionAccessCount = new HashMap<>();
            for (int userId : file.userAccess.keySet()) {
                int dcId = file.userToDatacenter.get(userId);
                regionAccessCount.merge(dcidToRegion.get(dcId), 1, Integer::sum);
            }
            
            List<String> candidateRegions = regionAccessCount.entrySet().stream()
                .filter(e -> e.getValue() > file.userAccess.size() * 0.1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            
            if (!candidateRegions.isEmpty()) {
                String targetRegion = candidateRegions.get(random.nextInt(candidateRegions.size()));
                List<RegionInfo> candidateDCs = datacenterMap.values().stream()
                    .filter(dc -> dc.region.equals(targetRegion))
                    .filter(dc -> dc.storageLibre >= file.sizeMB)
                    .filter(dc -> !file.datacenterIds.contains(dc.datacenterId))
                    .collect(Collectors.toList());
                    
                if (!candidateDCs.isEmpty()) {
                    RegionInfo targetDC = candidateDCs.get(random.nextInt(candidateDCs.size()));
                    file.datacenterIds.add(targetDC.datacenterId);
                    targetDC.storageLibre -= file.sizeMB;
                    targetDC.used += file.sizeMB;
                    replicationCount++;
                    System.out.printf("  + Réplique: Fichier %d (popularité=%d) → DC %d (%s)\n", 
                        file.dataID, file.userAccess.size(), targetDC.datacenterId, targetRegion);
                }
            }
        }
        System.out.printf("✓ Réplication: %d fichiers répliqués\n", replicationCount);
    }
}
