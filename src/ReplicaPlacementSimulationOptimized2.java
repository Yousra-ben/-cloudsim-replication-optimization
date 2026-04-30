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
   
    // CHEMIN CORRIGÉ POUR GITHUB ACTIONS (Linux) et Windows
    private static final String CSV_OUTPUT_DIR = 
        System.getProperty("user.home") + 
        (System.getProperty("os.name").toLowerCase().contains("win") ? "\\Desktop\\testcodedreplicationcsv\\" : "/results/");
    
    // FICHIERS AVANT DRIFT
    private static final String CSV_USER_ACCESS_BEFORE_PATH = CSV_OUTPUT_DIR + "1_user_access_details_before.csv";
    private static final String CSV_DATACENTER_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "2_datacenter_information_before.csv";
    private static final String CSV_FILE_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "3_file_information_before.csv";
    private static final String CSV_VM_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "4_vm_information_before.csv";
    private static final String CSV_FRAGMENT_INFO_BEFORE_PATH = CSV_OUTPUT_DIR + "5_fragment_information_before.csv";
    
    // FICHIERS APRES DRIFT
    private static final String CSV_DRIFT_REPORT_PATH = CSV_OUTPUT_DIR + "6_drift_impact_report.csv";
    private static final String CSV_USER_ACCESS_DRIFT_LOG_PATH = CSV_OUTPUT_DIR + "7_user_access_drift_log.csv";
    private static final String CSV_USER_ACCESS_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "8_user_access_details_after_drift.csv";
    private static final String CSV_DATACENTER_INFO_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "9_datacenter_information_after_drift.csv";
    private static final String CSV_FILE_INFO_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "10_file_information_after_drift.csv";
    private static final String CSV_FRAGMENT_INFO_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "11_fragment_information_after_drift.csv";
    private static final String CSV_SYSTEM_STATE_AFTER_DRIFT_PATH = CSV_OUTPUT_DIR + "12_system_state_after_drift.csv";
    
    // FICHIERS POUR OPTIMISATION
    private static final String CSV_OPTIMIZATION_INPUT_PATH = CSV_OUTPUT_DIR + "optimization_input_data.csv";
    private static final String CSV_OPTIMIZATION_CONFIG_PATH = CSV_OUTPUT_DIR + "optimization_config.json";
    
    // FICHIERS APRES REPLICATION
    private static final String CSV_REPLICATION_LOG_PATH = CSV_OUTPUT_DIR + "13_replication_log.csv";
    private static final String CSV_DATACENTER_INFO_AFTER_REPLICATION_PATH = CSV_OUTPUT_DIR + "14_datacenter_information_after_replication.csv";
    private static final String CSV_FILE_INFO_AFTER_REPLICATION_PATH = CSV_OUTPUT_DIR + "15_file_information_after_replication.csv";
    private static final String CSV_FRAGMENT_INFO_AFTER_REPLICATION_PATH = CSV_OUTPUT_DIR + "16_fragment_information_after_replication.csv";
    private static final String CSV_PLACEMENT_REPORT_PATH = CSV_OUTPUT_DIR + "17_placement_report.csv";
    private static final String CSV_SYSTEM_STATE_AFTER_REPLICATION_PATH = CSV_OUTPUT_DIR + "18_system_state_after_replication.csv";
    
    // RAPPORT FINAL
    private static final String CSV_SIMULATION_SUMMARY_PATH = CSV_OUTPUT_DIR + "19_simulation_summary.csv";
   
    // Configuration Google Drive (DESACTIVE par defaut pour GitHub Actions)
    private static boolean googleDriveEnabled = false;
   
    private static final int MAX_FRAGMENT_SIZE = 2000;
    private static int fragmentIdCounter = 1;
    private static Map<Integer, Map<Integer, List<FragmentMetadata>>> fileCopiesMap = new HashMap<>();
    private static Map<Integer, Integer> vmStorageFree = new HashMap<>();
    private static Map<Integer, Integer> vmToDcMap = new HashMap<>();
   
    enum AccessType {
        READ, WRITE, READ_WRITE
    }
    
    static class FragmentMetadata {
        int fragmentId;
        int originalFileId;
        int sizeMB;
        int vmId;
        int dcId;

        public FragmentMetadata(int fragmentId, int originalFileId, int sizeMB, int vmId, int dcId) {
            this.fragmentId = fragmentId;
            this.originalFileId = originalFileId;
            this.sizeMB = sizeMB;
            this.vmId = vmId;
            this.dcId = dcId;
        }
    }

    static class User {
        int id;
        String location;
        Map<Integer, AccessType> fileAccess;
        Map<Integer, Integer> fileToDatacenter;

        public User(int id, String location) {
            this.id = id;
            this.location = location;
            this.fileAccess = new HashMap<>();
            this.fileToDatacenter = new HashMap<>();
        }

        public void addFileAccess(int fileId, AccessType access, int datacenterId) {
            this.fileAccess.put(fileId, access);
            this.fileToDatacenter.put(fileId, datacenterId);
        }
    }

    static class ReplicaData {
        int dataID;
        int sizeMB;
        int importance;
        int popularite;
        List<Integer> datacenterIds;
        Map<Integer, AccessType> userAccess;
        Map<Integer, Integer> userToDatacenter;
        Map<Integer, Integer> accessCountByDatacenter;

        public ReplicaData(int dataID, int sizeMB, int importance, int popularite, List<Integer> datacenterIds) {
            this.dataID = dataID;
            this.sizeMB = sizeMB;
            this.importance = importance;
            this.popularite = popularite;
            this.datacenterIds = datacenterIds;
            this.userAccess = new HashMap<>();
            this.userToDatacenter = new HashMap<>();
            this.accessCountByDatacenter = new HashMap<>();
            for (int dcId : datacenterIds) {
                this.accessCountByDatacenter.put(dcId, 0);
            }
        }

        public int getDataID() { return dataID; }
        public int getSizeMB() { return sizeMB; }
        public List<Integer> getDatacenterIds() { return datacenterIds; }

        public void addUserAccess(int userId, AccessType access, int datacenterId) {
            this.userAccess.put(userId, access);
            this.userToDatacenter.put(userId, datacenterId);
            this.accessCountByDatacenter.put(datacenterId,
                    this.accessCountByDatacenter.getOrDefault(datacenterId, 0) + 1);
        }
        
        public void updateFileSize(int newSizeMB) {
            this.sizeMB = newSizeMB;
        }
    }

    static class RegionInfo {
        int datacenterId;
        String region;
        double costPerCloudlet;
        String typeDc;
        int hostCount;
        int storageCapacity;
        int storageLibre;
        int used;
        int vmCount;
        int vmPerHost;
        List<Vm> vmList;

        public RegionInfo(int datacenterId, String region, double costPerCloudlet, String typeDc, int hostCount, int storageCapacity) {
            this.datacenterId = datacenterId;
            this.region = region;
            this.costPerCloudlet = costPerCloudlet;
            this.typeDc = typeDc;
            this.hostCount = hostCount;
            this.storageCapacity = storageCapacity;
            this.storageLibre = storageCapacity;
            this.used = 0;
            this.vmCount = hostCount * VMS_PER_HOST;
            this.vmPerHost = VMS_PER_HOST;
            this.vmList = new ArrayList<>();
        }

        public void display() {
            System.out.printf(Locale.US, "%-12d | %-12s | %-16.3f | %-10s | %-10d | %-15d | %-15d | %-15d | %-10d | %-10d%n",
                    datacenterId, region, costPerCloudlet, typeDc, hostCount,
                    storageCapacity, storageLibre, used,
                    vmCount, vmPerHost);
        }
    }

    private static void uploadToGoogleDrive() {
        System.out.println("\n⚠️ Google Drive désactivé sur GitHub Actions. Fichiers sauvegardés localement.");
    }
    
    // ==================== MÉTHODES DE GÉNÉRATION CSV ====================
    
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
        
        Path path = Paths.get(CSV_OPTIMIZATION_INPUT_PATH);
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
        System.out.println("✓ Généré: " + CSV_OPTIMIZATION_INPUT_PATH);
        
        generateOptimizationConfigJSON(fichiers, datacenterInfos, dcidToRegion);
    }
    
    private static void generateOptimizationConfigJSON(List<ReplicaData> fichiers, 
                                                        List<RegionInfo> datacenterInfos,
                                                        Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_OPTIMIZATION_CONFIG_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        
        lines.add("{");
        lines.add("  \"optimization_config\": {");
        lines.add("    \"algorithm\": \"SPEA2_NSGAII\",");
        lines.add("    \"objectives\": [\"minimize_latency\", \"minimize_cost\", \"maximize_availability\", \"balance_load\"],");
        lines.add("    \"constraints\": {");
        lines.add("      \"max_replicas_per_file\": 5,");
        lines.add("      \"min_replicas_per_file\": 2,");
        int maxStorage = datacenterInfos.isEmpty() ? 1000000 : datacenterInfos.get(0).storageCapacity;
        lines.add("      \"max_storage_per_datacenter_mb\": " + maxStorage + ",");
        lines.add("      \"max_latency_ms\": 200");
        lines.add("    },");
        lines.add("    \"datacenters\": [");
        
        for (int i = 0; i < datacenterInfos.size(); i++) {
            RegionInfo dc = datacenterInfos.get(i);
            lines.add("      {");
            lines.add("        \"id\": " + dc.datacenterId + ",");
            lines.add("        \"region\": \"" + dc.region + "\",");
            lines.add("        \"type\": \"" + dc.typeDc + "\",");
            lines.add("        \"storage_capacity_mb\": " + dc.storageCapacity + ",");
            lines.add("        \"storage_used_mb\": " + dc.used + ",");
            lines.add("        \"storage_free_mb\": " + dc.storageLibre + ",");
            lines.add("        \"cost_per_gb\": " + String.format(Locale.US, "%.4f", dc.costPerCloudlet * 10) + "");
            lines.add("      }" + (i < datacenterInfos.size() - 1 ? "," : ""));
        }
        
        lines.add("    ],");
        lines.add("    \"parameters\": {");
        lines.add("      \"population_size\": 100,");
        lines.add("      \"generations\": 200,");
        lines.add("      \"crossover_probability\": 0.9,");
        lines.add("      \"mutation_probability\": 0.1,");
        lines.add("      \"tournament_size\": 2");
        lines.add("    }");
        lines.add("  }");
        lines.add("}");
        
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_OPTIMIZATION_CONFIG_PATH);
    }

    private static void generateReplicationLogCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_REPLICATION_LOG_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,PopularityScore,NewReplicasAdded,NewDatacenters,ReplicationStatus");
        for (ReplicaData file : fichiers) {
            int newReplicasAdded = Math.max(0, file.datacenterIds.size() - 2);
            String newDatacenters = "";
            if (newReplicasAdded > 0 && file.datacenterIds.size() >= 2) {
                newDatacenters = file.datacenterIds.stream()
                    .skip(2)
                    .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                    .collect(Collectors.joining("; "));
            }
            lines.add(String.format("%d,%d,%d,%d,%s,%s",
                file.dataID, file.sizeMB, file.userAccess.size(), 
                newReplicasAdded, newDatacenters.isEmpty() ? "None" : newDatacenters, 
                newReplicasAdded > 0 ? "REPLICATED" : "NOT_REPLICATED"));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_REPLICATION_LOG_PATH);
    }

    private static void generateDatacenterInfoAfterReplicationCSV(List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_DATACENTER_INFO_AFTER_REPLICATION_PATH);
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
        System.out.println("✓ Généré: " + CSV_DATACENTER_INFO_AFTER_REPLICATION_PATH);
    }

    private static void generateFileInfoAfterReplicationCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_FILE_INFO_AFTER_REPLICATION_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,SizeMB,Importance,TotalReplicaCount,OriginalReplicas,NewReplicas,AllReplicaLocations,UserAccessCount");
        for (ReplicaData fichier : fichiers) {
            String originalReplicas = fichier.datacenterIds.stream().limit(2)
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining("; "));
            String newReplicas = fichier.datacenterIds.size() > 2 ? 
                fichier.datacenterIds.stream().skip(2).map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")").collect(Collectors.joining("; ")) : "None";
            String allReplicas = fichier.datacenterIds.stream().map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")").collect(Collectors.joining("; "));
            lines.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%s,%s,%d",
                fichier.dataID, fichier.sizeMB, fichier.importance,
                fichier.datacenterIds.size(), originalReplicas, newReplicas, allReplicas, fichier.userAccess.size()));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_FILE_INFO_AFTER_REPLICATION_PATH);
    }

    private static void generateFragmentInfoAfterReplicationCSV() throws IOException {
        Path path = Paths.get(CSV_FRAGMENT_INFO_AFTER_REPLICATION_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FragmentID,FileID,SizeMB,VM_ID,DatacenterID,Region,IsNewReplica");
        for (Map.Entry<Integer, Map<Integer, List<FragmentMetadata>>> fileEntry : fileCopiesMap.entrySet()) {
            int dcCount = 0;
            for (Map.Entry<Integer, List<FragmentMetadata>> dcEntry : fileEntry.getValue().entrySet()) {
                int dcId = dcEntry.getKey();
                String region = getRegionForDatacenter(dcId);
                for (FragmentMetadata fragment : dcEntry.getValue()) {
                    lines.add(String.format("%d,%d,%d,%d,%d,%s,%s",
                            fragment.fragmentId, fragment.originalFileId, fragment.sizeMB,
                            fragment.vmId, fragment.dcId, region, dcCount >= 2 ? "YES" : "NO"));
                }
                dcCount++;
            }
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_FRAGMENT_INFO_AFTER_REPLICATION_PATH);
    }

    private static void generatePlacementReportCSV(List<ReplicaData> fichiers, Map<Integer, String> dcidToRegion) throws IOException {
        Path path = Paths.get(CSV_PLACEMENT_REPORT_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("FileID,FileSizeMB,UserAccessCount,OriginalDatacenters,NewDatacenters,TotalDatacenters");
        for (ReplicaData file : fichiers) {
            String originalDCs = file.datacenterIds.stream().limit(2)
                .map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")")
                .collect(Collectors.joining("; "));
            String newDCs = file.datacenterIds.size() > 2 ? 
                file.datacenterIds.stream().skip(2).map(dcId -> dcId + "(" + dcidToRegion.get(dcId) + ")").collect(Collectors.joining("; ")) : "None";
            lines.add(String.format("%d,%d,%d,%s,%s,%d", 
                file.dataID, file.sizeMB, file.userAccess.size(), originalDCs, newDCs, file.datacenterIds.size()));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_PLACEMENT_REPORT_PATH);
    }

    private static void generateSystemStateAfterReplicationCSV(List<ReplicaData> fichiers, 
                                                                List<User> users,
                                                                List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_SYSTEM_STATE_AFTER_REPLICATION_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("Timestamp," + System.currentTimeMillis());
        lines.add("TotalFiles," + fichiers.size());
        lines.add("TotalReplicas," + fichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum());
        lines.add("FilesWithExtraReplicas," + fichiers.stream().filter(f -> f.datacenterIds.size() > 2).count());
        lines.add("");
        lines.add("DatacenterID,Region,StorageUsedMB,StorageFreeMB,UsagePercentage");
        for (RegionInfo dc : datacenterInfos) {
            double usagePercentage = (double) dc.used / dc.storageCapacity * 100;
            lines.add(String.format(Locale.US, "%d,%s,%d,%d,%.2f%%", 
                dc.datacenterId, dc.region, dc.used, dc.storageLibre, usagePercentage));
        }
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_SYSTEM_STATE_AFTER_REPLICATION_PATH);
    }

    private static void generateSimulationSummaryCSV(List<ReplicaData> fichiers, List<User> users,
                                                      List<RegionInfo> datacenterInfos) throws IOException {
        Path path = Paths.get(CSV_SIMULATION_SUMMARY_PATH);
        Files.createDirectories(path.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("SimulationDate," + new Date());
        lines.add("TotalFiles," + fichiers.size());
        lines.add("TotalUsers," + users.size());
        lines.add("TotalDatacenters," + datacenterInfos.size());
        lines.add("TotalStorageCapacityMB," + datacenterInfos.stream().mapToInt(dc -> dc.storageCapacity).sum());
        lines.add("TotalStorageUsedMB," + datacenterInfos.stream().mapToLong(dc -> dc.used).sum());
        lines.add("TotalReplicas," + fichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum());
        lines.add("AverageReplicasPerFile," + String.format(Locale.US, "%.2f", 
            (double) fichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum() / fichiers.size()));
        Files.write(path, lines, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("✓ Généré: " + CSV_SIMULATION_SUMMARY_PATH);
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

    private static void applyPopularityBasedReplication(List<ReplicaData> fichiers,
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

    private static void createOutputDirectoryIfNotExists() throws IOException {
        Path path = Paths.get(CSV_OUTPUT_DIR);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
            System.out.println("✓ Dossier créé: " + CSV_OUTPUT_DIR);
        }
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

    private static int createDatacenter(String name, String type) {
        switch (type) { 
            case "grand": return 100; 
            case "moyen": return 60; 
            case "mini": return 30; 
            default: return 40; 
        }
    }

    private static AccessType getRandomAccessType(Random random) {
        int val = random.nextInt(3);
        return val == 0 ? AccessType.READ : (val == 1 ? AccessType.WRITE : AccessType.READ_WRITE);
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

    // ==================== MAIN ====================
    
    public static void main(String[] args) {
        try {
            createOutputDirectoryIfNotExists();
            
            System.out.println("=========================================");
            System.out.println("SIMULATION DE RÉPLICATION DE DONNÉES");
            System.out.println("=========================================");
            System.out.println("Démarrage: " + new Date());
            System.out.println("Dossier local: " + CSV_OUTPUT_DIR);
            System.out.println();
            
            Random seededRandom = new Random(RANDOM_SEED);
            CloudSim.init(1, Calendar.getInstance(), false);

            // Configuration des datacenters simplifiée
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
            List<Integer> allDatacenterIds = Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30);

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

            // Distribution des droits d'accès
            distributeFilesWithAccessRights(fichiers, users, dcidToRegion, datacenterMap, seededRandom);
            
            // ==================== PHASE 1: ÉTAT INITIAL ====================
            System.out.println("\n=== PHASE 1: GÉNÉRATION FICHIERS AVANT DRIFT ===");
            generateUserAccessBeforeCSV(users);
            generateDatacenterInfoBeforeCSV(datacenterInfos);
            generateFileInfoBeforeCSV(fichiers, dcidToRegion);
            generateVmInfoBeforeCSV(datacenterInfos);
            generateFragmentInfoBeforeCSV();
            
            // ==================== PHASE 2: DRIFTS ====================
            System.out.println("\n=== PHASE 2: APPLICATION DES DRIFTS ===");
            Map<Integer, Integer> originalSizes = new HashMap<>();
            Map<Integer, Map<Integer, AccessType>> originalAccess = new HashMap<>();
            List<User> originalUsers = new ArrayList<>();
            for (User user : users) {
                User copy = new User(user.id, user.location);
                copy.fileAccess.putAll(user.fileAccess);
                originalUsers.add(copy);
            }
            
            applyFileSizeDrift(fichiers, seededRandom, datacenterMap, originalSizes);
            applyUserAccessDrift(users, fichiers, seededRandom, dcidToRegion, originalAccess);
            
            generateDriftImpactReport(fichiers, originalSizes, originalAccess);
            generateUserAccessDriftLog(users, originalUsers);
            generateUserAccessAfterDriftCSV(users);
            generateDatacenterInfoAfterDriftCSV(datacenterInfos);
            generateFileInfoAfterDriftCSV(fichiers, dcidToRegion);
            generateFragmentInfoAfterDriftCSV();
            generateSystemStateAfterDriftCSV(fichiers, users, datacenterInfos);
            
            // ==================== PHASE 3: DONNÉES OPTIMISATION ====================
            generateOptimizationInputData(fichiers, users, datacenterInfos, dcidToRegion);
            
            // ==================== PHASE 4: RÉPLICATION ====================
            System.out.println("\n=== PHASE 4: RÉPLICATION BASÉE SUR POPULARITÉ ===");
            applyPopularityBasedReplication(fichiers, datacenterMap, dcidToRegion, seededRandom);
            
            generateReplicationLogCSV(fichiers, dcidToRegion);
            generateDatacenterInfoAfterReplicationCSV(datacenterInfos);
            generateFileInfoAfterReplicationCSV(fichiers, dcidToRegion);
            generateFragmentInfoAfterReplicationCSV();
            generatePlacementReportCSV(fichiers, dcidToRegion);
            generateSystemStateAfterReplicationCSV(fichiers, users, datacenterInfos);
            generateSimulationSummaryCSV(fichiers, users, datacenterInfos);
            
            // ==================== PHASE 5: UPLOAD ====================
            uploadToGoogleDrive();
            
            // ==================== RAPPORT FINAL ====================
            System.out.println("\n=========================================");
            System.out.println("✅ SIMULATION TERMINÉE AVEC SUCCÈS");
            System.out.println("=========================================");
            System.out.println("📁 Dossier local: " + CSV_OUTPUT_DIR);
            System.out.println("=========================================");

            CloudSim.stopSimulation();
            
        } catch (Exception e) {
            System.err.println("❌ ERREUR: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
