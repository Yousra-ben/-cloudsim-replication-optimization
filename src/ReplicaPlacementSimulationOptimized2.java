import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Locale;

/**
 * ReplicaPlacementSimulationOptimized2 — Version finale corrigée
 *
 * FLUX PAR DRIFT i (i = 1..120) :
 * ┌────────────────────────────────────────────────────────────────┐
 * │  [A] Générer 3 CSV avant drift i                               │
 * │       drift_NNNN/driftNNNN_avant_datacenter_information.csv    │
 * │       drift_NNNN/driftNNNN_avant_file_information.csv          │
 * │       drift_NNNN/driftNNNN_avant_useraccess_details.csv        │
 * │                                                                │
 * │  [B] Appliquer le drift (tailles + accès utilisateurs)         │
 * │                                                                │
 * │  [C] Optimisation SPEA2 → 3 CSV après SPEA2                   │
 * │  [D] Optimisation NSGA-II → 3 CSV après NSGA-II               │
 * │       (ces 3 CSV = état appliqué dans CloudSim avant drift i+1)│
 * │                                                                │
 * │  [E] L'état NSGA-II devient l'entrée du drift i+1             │
 * └────────────────────────────────────────────────────────────────┘
 *
 * POUR UTILISER VOS VRAIES CLASSES SPEA2/NSGA-II :
 *   Remplacez les lignes marquées "TODO" dans main() par :
 *     ReplicaOptimizer spea2 = new SPEA2Optimizer();   // votre classe
 *     ReplicaOptimizer nsga2 = new NSGA2Optimizer();   // votre classe
 */
public class ReplicaPlacementSimulationOptimized2 {

    // ── Constantes ─────────────────────────────────────────────────────────────
    private static final long   RANDOM_SEED  = 12345L;
    private static final int    VMS_PER_HOST = 5;
    private static final int    N_DRIFTS     = 120;

    private static final String CSV_OUTPUT_DIR =
        System.getProperty("user.home") +
        (System.getProperty("os.name").toLowerCase().contains("win")
            ? "\\Desktop\\testcodedreplicationcsv\\"
            : "/results/");

    // ── Maps latence/région ────────────────────────────────────────────────────
    private static final Map<String, String> BEST_DC_MAP = createBestDcMap();

    // ── État VMs ───────────────────────────────────────────────────────────────
    private static Map<Integer, Integer> vmStorageFree = new HashMap<>();
    private static Map<Integer, Integer> vmToDcMap     = new HashMap<>();

    // ══════════════════════════════════════════════════════════════════════════
    // TYPES
    // ══════════════════════════════════════════════════════════════════════════

    enum AccessType { READ, WRITE, READ_WRITE }

    static class User {
        int id;
        String location;
        Map<Integer, AccessType> fileAccess      = new HashMap<>();
        Map<Integer, Integer>    fileToDatacenter = new HashMap<>();

        User(int id, String location) { this.id = id; this.location = location; }

        void addFileAccess(int fileId, AccessType access, int dcId) {
            fileAccess.put(fileId, access);
            fileToDatacenter.put(fileId, dcId);
        }
    }

    static class ReplicaData {
        int dataID, sizeMB, importance, popularite;
        List<Integer>            datacenterIds;
        Map<Integer, AccessType> userAccess           = new HashMap<>();
        Map<Integer, Integer>    userToDatacenter     = new HashMap<>();
        Map<Integer, Integer>    accessCountByDc      = new HashMap<>();

        ReplicaData(int id, int size, int imp, int pop, List<Integer> dcIds) {
            dataID = id; sizeMB = size; importance = imp; popularite = pop;
            datacenterIds = dcIds;
            for (int dc : dcIds) accessCountByDc.put(dc, 0);
        }
        void addUserAccess(int uid, AccessType acc, int dcId) {
            userAccess.put(uid, acc);
            userToDatacenter.put(uid, dcId);
            accessCountByDc.merge(dcId, 1, Integer::sum);
        }
        void updateSize(int s) { sizeMB = s; }
    }

    static class RegionInfo {
        int datacenterId, hostCount, storageCapacity, storageLibre, used, vmCount;
        String region, typeDc;
        double costPerCloudlet;

        RegionInfo(int id, String reg, double cost, String type, int hosts, int storage) {
            datacenterId = id; region = reg; costPerCloudlet = cost;
            typeDc = type; hostCount = hosts; storageCapacity = storage;
            storageLibre = storage; used = 0;
            vmCount = hosts * VMS_PER_HOST;
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // INTERFACE OPTIMISEUR
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Interface à implémenter dans vos classes SPEA2Optimizer / NSGA2Optimizer.
     * La méthode optimize() reçoit l'état POST-drift et retourne l'état optimisé.
     */
    interface ReplicaOptimizer {
        /**
         * @param fichiers      fichiers avec répliques actuelles (post-drift)
         * @param dcMap         map dcId → RegionInfo
         * @param dcidToRegion  map dcId → nom région
         * @param users         utilisateurs
         * @param rng           générateur aléatoire
         * @return fichiers avec placement optimisé
         */
        List<ReplicaData> optimize(
            List<ReplicaData>        fichiers,
            Map<Integer, RegionInfo> dcMap,
            Map<Integer, String>     dcidToRegion,
            List<User>               users,
            Random                   rng
        );
    }

    // ── SPEA2 de secours ───────────────────────────────────────────────────────
    static class SPEA2FallbackOptimizer implements ReplicaOptimizer {
        @Override
        public List<ReplicaData> optimize(List<ReplicaData> fichiers,
                Map<Integer, RegionInfo> dcMap, Map<Integer, String> dcidToRegion,
                List<User> users, Random rng) {

            System.out.println("    [SPEA2 fallback] priorisation par popularité...");
            // Trier par accès décroissant
            List<ReplicaData> sorted = new ArrayList<>(fichiers);
            sorted.sort((a, b) -> b.userAccess.size() - a.userAccess.size());

            // Ajouter une réplique aux 30% les plus populaires
            int threshold = sorted.size() / 3;
            int added = 0;
            List<RegionInfo> dcLibre = new ArrayList<>(dcMap.values());
            dcLibre.sort((a, b) -> b.storageLibre - a.storageLibre);

            for (int i = 0; i < threshold && i < dcLibre.size(); i++) {
                ReplicaData file = sorted.get(i);
                for (RegionInfo dc : dcLibre) {
                    if (!file.datacenterIds.contains(dc.datacenterId)
                            && dc.storageLibre >= file.sizeMB) {
                        file.datacenterIds.add(dc.datacenterId);
                        dc.storageLibre -= file.sizeMB;
                        dc.used         += file.sizeMB;
                        added++;
                        break;
                    }
                }
            }
            System.out.printf("    [SPEA2 fallback] %d répliques ajoutées%n", added);
            return sorted;
        }
    }

    // ── NSGA-II de secours ────────────────────────────────────────────────────
    static class NSGA2FallbackOptimizer implements ReplicaOptimizer {
        @Override
        public List<ReplicaData> optimize(List<ReplicaData> fichiers,
                Map<Integer, RegionInfo> dcMap, Map<Integer, String> dcidToRegion,
                List<User> users, Random rng) {

            System.out.println("    [NSGA-II fallback] équilibrage de charge...");
            double totalUsed = dcMap.values().stream().mapToInt(d -> d.used).sum();
            double avg       = totalUsed / dcMap.size();

            List<RegionInfo> over  = dcMap.values().stream()
                .filter(d -> d.used > avg * 1.2).collect(Collectors.toList());
            List<RegionInfo> under = dcMap.values().stream()
                .filter(d -> d.used < avg * 0.8).collect(Collectors.toList());

            int moved = 0;
            for (ReplicaData file : fichiers) {
                if (moved >= 50) break;
                for (RegionInfo o : over) {
                    if (!file.datacenterIds.contains(o.datacenterId)) continue;
                    for (RegionInfo u : under) {
                        if (!file.datacenterIds.contains(u.datacenterId)
                                && u.storageLibre >= file.sizeMB) {
                            file.datacenterIds.remove(Integer.valueOf(o.datacenterId));
                            file.datacenterIds.add(u.datacenterId);
                            o.storageLibre += file.sizeMB; o.used -= file.sizeMB;
                            u.storageLibre -= file.sizeMB; u.used += file.sizeMB;
                            moved++;
                            break;
                        }
                    }
                    if (moved > 0) break;
                }
            }
            System.out.printf("    [NSGA-II fallback] %d répliques déplacées%n", moved);
            return fichiers;
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GÉNÉRATION DES 3 FICHIERS CSV
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Génère les 3 CSV pour un état donné.
     * @param prefixe  ex: "drift0001_avant" ou "drift0001_apres_nsga2"
     * @param dossier  chemin du sous-dossier (créé si absent)
     */
    private static void generer3CSV(
            String prefixe, String dossier,
            List<RegionInfo>     dcInfos,
            List<ReplicaData>    fichiers,
            List<User>           users,
            Map<Integer, String> dcidToRegion) throws IOException {

        Files.createDirectories(Paths.get(dossier));

        // ── 1. datacenter_information.csv ────────────────────────────────────
        String pDC = dossier + prefixe + "_datacenter_information.csv";
        List<String> lDC = new ArrayList<>();
        lDC.add("DatacenterID,Region,TypeDC,HostCount,StorageCapacityMB," +
                "StorageUsedMB,StorageFreeMB,UsagePercentage,VMCount,VMPerHost,CostPerGB");
        for (RegionInfo r : dcInfos) {
            double pct = (double) r.used / r.storageCapacity * 100;
            lDC.add(String.format(Locale.US,
                "%d,%s,%s,%d,%d,%d,%d,%.2f%%,%d,%d,%.4f",
                r.datacenterId, r.region, r.typeDc, r.hostCount,
                r.storageCapacity, r.used, r.storageLibre, pct,
                r.vmCount, VMS_PER_HOST, r.costPerCloudlet * 10));
        }
        Files.write(Paths.get(pDC), lDC, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("  ✓ " + Paths.get(pDC).getFileName());

        // ── 2. file_information.csv ───────────────────────────────────────────
        String pFile = dossier + prefixe + "_file_information.csv";
        List<String> lFile = new ArrayList<>();
        lFile.add("FileID,SizeMB,Importance,ReplicaCount,ReplicaLocations," +
                  "UserAccessCount,ReadCount,WriteCount,ReadWriteCount");
        for (ReplicaData f : fichiers) {
            String locs = f.datacenterIds.stream()
                .map(d -> d + "(" + dcidToRegion.getOrDefault(d, "?") + ")")
                .collect(Collectors.joining("; "));
            int r = 0, w = 0, rw = 0;
            for (AccessType a : f.userAccess.values()) {
                if (a == AccessType.READ) r++;
                else if (a == AccessType.WRITE) w++;
                else rw++;
            }
            lFile.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%d,%d,%d,%d",
                f.dataID, f.sizeMB, f.importance, f.datacenterIds.size(),
                locs, f.userAccess.size(), r, w, rw));
        }
        Files.write(Paths.get(pFile), lFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("  ✓ " + Paths.get(pFile).getFileName());

        // ── 3. useraccess_details.csv ─────────────────────────────────────────
        String pUser = dossier + prefixe + "_useraccess_details.csv";
        List<String> lUser = new ArrayList<>();
        lUser.add("UserID,UserLocation,FileID,AccessType,DatacenterAccessed,DatacenterRegion");
        for (User u : users) {
            for (Map.Entry<Integer, AccessType> e : u.fileAccess.entrySet()) {
                int fid  = e.getKey();
                int dcId = u.fileToDatacenter.getOrDefault(fid, -1);
                lUser.add(String.format("%d,%s,%d,%s,%d,%s",
                    u.id, u.location, fid, e.getValue(),
                    dcId, dcidToRegion.getOrDefault(dcId, "Unknown")));
            }
        }
        Files.write(Paths.get(pUser), lUser, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("  ✓ " + Paths.get(pUser).getFileName());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // DRIFTS
    // ══════════════════════════════════════════════════════════════════════════

    private static void applyFileSizeDrift(List<ReplicaData> fichiers, Random rng,
                                           Map<Integer, RegionInfo> dcMap,
                                           Map<Integer, Integer> origSizes) {
        int mod = 0;
        for (ReplicaData f : fichiers) {
            if (rng.nextDouble() < 0.3) {
                int old = f.sizeMB;
                origSizes.put(f.dataID, old);
                int nw = Math.max(500, Math.min(20000, (int)(old * (0.5 + rng.nextDouble()))));
                int diff = nw - old;
                if (diff > 0) {
                    int avail = f.datacenterIds.stream()
                        .mapToInt(d -> dcMap.get(d).storageLibre).sum();
                    if (avail >= diff) {
                        int rem = diff;
                        for (int d : f.datacenterIds) {
                            RegionInfo dc = dcMap.get(d);
                            int add = Math.min(rem, dc.storageLibre);
                            dc.storageLibre -= add; dc.used += add; rem -= add;
                            if (rem == 0) break;
                        }
                        f.updateSize(nw); mod++;
                    }
                } else if (diff < 0) {
                    for (int d : f.datacenterIds) {
                        RegionInfo dc = dcMap.get(d);
                        int rel = Math.min(-diff, dc.used);
                        dc.storageLibre += rel; dc.used -= rel;
                    }
                    f.updateSize(nw); mod++;
                }
            }
        }
        System.out.printf("  Drift taille : %d fichiers modifiés%n", mod);
    }

    private static void applyUserAccessDrift(List<User> users, List<ReplicaData> fichiers,
                                             Random rng, Map<Integer, String> dcidToRegion,
                                             Map<Integer, Map<Integer, AccessType>> origAccess) {
        for (User u : users)
            origAccess.put(u.id, new HashMap<>(u.fileAccess));

        int add = 0, del = 0;
        for (User user : users) {
            if (rng.nextDouble() < 0.4) {
                List<Integer> cur = new ArrayList<>(user.fileAccess.keySet());
                int toDel = Math.min(cur.size(), 1 + rng.nextInt(3));
                for (int i = 0; i < toDel && !cur.isEmpty(); i++) {
                    int fid = cur.get(rng.nextInt(cur.size()));
                    user.fileAccess.remove(fid);
                    user.fileToDatacenter.remove(fid);
                    fichiers.stream().filter(f -> f.dataID == fid).findFirst()
                            .ifPresent(f -> f.userAccess.remove(user.id));
                    cur.remove(Integer.valueOf(fid));
                    del++;
                }
                int toAdd = 1 + rng.nextInt(3);
                for (int i = 0; i < toAdd; i++) {
                    ReplicaData file = fichiers.get(rng.nextInt(fichiers.size()));
                    if (!user.fileAccess.containsKey(file.dataID)) {
                        AccessType acc = getRandomAcc(rng);
                        String best = BEST_DC_MAP.getOrDefault(user.location, "Europe");
                        int dc = file.datacenterIds.stream()
                            .filter(d -> best.equals(dcidToRegion.get(d)))
                            .findFirst().orElse(file.datacenterIds.get(0));
                        user.addFileAccess(file.dataID, acc, dc);
                        file.addUserAccess(user.id, acc, dc);
                        add++;
                    }
                }
            }
        }
        System.out.printf("  Drift accès : +%d / -%d%n", add, del);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // COPIES PROFONDES (SPEA2 et NSGA-II travaillent sur des copies indépendantes)
    // ══════════════════════════════════════════════════════════════════════════

    private static List<ReplicaData> deepCopyFichiers(List<ReplicaData> src) {
        List<ReplicaData> out = new ArrayList<>();
        for (ReplicaData f : src) {
            ReplicaData c = new ReplicaData(f.dataID, f.sizeMB, f.importance,
                                            f.popularite, new ArrayList<>(f.datacenterIds));
            c.userAccess.putAll(f.userAccess);
            c.userToDatacenter.putAll(f.userToDatacenter);
            c.accessCountByDc.putAll(f.accessCountByDc);
            out.add(c);
        }
        return out;
    }

    private static List<RegionInfo> deepCopyDC(List<RegionInfo> src) {
        List<RegionInfo> out = new ArrayList<>();
        for (RegionInfo r : src) {
            RegionInfo c = new RegionInfo(r.datacenterId, r.region, r.costPerCloudlet,
                                          r.typeDc, r.hostCount, r.storageCapacity);
            c.storageLibre = r.storageLibre;
            c.used         = r.used;
            out.add(c);
        }
        return out;
    }

    private static Map<Integer, RegionInfo> dcListToMap(List<RegionInfo> list) {
        Map<Integer, RegionInfo> m = new HashMap<>();
        for (RegionInfo r : list) m.put(r.datacenterId, r);
        return m;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // UTILITAIRES
    // ══════════════════════════════════════════════════════════════════════════

    private static AccessType getRandomAcc(Random r) {
        int v = r.nextInt(3);
        return v == 0 ? AccessType.READ : (v == 1 ? AccessType.WRITE : AccessType.READ_WRITE);
    }

    private static String regionForDC(int dcId) {
        if (dcId >=  1 && dcId <=  6) return "Europe";
        if (dcId >=  7 && dcId <= 12) return "USA";
        if (dcId >= 13 && dcId <= 18) return "Asie";
        if (dcId >= 19 && dcId <= 24) return "Afrique";
        if (dcId >= 25 && dcId <= 30) return "AmeriqueSud";
        return "Unknown";
    }

    private static Map<String, String> createBestDcMap() {
        Map<String, String> m = new HashMap<>();
        m.put("Paris (France)", "Europe");   m.put("Berlin (Allemagne)", "Europe");
        m.put("Madrid (Espagne)", "Europe"); m.put("New York (USA)", "USA");
        m.put("San Francisco (USA)", "USA"); m.put("Tokyo (Japon)", "Asie");
        m.put("Mumbai (Inde)", "Asie");      m.put("Nairobi (Kenya)", "Afrique");
        m.put("Lagos (Nigeria)", "Afrique"); m.put("Le Caire (Egypte)", "Afrique");
        return m;
    }

    private static void distributeAccess(List<ReplicaData> fichiers, List<User> users,
                                         Map<Integer, String> dcidToRegion,
                                         Map<Integer, RegionInfo> dcMap, Random rng) {
        for (User u : users) {
            for (int i = 0; i < 1 + rng.nextInt(5); i++) {
                ReplicaData f = fichiers.get(rng.nextInt(fichiers.size()));
                if (!u.fileAccess.containsKey(f.dataID)) {
                    AccessType acc = getRandomAcc(rng);
                    String best = BEST_DC_MAP.getOrDefault(u.location, "Europe");
                    int dc = f.datacenterIds.stream()
                        .filter(d -> best.equals(dcidToRegion.get(d)))
                        .findFirst().orElse(f.datacenterIds.get(0));
                    u.addFileAccess(f.dataID, acc, dc);
                    f.addUserAccess(u.id, acc, dc);
                }
            }
        }
        // Garantir ≥ 30 accès par fichier
        for (ReplicaData f : fichiers) {
            int iter = 0;
            while (f.userAccess.size() < 30 && iter++ < 10000) {
                User u = users.get(rng.nextInt(users.size()));
                if (!f.userAccess.containsKey(u.id)) {
                    AccessType acc = getRandomAcc(rng);
                    String best = BEST_DC_MAP.getOrDefault(u.location, "Europe");
                    int dc = f.datacenterIds.stream()
                        .filter(d -> best.equals(dcidToRegion.get(d)))
                        .findFirst().orElse(f.datacenterIds.get(0));
                    u.addFileAccess(f.dataID, acc, dc);
                    f.addUserAccess(u.id, acc, dc);
                }
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MAIN
    // ══════════════════════════════════════════════════════════════════════════

    public static void main(String[] args) {
        try {
            Files.createDirectories(Paths.get(CSV_OUTPUT_DIR));
            System.out.println("=========================================");
            System.out.println("  SIMULATION — " + N_DRIFTS + " DRIFTS");
            System.out.println("  SPEA2 + NSGA-II | 30 DC | 500 fichiers");
            System.out.println("=========================================");
            System.out.println("Démarrage : " + new Date());
            System.out.println("Dossier   : " + CSV_OUTPUT_DIR + "\n");

            Random rng = new Random(RANDOM_SEED);
            CloudSim.init(1, Calendar.getInstance(), false);

            // ── Configuration des 30 datacenters ──────────────────────────────
            String[] REGIONS = {
                "Europe","Europe","Europe","Europe","Europe","Europe",
                "USA","USA","USA","USA","USA","USA",
                "Asie","Asie","Asie","Asie","Asie","Asie",
                "Afrique","Afrique","Afrique","Afrique","Afrique","Afrique",
                "AmeriqueSud","AmeriqueSud","AmeriqueSud","AmeriqueSud","AmeriqueSud","AmeriqueSud"};
            String[] TYPES = {
                "grand","grand","moyen","moyen","mini","mini",
                "grand","grand","grand","moyen","moyen","mini",
                "grand","grand","moyen","moyen","mini","mini",
                "grand","moyen","moyen","mini","mini","mini",
                "grand","moyen","moyen","mini","mini","mini"};
            double[] COSTS = {
                0.06,0.06,0.06,0.06,0.06,0.06,
                0.05,0.05,0.05,0.05,0.05,0.05,
                0.045,0.045,0.045,0.045,0.045,0.045,
                0.035,0.035,0.035,0.035,0.035,0.035,
                0.04,0.04,0.04,0.04,0.04,0.04};

            List<RegionInfo>         dcInfosInit = new ArrayList<>();
            Map<Integer, String>     dcidToRegion = new HashMap<>();

            for (int i = 1; i <= 30; i++) {
                int hosts   = TYPES[i-1].equals("grand") ? 100 : TYPES[i-1].equals("moyen") ? 60 : 30;
                int storage = hosts * 100_000;
                RegionInfo ri = new RegionInfo(i, REGIONS[i-1], COSTS[i-1], TYPES[i-1], hosts, storage);
                dcInfosInit.add(ri);
                dcidToRegion.put(i, REGIONS[i-1]);
            }

            // ── 500 fichiers ───────────────────────────────────────────────────
            int[] SIZES = {1500, 2500, 5000, 7500, 10000};
            List<ReplicaData> fichiersInit = new ArrayList<>();
            for (int i = 1; i <= 500; i++) {
                int dc1 = ((i-1) % 30) + 1;
                int dc2 = ((i-1) + 15) % 30 + 1;
                fichiersInit.add(new ReplicaData(i, SIZES[(i-1)%5], (i-1)%5+1, 0,
                    new ArrayList<>(Arrays.asList(dc1, dc2))));
            }
            // Mise à jour stockage DC
            Map<Integer, RegionInfo> dcMapInit = dcListToMap(dcInfosInit);
            for (ReplicaData f : fichiersInit)
                for (int d : f.datacenterIds) {
                    dcMapInit.get(d).used        += f.sizeMB;
                    dcMapInit.get(d).storageLibre -= f.sizeMB;
                }

            // ── 1000 utilisateurs ──────────────────────────────────────────────
            String[] LOCS  = {"Paris (France)","Berlin (Allemagne)","Madrid (Espagne)",
                               "New York (USA)","San Francisco (USA)","Tokyo (Japon)",
                               "Mumbai (Inde)","Nairobi (Kenya)","Lagos (Nigeria)","Le Caire (Egypte)"};
            int[]    CNTS  = {80,160,100,120,80,100,120,60,100,80};
            List<User> usersInit = new ArrayList<>();
            int uid = 1;
            for (int i = 0; i < LOCS.length; i++)
                for (int j = 0; j < CNTS[i]; j++)
                    usersInit.add(new User(uid++, LOCS[i]));

            distributeAccess(fichiersInit, usersInit, dcidToRegion, dcMapInit, rng);
            System.out.printf("Init : %d DC | %d fichiers | %d users%n%n",
                dcInfosInit.size(), fichiersInit.size(), usersInit.size());

            // ── Charger les optimiseurs ────────────────────────────────────────
            // TODO : Remplacez les fallbacks par vos vraies classes :
            //   ReplicaOptimizer spea2 = new SPEA2Optimizer();
            //   ReplicaOptimizer nsga2 = new NSGA2Optimizer();
            ReplicaOptimizer spea2 = new SPEA2FallbackOptimizer();
            ReplicaOptimizer nsga2 = new NSGA2FallbackOptimizer();

            // ── État courant (évolue drift par drift) ─────────────────────────
            List<ReplicaData> curFichiers = deepCopyFichiers(fichiersInit);
            List<RegionInfo>  curDCInfos  = deepCopyDC(dcInfosInit);
            List<User>        curUsers    = usersInit;  // partagé (drift modifie en place)

            // ══════════════════════════════════════════════════════════════════
            // BOUCLE PRINCIPALE : N_DRIFTS drifts
            // ══════════════════════════════════════════════════════════════════
            System.out.println("--- DÉBUT DES DRIFTS ---");
            int ok = 0;

            for (int driftId = 1; driftId <= N_DRIFTS; driftId++) {
                System.out.printf("%n══════════════════════════════════════%n");
                System.out.printf("  DRIFT %3d / %d%n", driftId, N_DRIFTS);
                System.out.printf("══════════════════════════════════════%n");

                String dossier = CSV_OUTPUT_DIR
                    + String.format("drift_%04d", driftId)
                    + File.separator;
                Files.createDirectories(Paths.get(dossier));

                String pref = String.format("drift%04d", driftId);
                Map<Integer, RegionInfo> curDCMap = dcListToMap(curDCInfos);

                try {
                    // ── [A] 3 CSV AVANT drift i ───────────────────────────────
                    System.out.println("\n[A] 3 CSV avant drift " + driftId);
                    generer3CSV(pref + "_avant", dossier,
                                curDCInfos, curFichiers, curUsers, dcidToRegion);

                    // ── [B] Application du drift ──────────────────────────────
                    System.out.println("\n[B] Application drift " + driftId);
                    Map<Integer, Integer>                  origSizes  = new HashMap<>();
                    Map<Integer, Map<Integer, AccessType>> origAccess = new HashMap<>();
                    applyFileSizeDrift(curFichiers, new Random(RANDOM_SEED + driftId),
                                       curDCMap, origSizes);
                    applyUserAccessDrift(curUsers, curFichiers,
                                         new Random(RANDOM_SEED + driftId + 1000),
                                         dcidToRegion, origAccess);

                    // ── [C] Optimisation SPEA2 ────────────────────────────────
                    System.out.println("\n[C] Optimisation SPEA2");
                    List<ReplicaData> fiS = deepCopyFichiers(curFichiers);
                    List<RegionInfo>  dcS = deepCopyDC(curDCInfos);
                    fiS = spea2.optimize(fiS, dcListToMap(dcS), dcidToRegion,
                                         curUsers, new Random(RANDOM_SEED + driftId));

                    System.out.println("  → 3 CSV après SPEA2");
                    generer3CSV(pref + "_apres_spea2", dossier,
                                dcS, fiS, curUsers, dcidToRegion);

                    // ── [D] Optimisation NSGA-II ──────────────────────────────
                    System.out.println("\n[D] Optimisation NSGA-II");
                    List<ReplicaData> fiN = deepCopyFichiers(curFichiers);
                    List<RegionInfo>  dcN = deepCopyDC(curDCInfos);
                    fiN = nsga2.optimize(fiN, dcListToMap(dcN), dcidToRegion,
                                         curUsers, new Random(RANDOM_SEED + driftId + 2000));

                    // ── [E] 3 CSV APRÈS optimisation → entrée CloudSim drift i+1
                    System.out.println("\n[E] 3 CSV après optimisation (CloudSim drift " + (driftId+1) + ")");
                    generer3CSV(pref + "_apres_optim_cloudsim", dossier,
                                dcN, fiN, curUsers, dcidToRegion);

                    // ── [F] Mettre à jour l'état pour le drift suivant ─────────
                    curFichiers = fiN;
                    curDCInfos  = dcN;
                    // curUsers a été modifié en place par applyUserAccessDrift

                    ok++;
                    System.out.printf("  ✅ Drift %d OK%n", driftId);

                } catch (Exception e) {
                    System.err.printf("  ❌ Drift %d échoué : %s%n", driftId, e.getMessage());
                    e.printStackTrace();
                }
            }

            // ── Résumé final ───────────────────────────────────────────────────
            String summaryPath = CSV_OUTPUT_DIR + "simulation_summary.csv";
            List<String> summary = new ArrayList<>();
            summary.add("SimulationDate," + new Date());
            summary.add("TotalDrifts," + N_DRIFTS);
            summary.add("DriftsOK," + ok);
            summary.add("TotalFiles," + curFichiers.size());
            summary.add("TotalUsers," + curUsers.size());
            summary.add("TotalDatacenters," + curDCInfos.size());
            summary.add("FinalTotalReplicas,"
                + curFichiers.stream().mapToInt(f -> f.datacenterIds.size()).sum());
            Files.write(Paths.get(summaryPath), summary, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            System.out.println("\n=========================================");
            System.out.printf("✅ TERMINÉ — %d/%d drifts réussis%n", ok, N_DRIFTS);
            System.out.println("   Résultats : " + CSV_OUTPUT_DIR);
            System.out.println("=========================================");

            CloudSim.stopSimulation();

        } catch (Exception e) {
            System.err.println("❌ ERREUR FATALE : " + e.getMessage());
            e.printStackTrace();
        }
    }
}
