import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.*;

import java.util.*;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  ReplicaPlacementFast  —  Version Optimisée 120+ drifts     ║
 * ╠══════════════════════════════════════════════════════════════╣
 * ║  PROBLÈME ORIGINAL : Java bloquait 300s/drift → 10h/120     ║
 * ║                                                              ║
 * ║  SOLUTION :                                                  ║
 * ║  • Java NON-BLOQUANT : 0 attente Python                      ║
 * ║  • Chaque drift → dossier isolé drift_N/                     ║
 * ║  • 6 CSV par drift :                                         ║
 * ║      drift_N/avant/datacenter_information.csv                ║
 * ║      drift_N/avant/file_information.csv                      ║
 * ║      drift_N/avant/user_access_details.csv                   ║
 * ║      drift_N/apres/datacenter_information.csv                ║
 * ║      drift_N/apres/file_information.csv                      ║
 * ║      drift_N/apres/user_access_details.csv                   ║
 * ║  • Python traite en batch asynchrone en parallèle            ║
 * ║  • 120 drifts en ~5-10 minutes au lieu de 10h               ║
 * ╚══════════════════════════════════════════════════════════════╝
 */
public class ReplicaPlacementFast {

    // ══════════════════════════════════════════════════════════════
    //  PARAMÈTRES — modifier ici uniquement
    // ══════════════════════════════════════════════════════════════

    private static final int    NOMBRE_DRIFTS  = 120;
    private static final long   RANDOM_SEED    = 12345L;
    private static final int    VMS_PER_HOST   = 5;

    // Délai max d'attente Python par drift (millisecondes)
    // 0 = complètement non-bloquant (recommandé pour 120+ drifts)
    // 5000 = attend 5s max si Python est rapide
    private static final long   PYTHON_WAIT_MS = 0L;

    private static final int    VM_MIPS  = 1000;
    private static final int    VM_PES   = 1;
    private static final int    VM_RAM   = 2048;
    private static final long   VM_BW    = 1000;
    private static final long   VM_SIZE  = 10_000;
    private static final String VMM      = "Xen";

    // ══════════════════════════════════════════════════════════════
    //  CHEMINS
    // ══════════════════════════════════════════════════════════════

    private static final String BASE;
    static {
        String env = System.getenv("CLOUDSIM_DRIVE_PATH");
        BASE = (env != null && !env.isEmpty())
            ? (env.endsWith("/") ? env : env + "/")
            : System.getProperty("user.home") + "/cloudsim_optimization/";
    }

    // Java écrit les inputs pour Python ici :  input/drift_N/
    private static final String INPUT_BASE   = BASE + "input/";
    // Java lit les résultats Python ici :      output/drift_N/
    private static final String OUTPUT_BASE  = BASE + "output/";
    // Résultats finaux (avant + après) ici :   results/drift_N/avant/ et /apres/
    private static final String RESULTS_BASE = BASE + "results/";

    // ══════════════════════════════════════════════════════════════
    //  ÉTAT GLOBAL
    // ══════════════════════════════════════════════════════════════

    private static final Map<Integer, Integer>       vmFree            = new HashMap<>();
    private static final Map<Integer, Integer>       vmToDc            = new HashMap<>();
    private static final Map<Integer, Integer>       pyReplicas        = new HashMap<>();
    private static final Map<Integer, List<Integer>> pyPlacements      = new HashMap<>();
    private static final Map<String, String>         BEST_DC_MAP       = buildBestDcMap();

    // ══════════════════════════════════════════════════════════════
    //  MODÈLE DE DONNÉES
    // ══════════════════════════════════════════════════════════════

    enum AT { READ, WRITE, READ_WRITE }

    static class Fichier {
        int id, sizeMB, importance;
        List<Integer>         dcs        = new ArrayList<>();
        Map<Integer, AT>      userAccess = new HashMap<>();
        Map<Integer, Integer> userToDc   = new HashMap<>();

        Fichier(int id, int sizeMB, int importance, List<Integer> dcs) {
            this.id = id; this.sizeMB = sizeMB; this.importance = importance;
            this.dcs.addAll(dcs);
        }
        void addUser(int uid, AT a, int dcId) {
            userAccess.put(uid, a); userToDc.put(uid, dcId);
        }
        // Copie profonde pour snapshot avant-drift
        Fichier snapshot() {
            Fichier c = new Fichier(id, sizeMB, importance, dcs);
            c.userAccess.putAll(userAccess);
            c.userToDc.putAll(userToDc);
            return c;
        }
    }

    static class Utilisateur {
        int id; String location;
        Map<Integer, AT>      access  = new HashMap<>();
        Map<Integer, Integer> fileToDc = new HashMap<>();
        Utilisateur(int id, String loc) { this.id = id; this.location = loc; }
        void addAccess(int fid, AT a, int dcId) { access.put(fid, a); fileToDc.put(fid, dcId); }
        Utilisateur snapshot() {
            Utilisateur c = new Utilisateur(id, location);
            c.access.putAll(access); c.fileToDc.putAll(fileToDc); return c;
        }
    }

    static class DC {
        int id, hosts, capacite, utilise, libre, nbVms;
        String region, type; double cout;
        List<Vm> vms = new ArrayList<>();
        DC(int id, String region, String type, double cout, int hosts) {
            this.id = id; this.region = region; this.type = type;
            this.cout = cout; this.hosts = hosts;
            this.capacite = hosts * 100_000;
            this.libre = this.capacite; this.utilise = 0;
            this.nbVms = hosts * VMS_PER_HOST;
        }
        DC snapshot() {
            DC c = new DC(id, region, type, cout, hosts);
            c.utilise = utilise; c.libre = libre; c.nbVms = nbVms;
            return c;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  MAIN
    // ══════════════════════════════════════════════════════════════

    public static void main(String[] args) throws Exception {
        Files.createDirectories(Paths.get(INPUT_BASE));
        Files.createDirectories(Paths.get(OUTPUT_BASE));
        Files.createDirectories(Paths.get(RESULTS_BASE));

        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.printf ("║  SIMULATION FAST — %d drifts%n", NOMBRE_DRIFTS);
        System.out.printf ("║  Base : %s%n", BASE);
        System.out.println("╚══════════════════════════════════════════════════╝");

        CloudSim.init(1, Calendar.getInstance(), false);
        DatacenterBroker broker = new DatacenterBroker("Broker");

        // ── 30 Datacenters ──
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
        double[] couts = {
            0.06,0.06,0.06,0.06,0.06,0.06,
            0.05,0.05,0.05,0.05,0.05,0.05,
            0.045,0.045,0.045,0.045,0.045,0.045,
            0.035,0.035,0.035,0.035,0.035,0.035,
            0.04,0.04,0.04,0.04,0.04,0.04
        };

        List<DC> dcList = new ArrayList<>();
        Map<Integer, DC> dcMap = new HashMap<>();
        Map<Integer, String> dcRegion = new HashMap<>();

        for (int i = 1; i <= 30; i++) {
            int h = types[i-1].equals("grand") ? 100 : types[i-1].equals("moyen") ? 60 : 30;
            DC dc = new DC(i, regions[i-1], types[i-1], couts[i-1], h);
            createCloudSimDC("DC_" + i, h, couts[i-1]);
            dcList.add(dc); dcMap.put(i, dc); dcRegion.put(i, regions[i-1]);
        }

        // ── VMs ──
        int vid = 1;
        for (DC dc : dcList) {
            for (int j = 0; j < dc.nbVms; j++) {
                Vm vm = new Vm(vid, broker.getId(), VM_MIPS, VM_PES, VM_RAM,
                               VM_BW, VM_SIZE, VMM, new CloudletSchedulerTimeShared());
                dc.vms.add(vm);
                vmFree.put(vid, (int)VM_SIZE);
                vmToDc.put(vid, dc.id);
                vid++;
            }
        }

        // ── 500 fichiers ──
        Random rng = new Random(RANDOM_SEED);
        int[] tailles = {1500, 2500, 5000, 7500, 10000};
        List<Fichier> fichiers = new ArrayList<>();
        for (int i = 1; i <= 500; i++) {
            List<Integer> fDcs = new ArrayList<>();
            fDcs.add(((i-1) % 30) + 1);
            int d2 = ((i-1) + 15) % 30 + 1;
            if (d2 != fDcs.get(0)) fDcs.add(d2);
            fichiers.add(new Fichier(i, tailles[(i-1) % 5], ((i-1) % 5) + 1, fDcs));
        }
        // stockage initial
        for (Fichier f : fichiers)
            for (int dcId : f.dcs) { DC dc = dcMap.get(dcId); dc.utilise += f.sizeMB; dc.libre -= f.sizeMB; }

        // ── 1000 utilisateurs ──
        String[] locs  = {"Paris (France)","Berlin (Allemagne)","Madrid (Espagne)",
                           "New York (USA)","San Francisco (USA)","Tokyo (Japon)",
                           "Mumbai (Inde)","Nairobi (Kenya)","Lagos (Nigeria)","Le Caire (Egypte)"};
        int[]    nbUs  = {80,160,100,120,80,100,120,60,100,80};
        List<Utilisateur> users = new ArrayList<>();
        int uid = 1;
        for (int i = 0; i < locs.length; i++)
            for (int j = 0; j < nbUs[i]; j++)
                users.add(new Utilisateur(uid++, locs[i]));

        initAccess(fichiers, users, dcRegion, rng);

        // ══════════════════════════════════════════════════════════
        //  BOUCLE PRINCIPALE — NON-BLOQUANTE
        // ══════════════════════════════════════════════════════════

        long globalT = System.currentTimeMillis();
        List<String> summary = new ArrayList<>();
        summary.add("DriftID,DureeMs,Source,NbFichiersOpt,NbPlacements,TotalRepliques");

        for (int driftId = 1; driftId <= NOMBRE_DRIFTS; driftId++) {
            long t0 = System.currentTimeMillis();
            System.out.printf("%n─── Drift %3d/%d ───%n", driftId, NOMBRE_DRIFTS);

            String driftBase   = RESULTS_BASE + "drift_" + driftId + "/";
            String avantDir    = driftBase + "avant/";
            String apresDir    = driftBase + "apres/";
            String inputDrift  = INPUT_BASE + "drift_" + driftId + "/";

            Files.createDirectories(Paths.get(avantDir));
            Files.createDirectories(Paths.get(apresDir));
            Files.createDirectories(Paths.get(inputDrift));

            try {
                // ══ ÉTAPE 1 : Snapshot AVANT drift → 3 CSV "avant" ══════
                // Ces 3 CSV représentent l'état APRÈS le drift N-1 (avant le drift N)
                List<DC>          dcSnap   = dcList.stream().map(DC::snapshot).collect(Collectors.toList());
                List<Fichier>     fSnap    = fichiers.stream().map(Fichier::snapshot).collect(Collectors.toList());
                List<Utilisateur> uSnap    = users.stream().map(Utilisateur::snapshot).collect(Collectors.toList());

                ecrireDcCsv   (avantDir + "datacenter_information.csv", dcSnap);
                ecrireFichiersCsv(avantDir + "file_information.csv",    fSnap, dcRegion);
                ecrireUsersCsv(avantDir + "user_access_details.csv",    uSnap, dcRegion);
                System.out.println("  ✓ CSV avant drift écrits");

                // ══ ÉTAPE 2 : Appliquer le drift ════════════════════════
                applyDriftTailles(fichiers, rng, dcMap);
                applyDriftAcces  (fichiers, users, rng, dcRegion);

                // ══ ÉTAPE 3 : Écrire inputs pour Python ═════════════════
                ecrireInputsPython(inputDrift, fichiers, users, dcList, dcRegion, driftId);

                // ══ ÉTAPE 4 : Lire résultats Python (NON-BLOQUANT) ══════
                // Si Python a déjà traité ce drift → on applique ses résultats
                // Sinon → réplication par défaut immédiate (0 attente)
                boolean optimise = lireResultatsPython(driftId);

                // ══ ÉTAPE 5 : Appliquer optimisation ou défaut ══════════
                if (optimise) {
                    appliquerOptimisation(fichiers, dcMap);
                } else {
                    appliquerDefaut(fichiers, dcMap, dcRegion, rng);
                }

                // ══ ÉTAPE 6 : Snapshot APRÈS optimisation → 3 CSV "apres" ══
                ecrireDcCsv      (apresDir + "datacenter_information.csv", dcList);
                ecrireFichiersCsv(apresDir + "file_information.csv",       fichiers, dcRegion);
                ecrireUsersCsv   (apresDir + "user_access_details.csv",    users, dcRegion);
                System.out.println("  ✓ CSV après optimisation écrits");

                // ══ Résumé drift ════════════════════════════════════════
                long elapsed = System.currentTimeMillis() - t0;
                int totRep = fichiers.stream().mapToInt(f -> f.dcs.size()).sum();
                summary.add(String.format("%d,%d,%s,%d,%d,%d",
                    driftId, elapsed,
                    optimise ? "SPEA2+NSGA2" : "Defaut",
                    pyReplicas.size(), pyPlacements.size(), totRep));

                System.out.printf("  ✓ Drift %d terminé en %d ms [%s] répliques=%d%n",
                    driftId, elapsed, optimise ? "SPEA2+NSGA2" : "Defaut", totRep);

            } catch (Exception e) {
                System.err.printf("  ✗ Erreur drift %d : %s%n", driftId, e.getMessage());
                e.printStackTrace();
            }
        }

        // ── Résumé global ──
        ecrireCSV(RESULTS_BASE + "global_summary.csv", summary);
        long total = System.currentTimeMillis() - globalT;
        System.out.printf("%n╔══════════════════════════════════════════════════╗%n");
        System.out.printf("║  ✅ %d drifts en %.1f s (%.2f s/drift)%n",
            NOMBRE_DRIFTS, total/1000.0, (double)total/NOMBRE_DRIFTS/1000.0);
        System.out.printf("║  Résultats : %s%n", RESULTS_BASE);
        System.out.printf("╚══════════════════════════════════════════════════╝%n");

        CloudSim.stopSimulation();
    }

    // ══════════════════════════════════════════════════════════════
    //  LECTURE NON-BLOQUANTE DES RÉSULTATS PYTHON
    // ══════════════════════════════════════════════════════════════

    private static boolean lireResultatsPython(int driftId) {
        Path resultF    = Paths.get(OUTPUT_BASE + "drift_" + driftId + "/optimization_result.csv");
        Path placementF = Paths.get(OUTPUT_BASE + "drift_" + driftId + "/optimization_placement.csv");

        long deadline = System.currentTimeMillis() + PYTHON_WAIT_MS;
        do {
            if (Files.exists(resultF) && Files.exists(placementF)) {
                return chargerResultats(resultF, placementF);
            }
            if (PYTHON_WAIT_MS > 0) {
                try { Thread.sleep(200); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); break;
                }
            }
        } while (System.currentTimeMillis() < deadline);

        pyReplicas.clear(); pyPlacements.clear();
        return false;
    }

    private static boolean chargerResultats(Path rFile, Path pFile) {
        pyReplicas.clear(); pyPlacements.clear();
        try {
            // SPEA2
            List<String> lines = Files.readAllLines(rFile);
            if (lines.size() < 2) return false;
            String[] h = lines.get(0).split(",");
            int fC = col(h,"fileid","file_id","id");
            int rC = col(h,"replicas","replicacount","nbcopies");
            int sC = col(h,"selected","selection","is_selected");
            for (int i = 1; i < lines.size(); i++) {
                String[] p = lines.get(i).trim().split(",");
                if (fC < 0 || fC >= p.length) continue;
                int fid = Integer.parseInt(p[fC].trim());
                int rep = 2;
                if (rC >= 0 && rC < p.length) {
                    try { int r = Integer.parseInt(p[rC].trim()); if (r>=1&&r<=5) rep=r; }
                    catch (NumberFormatException ignored) {}
                } else if (sC >= 0 && sC < p.length) {
                    String s = p[sC].trim().toLowerCase();
                    if (s.equals("yes")||s.equals("true")||s.equals("1")) rep = 3;
                }
                pyReplicas.put(fid, rep);
            }
            // NSGA-II
            List<String> pl = Files.readAllLines(pFile);
            if (pl.size() >= 2) {
                String[] ph = pl.get(0).split(",");
                int pfC = col(ph,"fileid","file_id","id");
                int pdC = col(ph,"datacenterid","dc_id","datacenter");
                for (int i = 1; i < pl.size(); i++) {
                    String[] p = pl.get(i).trim().split(",");
                    if (pfC<0||pdC<0||pfC>=p.length||pdC>=p.length) continue;
                    int fid = Integer.parseInt(p[pfC].trim());
                    int did = Integer.parseInt(p[pdC].trim());
                    pyPlacements.computeIfAbsent(fid, k -> new ArrayList<>()).add(did);
                }
            }
            System.out.printf("  [Python] SPEA2=%d NSGA2=%d%n", pyReplicas.size(), pyPlacements.size());
            return !pyReplicas.isEmpty() || !pyPlacements.isEmpty();
        } catch (Exception e) {
            System.err.println("  [Python] Erreur lecture : " + e.getMessage());
            return false;
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  ÉCRITURE DES INPUTS POUR PYTHON
    // ══════════════════════════════════════════════════════════════

    private static void ecrireInputsPython(String dir, List<Fichier> fichiers,
            List<Utilisateur> users, List<DC> dcList,
            Map<Integer, String> dcRegion, int driftId) throws IOException {

        // file_information.csv
        List<String> l = new ArrayList<>();
        l.add("FileID,Taille(MB),Importance,Popularite,ReplicaCount,CurrentDatacenters,FrequenceAcces");
        for (Fichier f : fichiers) {
            String dcs = f.dcs.stream().map(String::valueOf).collect(Collectors.joining(";"));
            int freq = (int)(f.userAccess.values().stream().filter(a->a!=AT.WRITE).count()
                           + f.userAccess.values().stream().filter(a->a!=AT.READ).count());
            l.add(String.format(Locale.US, "%d,%d,%d,%d,%d,%s,%d",
                f.id, f.sizeMB, f.importance, f.userAccess.size(), f.dcs.size(), dcs, freq));
        }
        ecrireCSV(dir + "file_information.csv", l);

        // datacenter_information.csv
        l.clear();
        l.add("DatacenterID,Region,TypeDC,StorageCapacityMB,StorageUsedMB,StorageFreeMB,HostCount,CostPerCloudlet,VMPerHost");
        for (DC dc : dcList)
            l.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.4f,%d",
                dc.id, dc.region, dc.type, dc.capacite, dc.utilise, dc.libre,
                dc.hosts, dc.cout, VMS_PER_HOST));
        ecrireCSV(dir + "datacenter_information.csv", l);

        // user_access_details.csv
        l.clear();
        l.add("UserID,UserLocation,FileID,AccessType,PreferredDatacenter,Region");
        for (Utilisateur u : users)
            for (Map.Entry<Integer,AT> e : u.access.entrySet()) {
                int dcId = u.fileToDc.getOrDefault(e.getKey(), 1);
                l.add(String.format("%d,%s,%d,%s,%d,%s",
                    u.id, u.location, e.getKey(), e.getValue(), dcId,
                    dcRegion.getOrDefault(dcId, "?")));
            }
        ecrireCSV(dir + "user_access_details.csv", l);

        // Signal pour Python
        Files.write(Paths.get(dir + "data_ready.signal"),
            (driftId + ":" + System.currentTimeMillis()).getBytes(),
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    // ══════════════════════════════════════════════════════════════
    //  ÉCRITURE DES 3 CSV (datacenter / fichiers / utilisateurs)
    // ══════════════════════════════════════════════════════════════

    /** CSV datacenter_information.csv */
    private static void ecrireDcCsv(String path, List<DC> dcs) throws IOException {
        List<String> l = new ArrayList<>();
        l.add("DatacenterID,Region,TypeDC,HostCount,StorageCapacityMB,StorageUsedMB,StorageFreeMB,UsagePct,VMCount");
        for (DC dc : dcs)
            l.add(String.format(Locale.US, "%d,%s,%s,%d,%d,%d,%d,%.2f%%,%d",
                dc.id, dc.region, dc.type, dc.hosts,
                dc.capacite, dc.utilise, dc.libre,
                100.0 * dc.utilise / Math.max(1, dc.capacite), dc.nbVms));
        ecrireCSV(path, l);
    }

    /** CSV file_information.csv */
    private static void ecrireFichiersCsv(String path, List<Fichier> fichiers,
            Map<Integer, String> dcRegion) throws IOException {
        List<String> l = new ArrayList<>();
        l.add("FileID,SizeMB,Importance,ReplicaCount,ReplicaLocations," +
              "NbUsersAcces,NbRead,NbWrite,NbReadWrite");
        for (Fichier f : fichiers) {
            long r  = f.userAccess.values().stream().filter(a->a==AT.READ).count();
            long w  = f.userAccess.values().stream().filter(a->a==AT.WRITE).count();
            long rw = f.userAccess.values().stream().filter(a->a==AT.READ_WRITE).count();
            String locs = f.dcs.stream()
                .map(d -> d + "(" + dcRegion.getOrDefault(d,"?") + ")")
                .collect(Collectors.joining("; "));
            l.add(String.format(Locale.US, "%d,%d,%d,%d,%s,%d,%d,%d,%d",
                f.id, f.sizeMB, f.importance, f.dcs.size(), locs,
                f.userAccess.size(), r, w, rw));
        }
        ecrireCSV(path, l);
    }

    /** CSV user_access_details.csv */
    private static void ecrireUsersCsv(String path, List<Utilisateur> users,
            Map<Integer, String> dcRegion) throws IOException {
        List<String> l = new ArrayList<>();
        l.add("UserID,UserLocation,FileID,AccessType,DatacenterAcceded,Region");
        for (Utilisateur u : users)
            for (Map.Entry<Integer,AT> e : u.access.entrySet()) {
                int dcId = u.fileToDc.getOrDefault(e.getKey(), 1);
                l.add(String.format("%d,%s,%d,%s,%d,%s",
                    u.id, u.location, e.getKey(), e.getValue(), dcId,
                    dcRegion.getOrDefault(dcId, "?")));
            }
        ecrireCSV(path, l);
    }

    // ══════════════════════════════════════════════════════════════
    //  DRIFTS
    // ══════════════════════════════════════════════════════════════

    private static void applyDriftTailles(List<Fichier> fichiers, Random rng, Map<Integer,DC> dcMap) {
        int mod = 0;
        for (Fichier f : fichiers) {
            if (rng.nextDouble() >= 0.3) continue;
            int old = f.sizeMB;
            int nw  = Math.max(500, Math.min(20_000, (int)(old * (0.5 + rng.nextDouble()))));
            int delta = nw - old;
            if (delta > 0) {
                int avail = f.dcs.stream().mapToInt(d -> dcMap.get(d).libre).sum();
                if (avail < delta) continue;
                int rem = delta;
                for (int dcId : f.dcs) {
                    DC dc = dcMap.get(dcId);
                    int add = Math.min(rem, dc.libre);
                    dc.libre -= add; dc.utilise += add; rem -= add;
                    if (rem == 0) break;
                }
            } else {
                int toFree = -delta;
                for (int dcId : f.dcs) {
                    DC dc = dcMap.get(dcId);
                    int fr = Math.min(toFree, dc.utilise);
                    dc.libre += fr; dc.utilise -= fr; toFree -= fr;
                    if (toFree == 0) break;
                }
            }
            f.sizeMB = nw; mod++;
        }
        System.out.printf("  Drift tailles : %d fichiers modifiés%n", mod);
    }

    private static void applyDriftAcces(List<Fichier> fichiers, List<Utilisateur> users,
            Random rng, Map<Integer,String> dcRegion) {
        Map<Integer,Fichier> fMap = new HashMap<>();
        for (Fichier f : fichiers) fMap.put(f.id, f);
        int added=0, removed=0, changed=0;
        for (Utilisateur u : users) {
            if (rng.nextDouble() >= 0.4) continue;
            List<Integer> cur = new ArrayList<>(u.access.keySet());
            // Suppressions
            int toRem = Math.min(cur.size(), 1 + rng.nextInt(3));
            for (int i = 0; i < toRem && !cur.isEmpty(); i++) {
                int fid = cur.remove(rng.nextInt(cur.size()));
                u.access.remove(fid); u.fileToDc.remove(fid);
                Fichier f = fMap.get(fid);
                if (f != null) f.userAccess.remove(u.id);
                removed++;
            }
            // Ajouts
            for (int i = 0; i < 1 + rng.nextInt(3); i++) {
                Fichier f = fichiers.get(rng.nextInt(fichiers.size()));
                if (!u.access.containsKey(f.id)) {
                    AT a = rndAT(rng);
                    int dcId = meilleurDc(f, u.location, dcRegion);
                    u.addAccess(f.id, a, dcId);
                    f.addUser(u.id, a, dcId);
                    added++;
                }
            }
            // Modifications
            List<Integer> rem2 = new ArrayList<>(u.access.keySet());
            int toMod = Math.min(rem2.size(), 1 + rng.nextInt(2));
            for (int i = 0; i < toMod && !rem2.isEmpty(); i++) {
                int fid = rem2.get(rng.nextInt(rem2.size()));
                AT na = rndAT(rng);
                if (u.access.get(fid) != na) {
                    u.access.put(fid, na);
                    Fichier f = fMap.get(fid);
                    if (f != null) f.userAccess.put(u.id, na);
                    changed++;
                }
            }
        }
        System.out.printf("  Drift accès : +%d -%d ~%d%n", added, removed, changed);
    }

    // ══════════════════════════════════════════════════════════════
    //  OPTIMISATION
    // ══════════════════════════════════════════════════════════════

    private static void appliquerOptimisation(List<Fichier> fichiers, Map<Integer,DC> dcMap) {
        int count = 0;
        for (Fichier f : fichiers) {
            // SPEA2 : ajuster nombre de répliques
            if (pyReplicas.containsKey(f.id)) {
                int target = pyReplicas.get(f.id);
                for (DC dc : dcMap.values()) {
                    if (f.dcs.size() >= target) break;
                    if (!f.dcs.contains(dc.id) && dc.libre >= f.sizeMB) {
                        f.dcs.add(dc.id);
                        dc.libre -= f.sizeMB; dc.utilise += f.sizeMB;
                        count++;
                    }
                }
            }
            // NSGA-II : déplacer répliques
            if (pyPlacements.containsKey(f.id)) {
                List<Integer> nDcs = pyPlacements.get(f.id);
                boolean ok = nDcs.stream().allMatch(did -> {
                    DC dc = dcMap.get(did); return dc != null && dc.libre >= f.sizeMB;
                });
                if (ok && !nDcs.isEmpty()) {
                    for (int old : f.dcs) {
                        DC dc = dcMap.get(old);
                        if (dc != null) { dc.libre += f.sizeMB; dc.utilise -= f.sizeMB; }
                    }
                    f.dcs = new ArrayList<>(nDcs);
                    for (int nid : nDcs) {
                        DC dc = dcMap.get(nid);
                        if (dc != null) { dc.libre -= f.sizeMB; dc.utilise += f.sizeMB; }
                    }
                    count++;
                }
            }
        }
        System.out.printf("  Optimisation SPEA2+NSGA2 : %d fichiers%n", count);
    }

    private static void appliquerDefaut(List<Fichier> fichiers, Map<Integer,DC> dcMap,
            Map<Integer,String> dcRegion, Random rng) {
        int seuil = Math.max(1, fichiers.size() * 30 / 100);
        List<Fichier> tries = fichiers.stream()
            .sorted(Comparator.comparingInt(f -> -f.userAccess.size()))
            .limit(seuil).collect(Collectors.toList());
        int count = 0;
        for (Fichier f : tries) {
            Map<String,Long> regionCount = f.userToDc.values().stream()
                .collect(Collectors.groupingBy(d -> dcRegion.getOrDefault(d,"?"), Collectors.counting()));
            String target = regionCount.entrySet().stream()
                .filter(e -> f.dcs.stream().noneMatch(d -> dcRegion.get(d).equals(e.getKey())))
                .max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse(null);
            if (target == null) continue;
            final String tr = target;
            List<DC> cands = dcMap.values().stream()
                .filter(dc -> dc.region.equals(tr) && dc.libre >= f.sizeMB && !f.dcs.contains(dc.id))
                .collect(Collectors.toList());
            if (!cands.isEmpty()) {
                DC chosen = cands.get(rng.nextInt(cands.size()));
                f.dcs.add(chosen.id);
                chosen.libre -= f.sizeMB; chosen.utilise += f.sizeMB;
                count++;
            }
        }
        System.out.printf("  Réplication défaut : %d fichiers%n", count);
    }

    // ══════════════════════════════════════════════════════════════
    //  INIT ACCÈS
    // ══════════════════════════════════════════════════════════════

    private static void initAccess(List<Fichier> fichiers, List<Utilisateur> users,
            Map<Integer,String> dcRegion, Random rng) {
        for (Utilisateur u : users) {
            int n = 1 + rng.nextInt(5);
            for (int i = 0; i < n; i++) {
                Fichier f = fichiers.get(rng.nextInt(fichiers.size()));
                if (!u.access.containsKey(f.id)) {
                    AT a = rndAT(rng);
                    int dcId = meilleurDc(f, u.location, dcRegion);
                    u.addAccess(f.id, a, dcId); f.addUser(u.id, a, dcId);
                }
            }
        }
        for (Fichier f : fichiers) {
            if (f.userAccess.size() < 10) {
                for (int i = 0; i < 30 + rng.nextInt(20); i++) {
                    Utilisateur u = users.get(rng.nextInt(users.size()));
                    if (!f.userAccess.containsKey(u.id)) {
                        AT a = rndAT(rng); int dcId = meilleurDc(f, u.location, dcRegion);
                        u.addAccess(f.id, a, dcId); f.addUser(u.id, a, dcId);
                    }
                }
            }
        }
    }

    // ══════════════════════════════════════════════════════════════
    //  UTILITAIRES
    // ══════════════════════════════════════════════════════════════

    private static Datacenter createCloudSimDC(String name, int hosts, double cost) throws Exception {
        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < hosts; i++) {
            List<Pe> pes = new ArrayList<>();
            pes.add(new Pe(0, new PeProvisionerSimple(VM_MIPS)));
            hostList.add(new Host(i,
                new RamProvisionerSimple(VM_RAM * VMS_PER_HOST),
                new BwProvisionerSimple(VM_BW * VMS_PER_HOST),
                VM_SIZE * VMS_PER_HOST, pes, new VmSchedulerTimeShared(pes)));
        }
        DatacenterCharacteristics dc = new DatacenterCharacteristics(
            "x86","Linux","Xen", hostList, 10.0, cost, 0.05, 0.001, 0.001);
        return new Datacenter(name, dc, new VmAllocationPolicySimple(hostList),
            new LinkedList<>(), 0);
    }

    private static void ecrireCSV(String path, List<String> lines) throws IOException {
        Path p = Paths.get(path);
        Files.createDirectories(p.getParent());
        Files.write(p, lines, StandardCharsets.UTF_8,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static int meilleurDc(Fichier f, String loc, Map<Integer,String> dcRegion) {
        String reg = BEST_DC_MAP.getOrDefault(loc, "Europe");
        return f.dcs.stream().filter(d -> reg.equals(dcRegion.get(d)))
            .findFirst().orElse(f.dcs.get(0));
    }

    private static AT rndAT(Random r) {
        switch (r.nextInt(3)) { case 0: return AT.READ; case 1: return AT.WRITE;
                                 default: return AT.READ_WRITE; }
    }

    private static int col(String[] h, String... cands) {
        for (int i = 0; i < h.length; i++) {
            String s = h[i].trim().toLowerCase().replaceAll("[^a-z0-9]","");
            for (String c : cands) if (s.equals(c.toLowerCase().replaceAll("[^a-z0-9]",""))) return i;
        }
        return -1;
    }

    private static Map<String,String> buildBestDcMap() {
        Map<String,String> m = new HashMap<>();
        m.put("Paris (France)","Europe");      m.put("Berlin (Allemagne)","Europe");
        m.put("Madrid (Espagne)","Europe");    m.put("New York (USA)","USA");
        m.put("San Francisco (USA)","USA");    m.put("Tokyo (Japon)","Asie");
        m.put("Mumbai (Inde)","Asie");         m.put("Nairobi (Kenya)","Afrique");
        m.put("Lagos (Nigeria)","Afrique");    m.put("Le Caire (Egypte)","Afrique");
        return m;
    }
}
