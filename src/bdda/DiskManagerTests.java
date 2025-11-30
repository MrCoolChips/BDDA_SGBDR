package bdda;

import java.io.File;
import java.io.IOException;

/**
 * Classe de test complète pour DiskManager
 * Teste toutes les fonctionnalités : allocation, lecture, écriture, désallocation
 */
public class DiskManagerTests {

    public static void main(String[] args) {
        try {
            System.out.println("=== TEST DISKMANAGER ===\n");
            
            // 1. Chargement de la configuration
            testConfigLoading();
            
            // 2. Test allocation de pages
            testPageAllocation();
            
            // 3. Test écriture/lecture
            testWriteRead();
            
            // 4. Test désallocation et réutilisation
            testDeallocAndReuse();
            
            // 5. Test gestion d'erreurs
            testErrorHandling();
            
            // 6. Test Init/Finish (persistance)
            testInitFinish();
            
            System.out.println("\nTOUS LES TESTS REUSSIS !");
            
        } catch (Exception e) {
            System.err.println("ERREUR DANS LES TESTS : " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Test 1 : Chargement de la configuration
     */
    private static void testConfigLoading() throws IOException {
        System.out.println("1. Test chargement configuration...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        
        if (config == null) {
            throw new IOException("Configuration non chargée !");
        }
        
        System.out.println("   OK - Configuration chargée : " + config.getPath());
        System.out.println("   OK - Page size : " + config.getPageSize());
        System.out.println("   OK - Max files : " + config.getMaxFileCount());
    }
    
    /**
     * Test 2 : Allocation de pages
     */
    private static void testPageAllocation() throws IOException {
        System.out.println("\n2. Test allocation de pages...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);  // environnement propre pour ce test

        DiskManager dm = new DiskManager(config);
        
        // Allouer plusieurs pages
        PageId page1 = dm.allocPage();
        PageId page2 = dm.allocPage();
        PageId page3 = dm.allocPage();
        
        System.out.println("   OK - Page 1 allouée : File " + page1.getFileIdx() + ", Page " + page1.getPageIdx());
        System.out.println("   OK - Page 2 allouée : File " + page2.getFileIdx() + ", Page " + page2.getPageIdx());
        System.out.println("   OK - Page 3 allouée : File " + page3.getFileIdx() + ", Page " + page3.getPageIdx());
        
        // Vérifier que les PageId sont différents
        if (page1.equals(page2) || page1.equals(page3) || page2.equals(page3)) {
            throw new IOException("Erreur : PageId identiques détectés !");
        }
        
        System.out.println("   OK - Toutes les pages ont des ID uniques");
        
        dm.finish();
    }
    
    /**
     * Test 3 : Écriture et lecture de données
     */
    private static void testWriteRead() throws IOException {
        System.out.println("\n3. Test écriture/lecture...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);  // environnement propre

        DiskManager dm = new DiskManager(config);
        
        PageId pageId = dm.allocPage();
        
        // Préparer des données à écrire
        String message = "Hello DiskManager! Test d'écriture/lecture.";
        byte[] dataToWrite = new byte[config.getPageSize()];
        byte[] messageBytes = message.getBytes();
        System.arraycopy(messageBytes, 0, dataToWrite, 0, 
                        Math.min(messageBytes.length, dataToWrite.length));
        
        // Écrire
        dm.WritePage(pageId, dataToWrite);
        System.out.println("   OK - Données écrites : " + message);
        
        // Lire
        byte[] dataRead = new byte[config.getPageSize()];
        dm.ReadPage(pageId, dataRead);
        
        String messageRead = new String(dataRead, 0, messageBytes.length);
        System.out.println("   OK - Données lues : " + messageRead);
        
        // Vérifier que les données sont identiques
        if (!message.equals(messageRead)) {
            throw new IOException("Erreur : Données lues différentes des données écrites !");
        }
        
        System.out.println("   OK - Écriture/lecture cohérente");

        dm.finish();
    }
    
    /**
     * Test 4 : Désallocation et réutilisation des pages
     */
    private static void testDeallocAndReuse() throws IOException {
        System.out.println("\n4. Test désallocation et réutilisation...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);  // pour être sûr qu'il n'y a pas d'anciennes pages libres

        DiskManager dm = new DiskManager(config);
        
        // Allouer une page
        PageId originalPage = dm.allocPage();
        System.out.println("   OK - Page originale allouée : " + originalPage.getFileIdx() + "," + originalPage.getPageIdx());
        
        // Désallouer cette page
        dm.DeallocPage(originalPage);
        System.out.println("   OK - Page désallouée");
        
        // Allouer une nouvelle page (devrait réutiliser la page libérée)
        PageId reusedPage = dm.allocPage();
        System.out.println("   OK - Nouvelle page allouée : " + reusedPage.getFileIdx() + "," + reusedPage.getPageIdx());
        
        // Vérifier que c'est la même page qui a été réutilisée
        if (!originalPage.equals(reusedPage)) {
            throw new IOException("Erreur : La page libérée n'a pas été réutilisée !");
        }
        
        System.out.println("   OK - Réutilisation des pages libres fonctionne");

        dm.finish();
    }
    
    /**
     * Test 5 : Gestion d'erreurs
     */
    private static void testErrorHandling() throws IOException {
        System.out.println("\n5. Test gestion d'erreurs...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);  // environnement propre

        DiskManager dm = new DiskManager(config);
        
        PageId validPage = dm.allocPage();
        
        // Test buffer de mauvaise taille
        try {
            byte[] wrongSizeBuffer = new byte[32]; // Taille incorrecte
            dm.ReadPage(validPage, wrongSizeBuffer);
            throw new IOException("Erreur : Exception attendue pour buffer de mauvaise taille !");
        } catch (IOException e) {
            if (e.getMessage().contains("Taille du buffer")) {
                System.out.println("   OK - Erreur buffer mal dimensionné détectée");
            } else {
                throw e;
            }
        }
        
        // Test lecture page inexistante
        try {
            PageId invalidPage = new PageId(0, 999); // Page jamais allouée
            byte[] buffer = new byte[config.getPageSize()];
            dm.ReadPage(invalidPage, buffer);
            throw new IOException("Erreur : Exception attendue pour page inexistante !");
        } catch (IOException e) {
            if (e.getMessage().contains("inexistante")) {
                System.out.println("   OK - Erreur page inexistante détectée");
            } else {
                throw e;
            }
        }
        
        System.out.println("   OK - Gestion d'erreurs correcte");

        dm.finish();
    }
    
    /**
     * Test 6 : Persistance avec Init() et finish()
     */
    private static void testInitFinish() throws IOException {
        System.out.println("\n6. Test persistance Init/finish...");
        
        File configFile = new File("config/config.txt");
        DBConfig config = DBConfig.LoadDBConfig(configFile);
        cleanDBFiles(config);  // on part d'un état propre pour ce test
        
        // === PHASE 1 : Créer et sauvegarder des pages libres ===
        PageId page1, page2, page3;
        {
            DiskManager dm1 = new DiskManager(config);
            // dm1.Init(); // déjà appelé dans le constructeur

            // Allouer quelques pages
            page1 = dm1.allocPage();
            page2 = dm1.allocPage();
            page3 = dm1.allocPage();
            
            System.out.println("   OK - Pages allouées : " + 
                page1.getFileIdx() + "," + page1.getPageIdx() + " | " +
                page2.getFileIdx() + "," + page2.getPageIdx() + " | " +
                page3.getFileIdx() + "," + page3.getPageIdx());
            
            // Désallouer certaines pages pour créer des pages libres
            dm1.DeallocPage(page1);
            dm1.DeallocPage(page3);
            
            System.out.println("   OK - Pages désallouées : " + 
                page1.getFileIdx() + "," + page1.getPageIdx() + " et " +
                page3.getFileIdx() + "," + page3.getPageIdx());
            
            // Sauvegarder l'état
            dm1.finish();
            System.out.println("   OK - État sauvegardé avec finish()");
        }
        
        // === PHASE 2 : Charger les pages libres dans une nouvelle instance ===
        {
            DiskManager dm2 = new DiskManager(config);
            // dm2.Init(); // déjà appelé dans le constructeur
            
            System.out.println("   OK - État chargé avec Init() (constructeur)");
            
            // Les prochaines allocations devraient réutiliser les pages libres
            PageId reused1 = dm2.allocPage(); // Devrait être page1 ou page3
            PageId reused2 = dm2.allocPage(); // Devrait être l'autre page libre
            PageId newPage = dm2.allocPage(); // Devrait être une nouvelle page
            
            System.out.println("   OK - Pages réutilisées : " + 
                reused1.getFileIdx() + "," + reused1.getPageIdx() + " | " +
                reused2.getFileIdx() + "," + reused2.getPageIdx() + " | " +
                newPage.getFileIdx() + "," + newPage.getPageIdx());
            
            // Vérifier que reused1 et reused2 correspondent bien à page1 et page3 (ordre quelconque)
            boolean case1 = reused1.equals(page1) && reused2.equals(page3);
            boolean case2 = reused1.equals(page3) && reused2.equals(page1);
            if (!case1 && !case2) {
                throw new IOException("Erreur : les pages libres sauvegardées n'ont pas été réutilisées correctement !");
            }
            
            System.out.println("   OK - Réutilisation correcte des pages sauvegardées");
            
            // Sauvegarder à nouveau l'état
            dm2.finish();
        }
        
        // === PHASE 3 : Tester Init() sans fichier de sauvegarde ===
        {
            // Supprimer le fichier de sauvegarde
            File saveFile = new File(config.getPath(), "dm.save");
            if (saveFile.exists()) {
                saveFile.delete();
            }
            
            DiskManager dm3 = new DiskManager(config);
            // dm3.Init(); // déjà appelé dans le constructeur
            
            PageId firstPage = dm3.allocPage();
            System.out.println("   OK - Init() fonctionne sans fichier de sauvegarde, première page : " + 
                firstPage.getFileIdx() + "," + firstPage.getPageIdx());

            dm3.finish();
        }
        
        System.out.println("   OK - Persistance Init/finish complètement fonctionnelle");
    }

    /**
     * Helper : supprime les fichiers Data*.bin et dm.save pour repartir d'un état propre.
     * ATTENTION : à utiliser uniquement dans le cadre des tests.
     */
    private static void cleanDBFiles(DBConfig config) {
        File dir = new File(config.getPath());
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        File[] files = dir.listFiles();
        if (files == null) return;

        for (File f : files) {
            String name = f.getName();
            if (name.equals("dm.save") || (name.startsWith("Data") && name.endsWith(".bin"))) {
                // On ignore le résultat du delete, c'est uniquement pour les tests
                f.delete();
            }
        }
    }
}
