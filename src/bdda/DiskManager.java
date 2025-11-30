package bdda;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.BitSet;

public class DiskManager {
    
    private DBConfig config;

    /**
     * Bitmap d'utilisation des pages :
     * usedPages[fileIdx].get(pageIdx) == true  -> page utilisée (1)
     * usedPages[fileIdx].get(pageIdx) == false -> page libre (0)
     */
    private BitSet[] usedPages;

    /**
     * Constructeur du DiskManager.
     * Initialise le gestionnaire avec la configuration fournie et
     * reconstruit les bitmaps à partir des fichiers de données et de dm.save.
     * 
     * @param config configuration de la base de données contenant
     *               le chemin, la taille des pages et le nombre max de fichiers
     */
    public DiskManager(DBConfig config) throws IOException {
        this.config = config;
        this.usedPages = new BitSet[config.getMaxFileCount()];
        this.Init();
    }

    /**
     * Retourne la configuration actuelle du DiskManager.
     * 
     * @return l'objet DBConfig utilisé par ce gestionnaire
     */
    public DBConfig getConfig() {
        return config;
    }
    
    /**
     * Alloue une nouvelle page pour stockage.
     * 
     * 1) Si une page précédemment désallouée (bit = 0) est disponible, elle est réutilisée.
     * 2) Sinon, une nouvelle page est créée à la fin d'un fichier existant
     *    ou dans un nouveau fichier si nécessaire.
     * 
     * @return PageId identifiant unique de la page allouée
     * @throws IOException si impossible de créer le fichier ou d'écrire la page,
     *                     ou si la limite maximale de fichiers est atteinte
     */
    public PageId allocPage() throws IOException {

        int maxFiles = config.getMaxFileCount(); 
        int pageSize = config.getPageSize();

        // 1) Essayer d'abord de réutiliser une page libre (bit = 0)
        for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
            File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
            if (!f.exists()) {
                continue; // pas de fichier, donc pas de pages ici
            }

            long length = f.length();
            if (length <= 0) {
                continue;
            }

            int pageCount = (int) (length / pageSize);
            BitSet bitmap = getOrCreateBitmap(fileIdx);

            // Parcourir uniquement les pages existantes dans le fichier
            for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                if (!bitmap.get(pageIdx)) {   // false -> page libre
                    bitmap.set(pageIdx);      // devient utilisée (1)
                    return new PageId(fileIdx, pageIdx);
                }
            }
        }
        
        // 2) Aucune page libre : rajouter une nouvelle page à la fin d'un fichier
        for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
            File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
            if (!f.exists()) {
                f.createNewFile();
            }

            try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
                long pageIdxLong = raf.length() / pageSize;
                int pageIdx = (int) pageIdxLong;

                // Écrit une page vide (remplie de zéros) pour réserver l'espace
                raf.seek(raf.length());
                raf.write(new byte[pageSize]);

                BitSet bitmap = getOrCreateBitmap(fileIdx);
                bitmap.set(pageIdx); // nouvelle page = utilisée

                return new PageId(fileIdx, pageIdx);
            }
        }
        
        throw new IOException("Limite de fichiers atteinte (" + maxFiles + ")");
    }

    /**
     * Désalloue une page en la marquant libre dans la bitmap (bit = 0).
     * La page pourra être réutilisée lors du prochain appel à allocPage().
     * Vérifie que la page existe avant de la désallouer.
     * 
     * @param pageId identifiant de la page à désallouer
     * @throws IOException si la page n'existe pas ou si le fichier est inaccessible
     */
    public void DeallocPage(PageId pageId) throws IOException {
        File f = getFile(pageId);
        // Vérifie que la page existe (lève une exception sinon)
        getOffset(pageId, f);

        BitSet bitmap = getOrCreateBitmap(pageId.getFileIdx());
        bitmap.clear(pageId.getPageIdx()); // 0 -> libre
    }

    /**
     * Lit le contenu d'une page et le copie dans le buffer fourni.
     * Le buffer doit avoir exactement la taille d'une page.
     * 
     * @param pageId identifiant de la page à lire
     * @param buff buffer de destination (doit faire config.getPageSize() octets)
     * @throws IOException si la page n'existe pas, le fichier est inaccessible,
     *                     ou si la taille du buffer est incorrecte
     */
    public void ReadPage(PageId pageId, byte[] buff) throws IOException {

        if (buff.length != config.getPageSize()) {
            throw new IOException("Taille du buffer (" + buff.length + 
                ") différente de la taille d'une page (" + config.getPageSize() + ")");
        }

        File f = getFile(pageId);

        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            long offset = getOffset(pageId, f);
            raf.seek(offset);
            raf.readFully(buff);
        }
    }

    /**
     * Écrit le contenu du buffer dans la page spécifiée.
     * Le buffer doit avoir exactement la taille d'une page.
     * 
     * @param pageId identifiant de la page où écrire
     * @param buff buffer contenant les données à écrire (doit faire config.getPageSize() octets)
     * @throws IOException si la page n'existe pas, le fichier est inaccessible,
     *                     ou si la taille du buffer est incorrecte
     */
    public void WritePage(PageId pageId, byte[] buff) throws IOException {

        if (buff.length != config.getPageSize()) {
            throw new IOException("Taille du buffer (" + buff.length + 
                ") différente de la taille d'une page (" + config.getPageSize() + ")");
        }

        File f = getFile(pageId);

        try (RandomAccessFile raf = new RandomAccessFile(f, "rw")) {
            long offset = getOffset(pageId, f);
            raf.seek(offset);
            raf.write(buff);
        }
    }

    /**
     * Finalise le DiskManager à l'arrêt du SGBD.
     * Sauvegarde la liste des pages libres dans un fichier au format CSV
     * (fileIdx,pageIdx) pour permettre leur récupération au prochain démarrage.
     * 
     * @throws IOException si impossible d'écrire le fichier de sauvegarde
     */
    public void finish() throws IOException {
        File saveFile = new File(config.getPath(), "dm.save");
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(saveFile))) {
            int maxFiles = config.getMaxFileCount();
            int pageSize = config.getPageSize();

            for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
                File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
                if (!f.exists()) {
                    continue;
                }

                long length = f.length();
                if (length <= 0) {
                    continue;
                }

                int pageCount = (int) (length / pageSize);
                BitSet bitmap = usedPages[fileIdx];
                if (bitmap == null) {
                    bitmap = new BitSet();
                    usedPages[fileIdx] = bitmap;
                }

                // On écrit UNIQUEMENT les pages libres (bit = 0)
                for (int pageIdx = 0; pageIdx < pageCount; pageIdx++) {
                    if (!bitmap.get(pageIdx)) {
                        writer.write(fileIdx + "," + pageIdx);
                        writer.newLine();
                    }
                }
            }
        }
    }

    /**
     * Initialise le DiskManager au démarrage du SGBD.
     * 
     * 1) Pour chaque fichier Data existant, on considère toutes ses pages comme utilisées.
     * 2) Puis on lit dm.save (s'il existe) pour marquer certaines pages comme libres.
     * 
     * @throws IOException si erreur lors de la lecture du fichier de sauvegarde
     */
    public void Init() throws IOException {
        LoadState();
    }

    /**
     * Charge l'état des pages (utilisées / libres) depuis les fichiers de données
     * et le fichier dm.save.
     * 
     * dm.save contient une ligne par page libre : "fileIdx,pageIdx"
     */
    private void LoadState() throws IOException {
        int maxFiles = config.getMaxFileCount();
        int pageSize = config.getPageSize();

        // 1) Initialiser les bitmaps : toutes les pages existantes sont utilisées (bit = 1)
        for (int fileIdx = 0; fileIdx < maxFiles; fileIdx++) {
            File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
            BitSet bitmap = new BitSet();
            usedPages[fileIdx] = bitmap;

            if (!f.exists()) {
                continue;
            }

            long length = f.length();
            if (length <= 0) {
                continue;
            }

            int pageCount = (int) (length / pageSize);
            bitmap.set(0, pageCount); // toutes ces pages sont utilisées par défaut
        }

        // 2) Lire dm.save pour récupérer les pages libres (bit = 0)
        File saveFile = new File(config.getPath(), "dm.save");
        if (!saveFile.exists()) {
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(saveFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                
                if (parts.length == 2) {
                    int fileIdx = Integer.parseInt(parts[0].trim());
                    int pageIdx = Integer.parseInt(parts[1].trim());

                    if (fileIdx < 0 || fileIdx >= maxFiles) {
                        continue;
                    }

                    File f = new File(config.getPath(), "Data" + fileIdx + ".bin");
                    if (!f.exists()) {
                        continue;
                    }

                    long length = f.length();
                    int pageCount = (int) (length / pageSize);
                    if (pageIdx < 0 || pageIdx >= pageCount) {
                        continue; // entrée invalide, on ignore
                    }

                    BitSet bitmap = getOrCreateBitmap(fileIdx);
                    bitmap.clear(pageIdx); // cette page devient libre (0)
                }
            }
        }
    }

    /**
     * Retourne l'objet File correspondant au PageId fourni.
     * Vérifie que le fichier existe sur le disque.
     * 
     * @param pageId identifiant de la page
     * @return objet File correspondant au fichier contenant cette page
     * @throws IOException si le fichier n'existe pas
     */
    private File getFile(PageId pageId) throws IOException {
        File f = new File(config.getPath(), "Data" + pageId.getFileIdx() + ".bin");
        
        if (!f.exists()) {
            throw new IOException("Fichier inexistant : " + f.getAbsolutePath());
        }
        
        return f;
    }

    /**
     * Calcule l'offset (position en octets) d'une page dans son fichier.
     * Vérifie que la page existe réellement dans le fichier.
     * 
     * @param pageId identifiant de la page
     * @param f fichier contenant la page
     * @return position en octets du début de la page dans le fichier
     * @throws IOException si la page dépasse la taille actuelle du fichier
     */
    private long getOffset(PageId pageId, File f) throws IOException {
        long offset = (long) pageId.getPageIdx() * config.getPageSize();

        if (offset + config.getPageSize() > f.length()) {
            throw new IOException("Page " + pageId.getPageIdx() + 
                    " inexistante dans le fichier " + f.getName());
        }
        
        return offset;
    }

    /**
     * Récupère (ou crée) la bitmap d'un fichier donné.
     */
    private BitSet getOrCreateBitmap(int fileIdx) {
        BitSet bitmap = usedPages[fileIdx];
        if (bitmap == null) {
            bitmap = new BitSet();
            usedPages[fileIdx] = bitmap;
        }
        return bitmap;
    }

}
