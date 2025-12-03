package bdda;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gestionnaire de la base de donnees
 * Gere toutes les relations (tables) de la base
 */
public class DBManager {
    
    private DBConfig config;
    private DiskManager diskManager;
    private BufferManager bufferManager;
    
    // Structure pour stocker les relations (nom -> Relation)
    private Map<String, Relation> tables;
    
    // Nom du fichier de sauvegarde
    private static final String SAVE_FILE = "database.save";
    
    /**
     * Constructeur
     * @param config configuration de la base de donnees
     */
    public DBManager(DBConfig config) throws IOException {
        this.config = config;
        this.diskManager = new DiskManager(config);
        this.bufferManager = new BufferManager(config, diskManager);
        this.tables = new HashMap<>();
    }
    
    /**
     * Retourne le DiskManager
     */
    public DiskManager getDiskManager() {
        return diskManager;
    }
    
    /**
     * Retourne le BufferManager
     */
    public BufferManager getBufferManager() {
        return bufferManager;
    }
    
    /**
     * Retourne la configuration
     */
    public DBConfig getConfig() {
        return config;
    }
    
}