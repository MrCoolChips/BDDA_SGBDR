package bdda;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Représente une relation (table) avec son schéma
 * Gère l'écriture et la lecture des records dans les buffers
 */
public class Relation {
    
    private String name;
    private List<ColumnInfo> columns;
    
    /**
     * Constructeur avec liste de ColumnInfo
     * @param name nom de la relation
     * @param columns liste des colonnes
     */
    public Relation(String name, List<ColumnInfo> columns) {
        this.name = name;
        this.columns = new ArrayList<>(columns);
    }
    
    /**
     * Constructeur avec listes séparées de noms et types
     * @param name nom de la relation
     * @param columnNames liste des noms de colonnes
     * @param columnTypes liste des types de colonnes
     */
    public Relation(String name, List<String> columnNames, List<String> columnTypes) {
        this.name = name;
        this.columns = new ArrayList<>();
        
        if (columnNames.size() != columnTypes.size()) {
            throw new IllegalArgumentException("Le nombre de noms et de types doit être identique");
        }
        
        for (int i = 0; i < columnNames.size(); i++) {
            columns.add(new ColumnInfo(columnNames.get(i), columnTypes.get(i)));
        }
    }
    
    
    public String getName() {
        return name;
    }
    
    public List<ColumnInfo> getColumns() {
        return columns;
    }
    
    public int getColumnCount() {
        return columns.size();
    }
    
    public ColumnInfo getColumn(int index) {
        return columns.get(index);
    }
    
    /**
     * Calcule la taille totale d'un record en bytes
     * @return taille en bytes
     */
    public int getRecordSize() {
        int size = 0;
        for (ColumnInfo col : columns) {
            size += col.getSizeInBytes();
        }
        return size;
    }
        
    /**
     * Écrit un record dans le buffer à la position donnée
     * Format à taille fixe : chaque valeur est écrite sur un nombre fixe de bytes
     * 
     * @param record le record à écrire
     * @param buff le buffer (ByteBuffer)
     * @param pos la position de départ dans le buffer
     */
    public void writeRecordToBuffer(Record record, ByteBuffer buff, int pos) {
        // Vérifier que le record a le bon nombre de valeurs
        if (record.size() != columns.size()) {
            throw new IllegalArgumentException(
                "Le record a " + record.size() + " valeurs mais la relation a " + columns.size() + " colonnes");
        }
        
        // Positionner le curseur
        buff.position(pos);
        
        // Écrire chaque valeur selon son type
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            Object value = record.getValue(i);
            writeValue(buff, col, value);
        }
    }
    
    /**
     * Écrit une valeur dans le buffer selon son type
     */
    private void writeValue(ByteBuffer buff, ColumnInfo col, Object value) {
        if (col.isInt()) {
            int intValue = convertToInt(value);
            buff.putInt(intValue);
            
        } else if (col.isFloat()) {
            float floatValue = convertToFloat(value);
            buff.putFloat(floatValue);
            
        } else if (col.isChar()) {
            int maxLen = col.getMaxLength();
            String strValue = convertToString(value);
            writeFixedString(buff, strValue, maxLen);
            
        } else if (col.isVarchar()) {
            int maxLen = col.getMaxLength();
            String strValue = convertToString(value);
            writeVarcharString(buff, strValue, maxLen);
        }
    }
    
    /**
     * Écrit une chaîne de taille fixe (CHAR(T))
     * Remplit avec des espaces si la chaîne est plus courte que T
     * Tronque si la chaîne est plus longue que T
     */
    private void writeFixedString(ByteBuffer buff, String str, int maxLen) {
        // Tronquer si trop long
        if (str.length() > maxLen) {
            str = str.substring(0, maxLen);
        }
        
        // Écrire les caractères
        for (int i = 0; i < str.length(); i++) {
            buff.put((byte) str.charAt(i));
        }
        
        // Remplir avec des espaces (padding)
        for (int i = str.length(); i < maxLen; i++) {
            buff.put((byte) ' ');
        }
    }
    
    /**
     * Écrit une chaîne de taille variable (VARCHAR(T))
     * Format : 4 bytes pour la longueur réelle + T bytes pour les caractères
     * Les caractères non utilisés sont remplis avec des espaces
     */
    private void writeVarcharString(ByteBuffer buff, String str, int maxLen) {
        // Tronquer si trop long
        if (str.length() > maxLen) {
            str = str.substring(0, maxLen);
        }
        
        // Écrire la longueur réelle (4 bytes)
        buff.putInt(str.length());
        
        // Écrire les caractères
        for (int i = 0; i < str.length(); i++) {
            buff.put((byte) str.charAt(i));
        }
        
        // Remplir avec des espaces jusqu'à maxLen (padding)
        for (int i = str.length(); i < maxLen; i++) {
            buff.put((byte) ' ');
        }
    }
        
    /**
     * Lit un record depuis le buffer à la position donnée
     * Le record passé en paramètre sera rempli avec les valeurs lues
     * 
     * @param record le record à remplir (doit être vide)
     * @param buff le buffer (ByteBuffer)
     * @param pos la position de départ dans le buffer
     */
    public void readFromBuffer(Record record, ByteBuffer buff, int pos) {
        // Vider le record au cas où
        record.clear();
        
        // Positionner le curseur
        buff.position(pos);
        
        // Lire chaque valeur selon son type
        for (int i = 0; i < columns.size(); i++) {
            ColumnInfo col = columns.get(i);
            Object value = readValue(buff, col);
            record.addValue(value);
        }
    }
    
    /**
     * Lit une valeur depuis le buffer selon son type
     */
    private Object readValue(ByteBuffer buff, ColumnInfo col) {
        if (col.isInt()) {
            return buff.getInt();
            
        } else if (col.isFloat()) {
            return buff.getFloat();
            
        } else if (col.isChar()) {
            int maxLen = col.getMaxLength();
            return readFixedString(buff, maxLen);
            
        } else if (col.isVarchar()) {
            int maxLen = col.getMaxLength();
            return readVarcharString(buff, maxLen);
        }
        
        return null;
    }
    
    /**
     * Lit une chaîne de taille fixe (CHAR(T))
     * Supprime les espaces de fin (trailing spaces)
     */
    private String readFixedString(ByteBuffer buff, int maxLen) {
        StringBuilder sb = new StringBuilder();
        
        for (int i = 0; i < maxLen; i++) {
            char c = (char) buff.get();
            sb.append(c);
        }
        
        // Supprimer les espaces de fin
        return sb.toString().stripTrailing();
    }
    
    /**
     * Lit une chaîne de taille variable (VARCHAR(T))
     * Lit d'abord la longueur, puis les caractères
     */
    private String readVarcharString(ByteBuffer buff, int maxLen) {
        // Lire la longueur réelle (4 bytes)
        int realLength = buff.getInt();
        
        StringBuilder sb = new StringBuilder();
        
        // Lire uniquement les caractères réels
        for (int i = 0; i < realLength; i++) {
            char c = (char) buff.get();
            sb.append(c);
        }
        
        // Sauter les espaces de padding
        for (int i = realLength; i < maxLen; i++) {
            buff.get(); // ignorer
        }
        
        return sb.toString();
    }
        
    /**
     * Convertit une valeur en int
     */
    private int convertToInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new IllegalArgumentException("Impossible de convertir en INT : " + value);
    }
    
    /**
     * Convertit une valeur en float
     */
    private float convertToFloat(Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            return ((Double) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        throw new IllegalArgumentException("Impossible de convertir en FLOAT : " + value);
    }
    
    /**
     * Convertit une valeur en String
     */
    private String convertToString(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Relation '").append(name).append("' (");
        sb.append(getRecordSize()).append(" bytes/record) {\n");
        for (ColumnInfo col : columns) {
            sb.append("  ").append(col).append(" (").append(col.getSizeInBytes()).append(" bytes)\n");
        }
        sb.append("}");
        return sb.toString();
    }
}