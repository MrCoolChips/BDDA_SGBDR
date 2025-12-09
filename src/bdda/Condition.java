package bdda;

import java.util.List;

/**
 * Represente une condition dans une clause WHERE
 * Format : Terme1 OP Terme2
 * OP peut etre : =, <, >, <=, >=, <>
 */
public class Condition {
    
    // Operateurs possibles
    public static final String OP_EQUAL = "=";
    public static final String OP_NOT_EQUAL = "<>";
    public static final String OP_LESS = "<";
    public static final String OP_GREATER = ">";
    public static final String OP_LESS_EQUAL = "<=";
    public static final String OP_GREATER_EQUAL = ">=";
    
    // Terme gauche : soit un indice de colonne (-1 si c'est une constante)
    private int leftColIndex;
    private Object leftConstant;
    
    // Operateur
    private String operator;
    
    // Terme droit : soit un indice de colonne (-1 si c'est une constante)
    private int rightColIndex;
    private Object rightConstant;
    
    /**
     * Constructeur
     * @param leftColIndex indice de la colonne gauche (-1 si constante)
     * @param leftConstant valeur constante gauche (null si colonne)
     * @param operator operateur de comparaison
     * @param rightColIndex indice de la colonne droite (-1 si constante)
     * @param rightConstant valeur constante droite (null si colonne)
     */
    public Condition(int leftColIndex, Object leftConstant, 
                     String operator,
                     int rightColIndex, Object rightConstant) {
        this.leftColIndex = leftColIndex;
        this.leftConstant = leftConstant;
        this.operator = operator;
        this.rightColIndex = rightColIndex;
        this.rightConstant = rightConstant;
    }
}