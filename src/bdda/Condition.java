package bdda;

import java.util.List;

<<<<<<< HEAD
public class Condition {

    public enum Operator {
        EQ("="),
        NEQ("<>"),
        LT("<"),
        LE("<="),
        GT(">"),
        GE(">=");

        private final String symbol;

        Operator(String symbol) {
            this.symbol = symbol;
        }

        public static Operator fromString(String op) {
            switch (op) {
                case "=":  return EQ;
                case "<>": return NEQ;
                case "<":  return LT;
                case "<=": return LE;
                case ">":  return GT;
                case ">=": return GE;
                default:
                    throw new IllegalArgumentException("Operateur inconnu : " + op);
            }
        }

        public String getSymbol() {
            return symbol;
        }
    }

    // null si le terme est une constante
    private final Integer leftColumnIndex;
    private final Integer rightColumnIndex;

    // utilisés si le terme est une constante
    private final Object leftConstant;
    private final Object rightConstant;

    private final Operator operator;

    private Condition(Integer leftColumnIndex, Object leftConstant, Integer rightColumnIndex, Object rightConstant, Operator operator) {

        this.leftColumnIndex = leftColumnIndex;
        this.rightColumnIndex = rightColumnIndex;
        this.leftConstant = leftConstant;
        this.rightConstant = rightConstant;
        this.operator = operator;
    }

    /**
     * Fabrique une condition "col OP constante"
     * Exemple : C1 = 3  ->  columnIndex=0, constant=3
     * constantOnRight = true pour "col OP val"
     * constantOnRight = false pour "val OP col"
     */
    public static Condition columnVsConstant(int columnIndex, Operator op, Object constant, boolean constantOnRight) {
        if (constantOnRight) {
            // col OP constant
            return new Condition(columnIndex, null, null, constant, op);
        } else {
            // constant OP col
            return new Condition(null, constant, columnIndex, null, op);
        }
    }

    /**
     * Fabrique une condition "col OP col"
     * Exemple : C1 <= C2
     */
    public static Condition columnVsColumn(int leftColumnIndex, Operator op, int rightColumnIndex) {
        return new Condition(leftColumnIndex, null, rightColumnIndex, null, op);
    }

    /**
     * Evalue la condition sur un record donné.
     * @param record  record courant
     * @param columns schéma de la relation (pour connaître les types)
     * @return true si la condition est satisfaite, false sinon
     */
    public boolean evaluate(Record record, List<ColumnInfo> columns) {
        Object leftValue = (leftColumnIndex != null)
                ? record.getValue(leftColumnIndex)
                : leftConstant;

        Object rightValue = (rightColumnIndex != null)
                ? record.getValue(rightColumnIndex)
                : rightConstant;

        if (leftValue == null || rightValue == null) {
            return false;
        }

        // Déterminer le type de comparaison à partir de la colonne impliquée
        ColumnInfo typeCol = null;
        if (leftColumnIndex != null) {
            typeCol = columns.get(leftColumnIndex);
        } else if (rightColumnIndex != null) {
            typeCol = columns.get(rightColumnIndex);
        }

        int cmp = compareValues(leftValue, rightValue, typeCol);
        return applyOperator(cmp);
    }

    /**
     * Compare deux valeurs selon le type de la colonne.
     * @return valeur négative si left < right, 0 si égal, positive si left > right
     */
    private int compareValues(Object left, Object right, ColumnInfo col) {
        if (col != null && col.isInt()) {
            int l = ((Number) left).intValue();
            int r = ((Number) right).intValue();
            return Integer.compare(l, r);

        } else if (col != null && col.isFloat()) {
            double l = ((Number) left).doubleValue();
            double r = ((Number) right).doubleValue();
            return Double.compare(l, r);

        } else {
            // CHAR(T) ou VARCHAR(T) ou cas par défaut -> comparaison lexicographique
            String l = left.toString();
            String r = right.toString();
            return l.compareTo(r);
        }
    }


    // Applique l'opérateur à partir du résultat
    private boolean applyOperator(int cmp) {
        switch (operator) {
            case EQ:  return cmp == 0;
            case NEQ: return cmp != 0;
            case LT:  return cmp < 0;
            case LE:  return cmp <= 0;
            case GT:  return cmp > 0;
            case GE:  return cmp >= 0;
            default:
                throw new IllegalStateException("Operateur non géré : " + operator);
        }
    }

    public Operator getOperator() {
        return operator;
    }

    public Integer getLeftColumnIndex() {
        return leftColumnIndex;
    }

    public Integer getRightColumnIndex() {
        return rightColumnIndex;
    }

    public Object getLeftConstant() {
        return leftConstant;
    }

    public Object getRightConstant() {
        return rightConstant;
    }
}

=======
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

    /**
     * Evalue la condition sur un record
     * @param record le record a evaluer
     * @param columns les colonnes de la relation (pour connaitre les types)
     * @return true si la condition est satisfaite
     */
    public boolean evaluate(Record record, List<ColumnInfo> columns) {
        // Recuperer la valeur gauche
        Object leftValue;
        ColumnInfo leftCol = null;
        if (leftColIndex >= 0) {
            leftValue = record.getValue(leftColIndex);
            leftCol = columns.get(leftColIndex);
        } else {
            leftValue = leftConstant;
        }
        
        // Recuperer la valeur droite
        Object rightValue;
        ColumnInfo rightCol = null;
        if (rightColIndex >= 0) {
            rightValue = record.getValue(rightColIndex);
            rightCol = columns.get(rightColIndex);
        } else {
            rightValue = rightConstant;
        }
        
        // Determiner le type pour la comparaison
        ColumnInfo refCol;

        if (leftCol != null) {
            refCol = leftCol;
        } else {
            refCol = rightCol;
        }
        
        // Comparer selon le type
        return compare(leftValue, rightValue, refCol);
    }

    /**
     * Compare deux valeurs selon le type de colonne
     */
    private boolean compare(Object left, Object right, ColumnInfo col) {
        int cmp;
        
        if (col == null || col.isInt()) {
            // Comparaison entiere
            int leftInt = toInt(left);
            int rightInt = toInt(right);
            cmp = Integer.compare(leftInt, rightInt);
            
        } else if (col.isFloat()) {
            // Comparaison flottante
            float leftFloat = toFloat(left);
            float rightFloat = toFloat(right);
            cmp = Float.compare(leftFloat, rightFloat);
            
        } else {
            // Comparaison de chaines
            String leftStr = toString(left);
            String rightStr = toString(right);
            cmp = leftStr.compareTo(rightStr);
        }
        
        // Appliquer l'operateur
        switch (operator) {
            case OP_EQUAL:
                return cmp == 0;
            case OP_NOT_EQUAL:
                return cmp != 0;
            case OP_LESS:
                return cmp < 0;
            case OP_GREATER:
                return cmp > 0;
            case OP_LESS_EQUAL:
                return cmp <= 0;
            case OP_GREATER_EQUAL:
                return cmp >= 0;
            default:
                return false;
        }
    }
    
    private int toInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        return 0;
    }
    
    private float toFloat(Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof Double) {
            return ((Double) value).floatValue();
        } else if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        return 0.0f;
    }
    
    private String toString(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString();
    }
    
    @Override
    public String toString() {
        String left = (leftColIndex >= 0) ? "col[" + leftColIndex + "]" : leftConstant.toString();
        String right = (rightColIndex >= 0) ? "col[" + rightColIndex + "]" : rightConstant.toString();
        return left + " " + operator + " " + right;
    }

}
>>>>>>> 6cad22716dfbdf8388ca1475f8b747b89ed3f909
