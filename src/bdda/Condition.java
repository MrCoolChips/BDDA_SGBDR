package bdda;

import java.util.List;

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
     * Exemple : C1 = 3  →  columnIndex=0, constant=3
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
            // On ne gère pas les NULL dans ce projet → on considère que la condition échoue
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
            // CHAR(T) ou VARCHAR(T) ou cas par défaut → comparaison lexicographique
            String l = left.toString();
            String r = right.toString();
            return l.compareTo(r);
        }
    }

    /**
     * Applique l'opérateur à partir du résultat d'un compareTo-like.
     */
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

