package ru.webgrozny.iql;

import ru.webgrozny.iql.exceptions.*;
import ru.webgrozny.iql.queryfilter.QueryFilter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class IQL {
    public static final String EQUAL = "=";
    public static final String NOT_EQUAL = "!=";
    public static final String MORE = ">";
    public static final String MORENEQUAL = ">=";
    public static final String LESSNEQUAL = "<=";
    public static final String LESS = "<";
    public static final String ISNULL = "isnull";
    public static final String ISNTNULL = "isntnull";
    public static final String LIKE = "like";

    public static final String JOIN_FULL = "full";
    public static final String JOIN_LEFT = "left";
    public static final String JOIN_RIGHT = "right";

    public static final String JOIN_OUTER = "outer";
    public static final String JOIN_INNER = "inner";

    public static final String ASC = "asc";
    public static final String DESC = "desc";

    private static StringFilter stringParser = (s) -> s;
    private static StringFilter textParser = (s) -> s;
    private static String dateFormat = "dd.MM.yyyy";

    private Connection con;
    private List<String> tables;
    private Operation opType;
    private Field[] modifyingFields;
    private List<Object[]> insertableData;
    private Object[] updateData;
    private StringBuilder sql;
    private List<PreparedData> preparedQueryData;
    private List<PreparedData> preparedWhereData;
    private StringBuilder where;
    private int openBracketsCnt;
    private int currentTableIndex;
    private List<Field> createFields;
    private List<SelectedField> selectedFields;
    private List<String> excludedTables;
    private List<Join> joins;
    private List<Order> orders;
    private List<Group> groups;
    private String limit;
    private String selectRaw;
    private boolean whereOr = false;
    private String codepage = "utf8";

    public IQL(Connection con) {
        reset();
        setConnection(con);
    }

    public IQL() {
        this(null);
    }

    /**
     * Will reset object
     */
    public void reset() {
        opType = Operation.NOT_SET;
        preparedWhereData = new ArrayList<>();
        createFields = new ArrayList<>();
        tables = new ArrayList<>();
        insertableData = new ArrayList<>();
        openBracketsCnt = 0;
        currentTableIndex = 0;
        where = new StringBuilder();
        selectedFields = new ArrayList<>();
        excludedTables = new ArrayList<>();
        joins = new ArrayList<>();
        orders = new ArrayList<>();
        groups = new ArrayList<>();
        limit = null;
        selectRaw = null;
    }

    public void setCodepage(String codepage) {
        this.codepage = codepage;
    }

    /**
     * 
     * @param con Connection to database
     * @return this
     */
    public IQL setConnection(Connection con) {
        this.con = con;
        return this;
    }

    /**
     * @param sf Filter for %s type, before inserting to query
     */
    public static void setStringFilter(StringFilter sf) {
        stringParser = sf;
    }

    /**
     * @param sf Filter for %t type, before inserting to query
     */
    public static void setTextFilter(StringFilter sf) {
        textParser = sf;
    }

    /**
     * @param format Date format for %d, before inserting to query (Default: dd.MM.yyyy)
     */
    public static void setDateFormat(String format) {
        dateFormat = format;
    }

    private class Field {
        String name;
        DataType type;

        Field(String name, DataType type) {
            this.name = name;
            this.type = type;
        }
    }

    private class PreparedData {
        Object data;
        DataType type;

        PreparedData(Object data, DataType type) {
            this.data = data;
            this.type = type;
        }
    }

    private class SelectedField {
        String table;
        String field;
        String alias;

        SelectedField(String field, int table) {
            this.table = tables.get(table - 1);
            if (field.indexOf(' ') != -1) {
                String[] fieldVals = field.split(" ");
                this.field = fieldVals[0];
                this.alias = fieldVals[1];
            } else {
                this.field = field;
                this.alias = this.table + "_" + this.field;
            }
        }

        SelectedField(String field) {
            this(field, currentTableIndex + 1);
        }
    }

    private class Join {
        String table1;
        String field1;
        String table2;
        String field2;
        String side;
        String type;

        Join(int table1, String field1, int table2, String field2, String side, String type) {
            this.table1 = tables.get(table1 - 1);
            this.table2 = tables.get(table2 - 1);
            this.field1 = field1;
            this.field2 = field2;
            this.side = side != null ? side : "";
            this.type = type != null ? type : "";
        }

        public String getCommand() {
            String propSide;
            String propType;
            switch (side) {
                case JOIN_LEFT:
                    propSide = " LEFT";
                    break;
                case JOIN_RIGHT:
                    propSide = " RIGHT";
                    break;
                case JOIN_FULL:
                    propSide = " FULL";
                    break;
                default:
                    propSide = "";
            }
            switch (type) {
                case JOIN_INNER:
                    propType = " INNER";
                    break;
                case JOIN_OUTER:
                    propType = " OUTER";
                    break;
                default:
                    propType = "";
            }

            String ret = propSide + propType + " JOIN `" + table2 + "` ON `" + table1 + "`.`" + field1 + "` = `" + table2 + "`.`" + field2 + "`";
            return ret;
        }
    }

    private class Order {
        String field;
        String type;
        int table;

        Order(String field, String type, int table) {
            this.field = field;
            this.type = type;
            this.table = table - 1;
        }

        Order(String field, int table) {
            this(field, ASC, table);
        }

        Order(String field, String type) {
            this(field, type, currentTableIndex + 1);
        }

        public String toString() {
            String table = tables.get(this.table);
            String type = this.type.equals(DESC) ? "DESC" : "ASC";
            String ret = "`" + table + "`.`" + field + "` " + type;
            return ret;
        }
    }

    private class Group {
        int table;
        String field;

        Group(String field, int table) {
            this.field = field;
            this.table = table - 1;
        }

        Group(String field) {
            this(field, currentTableIndex + 1);
        }

        public String toString() {
            String table = tables.get(this.table);
            return "`" + table + "`.`" + field + "`";
        }
    }

    /**
     * @param field Field name in format "name %s", where %s is type signature
     * @return Field object
     * @throws RowFormatException if type signature not found
     */
    private Field parseField(String field) throws RowFormatException {
        String name;
        DataType type;
        int delimiterIndex = field.lastIndexOf('%');
        if (delimiterIndex == field.length() - 2) {
            name = field.substring(0, delimiterIndex - 1);
            char typeChar = field.charAt(field.length() - 1);
            switch (typeChar) {
                case 's':
                    type = DataType.RT_S;
                    break;
                case 'v':
                    type = DataType.RT_V;
                    break;
                case 't':
                    type = DataType.RT_T;
                    break;
                case 'i':
                    type = DataType.RT_I;
                    break;
                case 'b':
                    type = DataType.RT_B;
                    break;
                case 'd':
                    type = DataType.RT_D;
                    break;
                case 'f':
                    type = DataType.RT_F;
                    break;
                default:
                    throw new RowFormatException();
            }
            return new Field(name, type);
        }
        throw new RowFormatException();
    }

    /**
     * Used for type %v
     * @param str Object, that contains string to parse
     * @return parsed string
     */
    private String parseString(Object str) {
        return (String) str;
    }

    /**
     * Used for type %s
     * @param str Object, that contains string to parse and filter with StringFilter object, set by setStringFilter()
     * @return parsed string
     */
    private String parseStringFilter(Object str) { //RT_S
        return stringParser.filter((String) str);
    }

    /**
     * Used for type %t
     * @param str Object, that contains string to parse and filter with StringFilter object, set by setTextFilter();
     * @return parsed string
     */
    private String parseText(Object str) { //RT_T
        return textParser.filter((String) str);
    }

    /**
     * Used for type %i
     * @param intVal Object, that contains Integer or String with integer value
     * @return parsed integer
     */
    private int parseInt(Object intVal) {
        int ret;
        try {
            ret = (Integer) intVal;
        } catch (ClassCastException e) {
            try {
                ret = Integer.parseInt(intVal.toString());
            } catch (NumberFormatException e2) {
                ret = 0;
            }
        }
        return ret;
    }

    /**
     * Used for type %b
     * @param booleanVal Object, that contains Boolean or String with boolean value
     * @return parsed boolean
     */
    private boolean parseBoolean(Object booleanVal) {
        boolean ret;
        try {
            ret = (Boolean) booleanVal;
        } catch (ClassCastException e) {
            ret = Boolean.parseBoolean(booleanVal.toString());
        }
        return ret;
    }

    /**
     * Used for %d
     * @param date Object, that contains Integer, String or Date with inserting date
     * @return parsed integer date timestamp
     */
    private int parseDate(Object date) {
        int ret;
        try {
            ret = (Integer) date;
        } catch (ClassCastException e) {
            Date d;
            try {
                d = (Date) date;
            } catch (ClassCastException e1) {
                try {
                    String sDate = (String) date;
                    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
                    d = sdf.parse(sDate);
                } catch (ClassCastException e2) {
                    throw new RowFormatException();
                } catch (ParseException e3) {
                    throw new RowFormatException();
                }
            }
            long time = d.getTime();
            ret = (int) (time / 1000l);
        }

        return ret;
    }

    /**
     * Used for %f
     * @param data Object, that contains Float or String value
     * @return parsed float
     */
    private float parseFloat(Object data){
        float ret;
        try {
            ret = (Float) data;
        } catch (ClassCastException e0){
            try {
                ret = Float.parseFloat(data.toString());
            }catch (NumberFormatException e1){
                throw new RowFormatException();
            }
        }
        return ret;
    }

    /**
     * Data for preparator
     * @param field field to get type data
     * @param data data, which will be parsed with field type
     * @return Object, that contains right data type for set field type
     */
    private Object prepareForRow(Field field, Object data) {
        switch (field.type) {
            case RT_S:
                return parseStringFilter(data);
            case RT_V:
                return parseString(data);
            case RT_T:
                return parseText(data);
            case RT_I:
                return parseInt(data);
            case RT_B:
                return parseBoolean(data);
            case RT_D:
                return parseDate(data);
            case RT_F:
                return parseFloat(data);
            default:
                throw new RowFormatException();
        }
    }

    /**
     * @param tables tables, which will be added to query
     * @return this
     */
    public IQL addTable(String... tables) {
        this.tables.addAll(Arrays.asList(tables));
        if(tables.length == 1){
            currentTableIndex = this.tables.size() - 1;
        }
        return this;
    }

    /**
     * Sets active table
     * @param index active table index, started from 1
     * @return this
     */
    public IQL setTable(int index) {
        currentTableIndex = index - 1;
        return this;
    }

    /**
     * Method, that is called by setInsertFields and setUpdateFields, which will set fields, to add or update data
     * @param fields fields to modificate. Must be set with type signature
     */
    private void setModifyingFields(String[] fields) {
        modifyingFields = new Field[fields.length];
        int i = 0;
        for (String field : fields) {
            modifyingFields[i++] = parseField(field);
        }
    }

    /**
     * Setting fields for data insert
     * @param fields fields with type signature to insert
     * @return this
     */
    public IQL setInsertFields(String... fields) {
        opType = Operation.INSERT;
        setModifyingFields(fields);
        return this;
    }

    /**
     * Inserts data to declared with setInsertFields() fields
     * @param data data to insert
     * @return this
     */
    public IQL insert(Object... data) {
        if (data.length == modifyingFields.length) {
            Object[] toInsert = new Object[data.length];
            for (int i = 0; i < data.length; i++) {
                toInsert[i] = prepareForRow(modifyingFields[i], data[i]);
            }
            insertableData.add(toInsert);
            return this;
        }
        throw new DataRowsCountMismatchException();
    }

    /**
     * Setting fields to data update
     * @param fields field names with type signature to update
     * @return this
     */
    public IQL setUpdateFields(String... fields) {
        opType = Operation.UPDATE;
        setModifyingFields(fields);
        return this;
    }

    /**
     * Updates data in declared with setUpdateFields() fields
     * @param data data to insert
     * @return this
     */
    public IQL update(Object... data) {
        if (data.length == modifyingFields.length) {
            Object[] update = new Object[data.length];
            for (int i = 0; i < data.length; i++) {
                update[i] = prepareForRow(modifyingFields[i], data[i]);
            }
            updateData = update;
            return this;
        }
        throw new DataRowsCountMismatchException();
    }

    /**
     * Setting fields to update or insert
     * @param fields field names with type signature
     * @return this
     */
    public IQL setUpsertFields(String... fields) {
        opType = Operation.UPSERT;
        setModifyingFields(fields);
        return this;
    }

    /**
     * Setting data to update or insert
     * @param data data to insert or update
     * @return this
     */
    public IQL upsert(Object... data) {
        insert(data);
        update(data);
        return this;
    }

    /**
     * DELETE field with id
     * @param id field id to delete
     * @return this
     */
    public IQL delete(int id) {
        if (id != -1) {
            whereId(id);
        }
        opType = Operation.DELETE;
        return this;
    }

    /**
     * Creating delete query
     * @return this
     */
    public IQL delete() {
        return delete(-1);
    }

    /**
     * Table creating
     * @param tableName Table name to create
     * @param fields Rows to create in table
     * @return this
     */
    public IQL createTable(String tableName, String... fields) {
        createTable(tableName);
        addField(fields);
        return this;
    }

    /**
     * Adding fields to creating table
     * @param fields field names with type signature
     * @return
     */
    public IQL addField(String... fields) {
        for (String field : fields) {
            addField(field);
        }
        return this;
    }

    /**
     * Table create
     * @param tableName table name to create
     * @return this
     */
    public IQL createTable(String tableName) {
        opType = Operation.CREATE;
        addTable(tableName);
        return this;
    }

    /**
     * Adding field to creating table
     * @param field field name with type signature
     * @return
     */
    public IQL addField(String field) {
        createFields.add(parseField(field));
        return this;
    }

    /**
     * Selects fields from table
     * @param fields field to select
     * @return this
     */
    public IQL select(String... fields) {
        opType = Operation.SELECT;
        for (String field : fields) {
            selectedFields.add(new SelectedField(field));
        }
        return this;
    }

    /**
     * Raw select command, for example for COUNT(*)
     * @param select select command
     * @return this
     */
    public IQL selectRaw(String select) {
        opType = Operation.SELECT;
        this.selectRaw = select;
        return this;
    }

    /**
     * Opens bracket in where
     * @return this
     */
    public IQL openBracket() {
        openBracketsCnt++;
        return this;
    }

    /**
     * Closing bracket in where
     * @return this
     */
    public IQL closeBracket() {
        where.append(')');
        return this;
    }

    /**
     * Where statement
     * @param what what compare
     * @param operation compare operation (Some of EQUAL, NOT_EQUAL, MORE, MORENEQUAL, LESSNEQUAL, LESS, ISNULL, ISNTNULL, LIKE)
     * @param value value to compare with
     * @return this
     */
    public IQL where(String what, String operation, Object value) {
        String cOperation;
        boolean withoutData = false;
        switch (operation) {
            case EQUAL:
                cOperation = "=";
                break;
            case NOT_EQUAL:
                cOperation = "<>";
                break;
            case MORE:
                cOperation = ">";
                break;
            case MORENEQUAL:
                cOperation = ">=";
                break;
            case LESS:
                cOperation = "<";
                break;
            case LESSNEQUAL:
                cOperation = "<=";
                break;
            case ISNULL:
                cOperation = "IS NULL";
                withoutData = true;
                break;
            case ISNTNULL:
                cOperation = "IS NOT NULL";
                withoutData = true;
                break;
            case LIKE:
                cOperation = "LIKE";
                break;
            default:
                cOperation = "=";
        }

        Field field = parseField(what);
        Object data = null;
        if (!withoutData) {
            data = prepareForRow(field, value);
        }
        String table = tables.get(currentTableIndex);
        if (where.length() > 0) {
            where.append(" " + (whereOr ? "OR" : "AND") + " ");
        } else {
            where.append(" WHERE ");
        }
        and();
        for (int i = 0; i < openBracketsCnt; i++) {
            where.append('(');
        }
        openBracketsCnt = 0;

        if (withoutData) {
            where.append("`" + table + "`.`" + field.name + "` " + cOperation);
        } else {
            where.append("`" + table + "`.`" + field.name + "` " + cOperation + " ?");
            preparedWhereData.add(new PreparedData(data, field.type));
        }
        return this;
    }

    /**
     * OR
     * @return this
     */
    public IQL or() {
        whereOr = true;
        return this;
    }

    /**
     * AND
     * @return this
     */
    public IQL and() {
        whereOr = false;
        return this;
    }

    /**
     * where statement without value (for isnull, isntnull)
     * @param what field to compare
     * @param operation operation (ISNULL, ISNTNULL)
     * @return this
     */
    public IQL where(String what, String operation) {
        return where(what, operation, null);
    }

    /**
     * Comparing id with value from current active table
     * @param value id
     * @return this
     */
    public IQL whereId(int value) {
        return where("id %i", "=", value);
    }

    /**
     * Grouping data
     * @param field field to group
     * @param table table, that contains field
     * @return this
     */
    public IQL groupBy(String field, int table) {
        groups.add(new Group(field, table));
        return this;
    }

    /**
     * Grouping data
     * @param field field from current active table to group
     * @return this
     */
    public IQL groupBy(String field) {
        groups.add(new Group(field));
        return this;
    }

    /**
     * Ordering data
     * @param field field for order
     * @param type ordering type (ASC or DESC)
     * @param table table, that contains field
     * @return this
     */
    public IQL orderBy(String field, String type, int table) {
        orders.add(new Order(field, type, table));
        return this;
    }

    /**
     * Ordering data
     * @param field field for order
     * @param table table, that contains field
     * @return this
     */
    public IQL orderBy(String field, int table) {
        orders.add(new Order(field, table));
        return this;
    }

    /**
     * Ordering data
     * @param field field from current active table
     * @param type ordering type (ASC or DESC)
     * @return
     */
    public IQL orderBy(String field, String type) {
        orders.add(new Order(field, type));
        return this;
    }

    /**
     * Setting limits
     * @param from limit from
     * @param to limit to
     * @return this
     */
    public IQL limit(int from, int to) {
        limit = " LIMIT " + from + ", " + to;
        return this;
    }

    /**
     * Setting limit from zero to limit
     * @param limit number of elements
     * @return this
     */
    public IQL limit(int limit) {
        return limit(0, limit);
    }

    /**
     * Joining tables
     * @param index1 table to join to
     * @param field1 field name for join to
     * @param index2 joining table index
     * @param field2 joining field name
     * @param side join side (LEFT, RIGHT, FULL)
     * @param type join type (INNER, OUTER)
     * @return this
     */
    public IQL join(int index1, String field1, int index2, String field2, String side, String type) {
        excludedTables.add(tables.get(index2 - 1));
        joins.add(new Join(index1, field1, index2, field2, side, type));
        return this;
    }

    /**
     * Joining tables
     * @param index1 table to join to
     * @param field1 field name for join to
     * @param index2 joining table index
     * @param field2 joining field name
     * @param typeOrSide join side (LEFT, RIGHT, FULL) or type (INNER, OUTER)            
     * @return this
     */
    public IQL join(int index1, String field1, int index2, String field2, String typeOrSide) {
        if (typeOrSide.equals(JOIN_OUTER) || typeOrSide.equals(JOIN_INNER)) {
            return join(index1, field1, index2, field2, null, typeOrSide);
        } else {
            return join(index1, field1, index2, field2, typeOrSide, null);
        }
    }

    /**
     * Joining tables
     * @param index1 table to join to
     * @param field1 field name for join to
     * @param index2 joining table index
     * @param field2 joining field name
     * @return this
     */
    public IQL join(int index1, String field1, int index2, String field2) {
        return join(index1, field1, index2, field2, null, null);
    }

    /**
     * Getting PreparedStatement for built query
     * @return PreparedStatement with query set
     */
    public PreparedStatement getStatement() {
        if (con == null) {
            throw new ConnectionNotSetException();
        }
        compileQuery();
        try {
            PreparedStatement ps = con.prepareStatement(sql.toString());

            int i = 1;
            for (PreparedData preparedData : this.preparedQueryData) {
                switch (preparedData.type) {
                    case RT_S:
                    case RT_V:
                    case RT_T:
                        ps.setString(i++, (String) preparedData.data);
                        break;
                    case RT_I:
                    case RT_D:
                        ps.setInt(i++, (int) preparedData.data);
                        break;
                    case RT_B:
                        ps.setBoolean(i++, (boolean) preparedData.data);
                        break;
                    case RT_F:
                        ps.setFloat(i++, (float) preparedData.data);
                }
            }
            reset();
            return ps;
        } catch (SQLException e) {
            return null;
        }
    }

    /**
     * Build query to String 
     * @return String with buil query
     */
    public String getSQL() {
        compileQuery();
        QueryFilter qf = new QueryFilter(sql.toString());
        for (PreparedData preparedData : preparedQueryData) {
            switch (preparedData.type) {
                case RT_S:
                case RT_V:
                case RT_T:
                    qf.setString((String) preparedData.data);
                    break;
                case RT_I:
                case RT_D:
                    qf.setInt((int) preparedData.data);
                    break;
                case RT_B:
                    qf.setBoolean((boolean) preparedData.data);
                    break;
                case RT_F:
                    qf.setFloat((float) preparedData.data);
            }
        }
        reset();
        return qf.toString();
    }

    private void compileQuery() {
        preparedQueryData = new ArrayList<>();
        sql = new StringBuilder();
        switch (opType) {
            case INSERT:
                compileInsert();
                break;
            case UPDATE:
                compileUpdate();
                break;
            case DELETE:
                compileDelete();
                break;
            case CREATE:
                compileCreate();
                break;
            case SELECT:
                compileSelect();
                break;
            case UPSERT:
                compileUpsert();
                break;
            default:
                throw new OperationNotSetException();
        }
        if ((opType == Operation.UPDATE || opType == Operation.DELETE) && where.length() == 0) {
            throw new InsecureOperationException();
        }
        sql.append(where);

        if (groups.size() > 0) {
            sql.append(" GROUP BY");
            for (Group group : groups) {
                sql.append(" " + group + ",");
            }
            sql.deleteCharAt(sql.length() - 1);
        }

        if (orders.size() > 0) {
            sql.append(" ORDER BY");
            for (Order order : orders) {
                sql.append(" " + order + ",");
            }
            sql.deleteCharAt(sql.length() - 1);
        }
        if (limit != null) {
            sql.append(limit);
        }
        preparedQueryData.addAll(preparedWhereData);
    }

    private void compileInsert() {
        sql.append("INSERT INTO `" + tables.get(0) + "`(`" + modifyingFields[0].name + "`");
        for (int i = 1; i < modifyingFields.length; i++) {
            sql.append(", `" + modifyingFields[i].name + "`");
        }
        sql.append(") VALUES");
        for (Object[] line : insertableData) {
            sql.append(" (?");
            preparedQueryData.add(new PreparedData(line[0], modifyingFields[0].type));
            for (int i = 1; i < line.length; i++) {
                sql.append(", ?");
                preparedQueryData.add(new PreparedData(line[i], modifyingFields[i].type));
            }
            sql.append("),");
        }
        sql.deleteCharAt(sql.length() - 1);
    }

    private void compileUpdate() {
        sql.append("UPDATE `" + tables.get(0) + "` SET");
        int i = 0;
        for (Field cField : modifyingFields) {
            sql.append(" `" + cField.name + "` = ?,");
            preparedQueryData.add(new PreparedData(updateData[i++], cField.type));
        }
        sql.deleteCharAt(sql.length() - 1);
    }

    private void compileUpsert() {
        if (where.length() == 0) {
            compileInsert();
        } else {
            compileUpdate();
        }
    }

    private void compileDelete() {
        sql.append("DELETE FROM `" + tables.get(0) + "`");
    }

    private String getRowCreateCmd(Field field) {
        StringBuilder ret = new StringBuilder("`" + field.name + "` ");
        switch (field.type) {
            case RT_B:
                ret.append("BOOL");
                break;
            case RT_D:
            case RT_I:
                ret.append("INTEGER");
                break;
            case RT_V:
            case RT_S:
                ret.append("VARCHAR(255)");
                break;
            case RT_T:
                ret.append("TEXT");
                break;
            case RT_F:
                ret.append("FLOAT");
                break;
        }
        return ret.toString();
    }

    private void compileCreate() {
        sql.append("CREATE TABLE IF NOT EXISTS `" + tables.get(0) + "`(`id` INTEGER PRIMARY KEY AUTO_INCREMENT,");
        for (Field cField : createFields) {
            sql.append(" " + getRowCreateCmd(cField) + ",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") DEFAULT CHARSET=" + codepage);
    }

    private void compileSelect() {
        sql.append("SELECT");
        if (selectedFields.size() > 0) {
            for (SelectedField field : selectedFields) {
                sql.append(" `" + field.table + "`.`" + field.field + "` AS `" + field.alias + "`,");
            }
            sql.deleteCharAt(sql.length() - 1);
        } else {
            if (selectRaw != null) {
                sql.append(" " + selectRaw);
            } else {
                sql.append(" *");
            }
        }
        sql.append(" FROM");
        for (String table : tables) {
            if (!excludedTables.contains(table)) {
                sql.append(" `" + table + "`,");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        for (Join join : joins) {
            sql.append(join.getCommand());
        }
    }
}