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
    private static final int SELECT = 1;
    private static final int CREATE = 2;
    private static final int UPDATE = 3;
    private static final int INSERT = 4;
    private static final int DELETE = 5;
    private static final int UPSERT = 6;

    private static final String OR = "OR";
    private static final String AND = "AND";

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

    private static final int RT_S = 1; //varchar
    private static final int RT_V = 2; //varchar without filter
    private static final int RT_T = 3; //text
    private static final int RT_I = 4; //integer
    private static final int RT_B = 5; //boolean
    private static final int RT_D = 6; //date
    private static final int RT_F = 7; //float

    private Connection con;
    private List<String> tables;
    private int opType;
    private Row[] modifyingRows;
    private List<Object[]> insertableData;
    private Object[] updateData;
    private StringBuilder sql;
    private List<PreparedData> preparedQueryData;
    private List<PreparedData> preparedWhereData;
    private StringBuilder where;
    private String whereConcat = AND;
    private int openBracketsCnt;
    private int currentTableIndex;
    private List<Row> createRows;
    private List<SelectedRow> selectedRows;
    private List<String> excludedTables;
    private List<Join> joins;
    private List<Order> orders;
    private List<Group> groups;
    private String limit;
    private String selectRaw;

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
        opType = 0;
        preparedWhereData = new ArrayList<>();
        createRows = new ArrayList<>();
        tables = new ArrayList<>();
        insertableData = new ArrayList<>();
        openBracketsCnt = 0;
        currentTableIndex = 0;
        where = new StringBuilder();
        selectedRows = new ArrayList<>();
        excludedTables = new ArrayList<>();
        joins = new ArrayList<>();
        orders = new ArrayList<>();
        groups = new ArrayList<>();
        limit = null;
        selectRaw = null;
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

    private class Row {
        String name;
        int type;

        Row(String name, int type) {
            this.name = name;
            this.type = type;
        }
    }

    private class PreparedData {
        Object data;
        int type;

        PreparedData(Object data, int type) {
            this.data = data;
            this.type = type;
        }
    }

    private class SelectedRow {
        String table;
        String row;
        String alias;

        SelectedRow(String row, int table) {
            this.table = tables.get(table - 1);
            if (row.indexOf(' ') != -1) {
                String[] rowVals = row.split(" ");
                this.row = rowVals[0];
                this.alias = rowVals[1];
            } else {
                this.row = row;
                this.alias = this.table + "_" + this.row;
            }
        }

        SelectedRow(String row) {
            this(row, currentTableIndex + 1);
        }
    }

    private class Join {
        String table1;
        String row1;
        String table2;
        String row2;
        String side;
        String type;

        Join(int table1, String row1, int table2, String row2, String side, String type) {
            this.table1 = tables.get(table1 - 1);
            this.table2 = tables.get(table2 - 1);
            this.row1 = row1;
            this.row2 = row2;
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

            String ret = propSide + propType + " JOIN `" + table2 + "` ON `" + table1 + "`.`" + row1 + "` = `" + table2 + "`.`" + row2 + "`";
            return ret;
        }
    }

    private class Order {
        String row;
        String type;
        int table;

        Order(String row, String type, int table) {
            this.row = row;
            this.type = type;
            this.table = table - 1;
        }

        Order(String row, int table) {
            this(row, ASC, table);
        }

        Order(String row, String type) {
            this(row, type, currentTableIndex + 1);
        }

        public String toString() {
            String table = tables.get(this.table);
            String type = this.type.equals(DESC) ? "DESC" : "ASC";
            String ret = "`" + table + "`.`" + row + "` " + type;
            return ret;
        }
    }

    private class Group {
        int table;
        String row;

        Group(String row, int table) {
            this.row = row;
            this.table = table - 1;
        }

        Group(String row) {
            this(row, currentTableIndex + 1);
        }

        public String toString() {
            String table = tables.get(this.table);
            return "`" + table + "`.`" + row + "`";
        }
    }

    /**
     * @param row Row name in format "name %s", where %s is type signature
     * @return Row object
     * @throws RowFormatException if type signature not found
     */
    private Row parseRow(String row) throws RowFormatException {
        String name;
        int type;
        int delimiterIndex = row.lastIndexOf('%');
        if (delimiterIndex == row.length() - 2) {
            name = row.substring(0, delimiterIndex - 1);
            char typeChar = row.charAt(row.length() - 1);
            switch (typeChar) {
                case 's':
                    type = RT_S;
                    break;
                case 'v':
                    type = RT_V;
                    break;
                case 't':
                    type = RT_T;
                    break;
                case 'i':
                    type = RT_I;
                    break;
                case 'b':
                    type = RT_B;
                    break;
                case 'd':
                    type = RT_D;
                    break;
                case 'f':
                    type = RT_F;
                    break;
                default:
                    throw new RowFormatException();
            }
            return new Row(name, type);
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
     * @param row row to get type data
     * @param data data, which will be parsed with row type
     * @return Object, that contains right data type for set row type
     */
    private Object prepareForRow(Row row, Object data) {
        switch (row.type) {
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
     * Method, that is called by setInsertRows and setUpdateRows, which will set rows, to add or update data
     * @param rows rows to modificate. Must be set with type signature
     */
    private void setModifyingRows(String[] rows) {
        modifyingRows = new Row[rows.length];
        int i = 0;
        for (String row : rows) {
            modifyingRows[i++] = parseRow(row);
        }
    }

    /**
     * Setting rows for data insert
     * @param rows rows with type signature to insert
     * @return this
     */
    public IQL setInsertRows(String... rows) {
        opType = INSERT;
        setModifyingRows(rows);
        return this;
    }

    /**
     * Inserts data to declared with setInsertRows() rows
     * @param data data to insert
     * @return this
     */
    public IQL insert(Object... data) {
        if (data.length == modifyingRows.length) {
            Object[] toInsert = new Object[data.length];
            for (int i = 0; i < data.length; i++) {
                toInsert[i] = prepareForRow(modifyingRows[i], data[i]);
            }
            insertableData.add(toInsert);
            return this;
        }
        throw new DataRowsCountMismatchException();
    }

    /**
     * Setting rows to data update
     * @param rows row names with type signature to update
     * @return this
     */
    public IQL setUpdateRows(String... rows) {
        opType = UPDATE;
        setModifyingRows(rows);
        return this;
    }

    /**
     * Updates data in declared with setUpdateRows() rows
     * @param data data to insert
     * @return this
     */
    public IQL update(Object... data) {
        if (data.length == modifyingRows.length) {
            Object[] update = new Object[data.length];
            for (int i = 0; i < data.length; i++) {
                update[i] = prepareForRow(modifyingRows[i], data[i]);
            }
            updateData = update;
            return this;
        }
        throw new DataRowsCountMismatchException();
    }

    /**
     * Setting rows to update or insert
     * @param rows row names with type signature
     * @return this
     */
    public IQL setUpsertRows(String... rows) {
        opType = UPSERT;
        setModifyingRows(rows);
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
     * DELETE row with id
     * @param id row id to delete
     * @return this
     */
    public IQL delete(int id) {
        if (id != -1) {
            whereId(id);
        }
        opType = DELETE;
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
     * @param rows Rows to create in table
     * @return this
     */
    public IQL createTable(String tableName, String... rows) {
        createTable(tableName);
        addRow(rows);
        return this;
    }

    /**
     * Adding rows to creating table
     * @param rows row names with type signature
     * @return
     */
    public IQL addRow(String... rows) {
        for (String row : rows) {
            addRow(row);
        }
        return this;
    }

    /**
     * Table create
     * @param tableName table name to create
     * @return this
     */
    public IQL createTable(String tableName) {
        opType = CREATE;
        addTable(tableName);
        return this;
    }

    /**
     * Adding row to creating table
     * @param row row name with type signature
     * @return
     */
    public IQL addRow(String row) {
        createRows.add(parseRow(row));
        return this;
    }

    /**
     * Selects rows from table
     * @param rows row to select
     * @return this
     */
    public IQL select(String... rows) {
        opType = SELECT;
        for (String row : rows) {
            selectedRows.add(new SelectedRow(row));
        }
        return this;
    }

    /**
     * Raw select command, for example for COUNT(*)
     * @param select select command
     * @return this
     */
    public IQL selectRaw(String select) {
        opType = SELECT;
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

        Row row = parseRow(what);
        Object data = null;
        if (!withoutData) {
            data = prepareForRow(row, value);
        }
        String table = tables.get(currentTableIndex);
        if (where.length() > 0) {
            where.append(" " + whereConcat + " ");
        } else {
            where.append(" WHERE ");
        }
        and();
        for (int i = 0; i < openBracketsCnt; i++) {
            where.append('(');
        }
        openBracketsCnt = 0;

        if (withoutData) {
            where.append("`" + table + "`.`" + row.name + "` " + cOperation);
        } else {
            where.append("`" + table + "`.`" + row.name + "` " + cOperation + " ?");
            preparedWhereData.add(new PreparedData(data, row.type));
        }
        return this;
    }

    /**
     * OR
     * @return this
     */
    public IQL or() {
        this.whereConcat = OR;
        return this;
    }

    /**
     * AND
     * @return this
     */
    public IQL and() {
        this.whereConcat = AND;
        return this;
    }

    /**
     * where statement without value (for isnull, isntnull)
     * @param what row to compare
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
     * @param row row to group
     * @param table table, that contains row
     * @return this
     */
    public IQL groupBy(String row, int table) {
        groups.add(new Group(row, table));
        return this;
    }

    /**
     * Grouping data
     * @param row row from current active table to group
     * @return this
     */
    public IQL groupBy(String row) {
        groups.add(new Group(row));
        return this;
    }

    /**
     * Ordering data
     * @param row row for order
     * @param type ordering type (ASC or DESC)
     * @param table table, that contains row
     * @return this
     */
    public IQL orderBy(String row, String type, int table) {
        orders.add(new Order(row, type, table));
        return this;
    }

    /**
     * Ordering data
     * @param row row for order
     * @param table table, that contains row
     * @return this
     */
    public IQL orderBy(String row, int table) {
        orders.add(new Order(row, table));
        return this;
    }

    /**
     * Ordering data
     * @param row row from current active table
     * @param type ordering type (ASC or DESC)
     * @return
     */
    public IQL orderBy(String row, String type) {
        orders.add(new Order(row, type));
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
     * @param row1 field name for join to
     * @param index2 joining table index
     * @param row2 joining field name
     * @param side join side (LEFT, RIGHT, FULL)
     * @param type join type (INNER, OUTER)
     * @return this
     */
    public IQL join(int index1, String row1, int index2, String row2, String side, String type) {
        excludedTables.add(tables.get(index2 - 1));
        joins.add(new Join(index1, row1, index2, row2, side, type));
        return this;
    }

    /**
     * Joining tables
     * @param index1 table to join to
     * @param row1 field name for join to
     * @param index2 joining table index
     * @param row2 joining field name
     * @param typeOrSide join side (LEFT, RIGHT, FULL) or type (INNER, OUTER)            
     * @return this
     */
    public IQL join(int index1, String row1, int index2, String row2, String typeOrSide) {
        if (typeOrSide.equals(JOIN_OUTER) || typeOrSide.equals(JOIN_INNER)) {
            return join(index1, row1, index2, row2, null, typeOrSide);
        } else {
            return join(index1, row1, index2, row2, typeOrSide, null);
        }
    }

    /**
     * Joining tables
     * @param index1 table to join to
     * @param row1 field name for join to
     * @param index2 joining table index
     * @param row2 joining field name
     * @return this
     */
    public IQL join(int index1, String row1, int index2, String row2) {
        return join(index1, row1, index2, row2, null, null);
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
        if ((opType == UPDATE || opType == DELETE) && where.length() == 0) {
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
        sql.append("INSERT INTO `" + tables.get(0) + "`(`" + modifyingRows[0].name + "`");
        for (int i = 1; i < modifyingRows.length; i++) {
            sql.append(", `" + modifyingRows[i].name + "`");
        }
        sql.append(") VALUES");
        for (Object[] line : insertableData) {
            sql.append(" (?");
            preparedQueryData.add(new PreparedData(line[0], modifyingRows[0].type));
            for (int i = 1; i < line.length; i++) {
                sql.append(", ?");
                preparedQueryData.add(new PreparedData(line[i], modifyingRows[i].type));
            }
            sql.append("),");
        }
        sql.deleteCharAt(sql.length() - 1);
    }

    private void compileUpdate() {
        sql.append("UPDATE `" + tables.get(0) + "` SET");
        int i = 0;
        for (Row cRow : modifyingRows) {
            sql.append(" `" + cRow.name + "` = ?,");
            preparedQueryData.add(new PreparedData(updateData[i++], cRow.type));
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

    private String getRowCreateCmd(Row row) {
        StringBuilder ret = new StringBuilder("`" + row.name + "` ");
        switch (row.type) {
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
            case RT_F:
                ret.append("FLOAT");
        }
        return ret.toString();
    }

    private void compileCreate() {
        sql.append("CREATE TABLE `" + tables.get(0) + "`(`id` INTEGER PRIMARY KEY AUTO_INCREMENT,");
        for (Row cRow : createRows) {
            sql.append(" " + getRowCreateCmd(cRow) + ",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
    }

    private void compileSelect() {
        sql.append("SELECT");
        if (selectedRows.size() > 0) {
            for (SelectedRow row : selectedRows) {
                sql.append(" `" + row.table + "`.`" + row.row + "` AS `" + row.alias + "`,");
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