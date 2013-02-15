package org.neo4j.batchimport.importer;

import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.lang3.builder.ToStringBuilder;

import static org.neo4j.helpers.collection.MapUtil.map;

public class RowData {
    private Object[] data;
    private final int offset;
    private final String delim;
    private final String[] lineDataHeaders;
    private final String[] lineDataValues;
    private final Type dataTypes[];
    private final int numColumns;
    private int numColsToLoad;
    private int count;
    private static final String DEFAULT_DELIMITER = ",";

    /**
     * Creates a RowData object, used for further importing rows from a csv.
     * 
     * @param header the header string for the csv file to be parsed.
     * @param delim, delimiter for the csv file.
     * @param offset, the columns beyond which they are properties
     */
    public RowData(String header, String delim, int offset) {
    	System.out.println("header is " + header);
        this.offset = offset;
        this.delim = delim;
        lineDataHeaders = header.split(delim);
        numColumns = lineDataHeaders.length;
        dataTypes = parseTypes(lineDataHeaders);
        lineDataValues = new String[numColumns];
        createMapData(numColumns, offset);
    }
    
    /**
     * Default delimiter ","is used;
     * @see #RowData(String, String, int)
     */
    public RowData(String header, int offset) {
    	this( header, DEFAULT_DELIMITER, offset);
    }
    
    /**
     * Default delimiter ","is used; Default offset is 0
     * @see #RowData(String, String, int)
     */    
    public RowData(String header) {
    	this( header, DEFAULT_DELIMITER, 0);
    }

    public String[] getFields() {
        if (offset==0) return lineDataHeaders;
        return Arrays.copyOfRange(lineDataHeaders,offset,lineDataHeaders.length);
    }

    private Object[] createMapData(int numColumns, int offset) {
        numColsToLoad = numColumns - offset;
        System.out.println("numColsToLoad is " + numColsToLoad);
        data = new Object[numColsToLoad*2];
        for (int i = 0; i < numColsToLoad; i++) {
            data[i * 2] = lineDataHeaders[i + offset];
            System.out.println("data[" + i*2 + "]=" + data[i * 2]);
        }
        return data;
    }

    private Type[] parseTypes(String[] fields) {
        Type[] types = new Type[numColumns];
        Arrays.fill(types, Type.STRING);
        for (int i = 0; i < numColumns; i++) {
            String field = fields[i];
            int idx = field.indexOf(':');
            if (idx!=-1) {
               fields[i]=field.substring(0,idx);
               types[i]= Type.fromString(field.substring(idx + 1));
            }
        }
        return types;
    }

    private void parse(String line) {
        final StringTokenizer st = new StringTokenizer(line, delim,true);
        for (int i = 0; i < numColumns; i++) {
            String value = st.hasMoreTokens() ? st.nextToken() : delim;
            if (value.equals(delim)) {
                lineDataValues[i] = null;
            } else {
                lineDataValues[i] = value.trim().isEmpty() ? null : value;
                if (i< numColumns -1 && st.hasMoreTokens()) st.nextToken();
            }
        }
    }
    
    public Object[] process(String line) {
        parse(line);
        count = 0;
        for (int i=offset;i<numColumns;i++) {
            data[count++] = lineDataValues[i] == null ? null : dataTypes[i].convert(lineDataValues[i]);
        }
        return data;
    }

    private int split(String line) {
        parse(line);
        count = 0;
        for (int i = offset; i < numColumns; i++) {
        	System.out.println("i is - " + i);
            if (lineDataValues[i] == null) continue;
            data[count++]=lineDataHeaders[i];
            data[count++]=dataTypes[i].convert(lineDataValues[i]);
        }
        return count;
    }

    /**
     * not sure what the header stands for...
     * @param line
     * @param header
     * @return
     */
    public Map<String,Object> updateMap(String line, Object... header) {
    	//System.out.println(line);
        split(line);
        if (header.length > 0) {
            System.arraycopy(lineDataValues, 0, header, 0, header.length);
        }

        if (count == numColsToLoad*2) {
            return map(data);
        }
        Object[] newData=new Object[count];
        System.arraycopy(data,0,newData,0,count);
        return map(newData);
    }

    public Object[] updateArray(String line, Object... header) {
        process(line);
        if (header!=null && header.length > 0) {
            System.arraycopy(lineDataValues, 0, header, 0, header.length);
        }
        return data;
    }

    public Object[] getData() {
        return data;
    }

    public int getCount() {
        return count;
    }

    public int getLineSize() {
        return numColumns;
    }
    
    public String toString() {
    	return ToStringBuilder.reflectionToString(this);
    }
}
