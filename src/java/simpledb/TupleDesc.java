package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private final List<TDItem> fields;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        Iterator<TDItem> itr = this.fields.iterator();
        return itr;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // if (typeAr == null || typeAr.length == 0) {
        //     throw new IllegalArgumentException("typeAr must contain at least one entry");
        // }
        // if (fieldAr != null && fieldAr.length != typeAr.length) {
        //     throw new IllegalArgumentException("fieldAr must have the same length as typeAr");
        // }

        List<TDItem> items = new ArrayList<>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            Type fieldType = typeAr[i];
            String fieldName = fieldAr[i];
            items.add(new TDItem(fieldType, fieldName));
        }
        this.fields = items;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        List<TDItem> items = new ArrayList<>(typeAr.length);
        for (int i = 0; i < typeAr.length; i++) {
            Type fieldType = typeAr[i];
            items.add(new TDItem(fieldType, null));
        }
        this.fields = items;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.fields.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= this.fields.size()){
            throw new NoSuchElementException("Error: Array index of out bounds");
        }
        return this.fields.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= this.fields.size()){
            throw new NoSuchElementException("Error: Array index of out bounds");
        }
        return this.fields.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * No match if name is null.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i<this.fields.size(); i++){
            String field_name = this.fields.get(i).fieldName;
            if (field_name != null && field_name.equals(name)){
                return i;
            }
        }
        throw new NoSuchElementException("Error: desired field name was not found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     * @see Type#getSizeInBytes
     */
    public int getSizeInBytes() {
        int total_size = 0;
        for (int i = 0; i<this.fields.size(); i++){
            total_size += this.fields.get(i).fieldType.getSizeInBytes();
        }
        return total_size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int newsize = td1.numFields()+td2.numFields();
        Type[] merged_types = new Type[newsize];
        String[] merged_names = new String[newsize];
        for(int i = 0; i<td1.fields.size(); i++){
            merged_types[i] = td1.fields.get(i).fieldType;
            merged_names[i] = td1.fields.get(i).fieldName;
        }
        for(int i = 0; i<td2.fields.size(); i++){
            merged_types[i+td1.numFields()] = td2.fields.get(i).fieldType;
            merged_names[i+td1.numFields()] = td2.fields.get(i).fieldName;
        }
        TupleDesc retTupleDesc = new TupleDesc(merged_types, merged_names);
        return retTupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i. It does not matter if the field names are equal.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if(!(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc o_tupledesc = (TupleDesc) o;
        if(this.numFields() != o_tupledesc.numFields()){
            return false;
        }
        for (int i = 0; i<this.fields.size(); i++){
            if(!this.fields.get(i).fieldType.equals(o_tupledesc.fields.get(i).fieldType)){
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // Enables us to hash tupleDesc as keys
        int result = 1;
        for (TDItem item : fields) {
            // Only the type contributes because equals only compares types.
            result = 31 * result + (item.fieldType == null ? 0 : item.fieldType.hashCode());
        }
        return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldName[0](fieldType[0]), ..., fieldName[M](fieldType[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        List<String> fields_strs = new ArrayList<String>();
        for (TDItem i : this.fields){
            String curr = "";
            if(i.fieldName == null){
                curr = "(" + i.fieldType + ")";
            } else {
                curr = i.fieldName + "(" + i.fieldType + ")";
            }
            fields_strs.add(curr);
        }
        return String.join(",",fields_strs);
    }
}
