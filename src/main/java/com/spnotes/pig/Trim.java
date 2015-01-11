package com.spnotes.pig;

import org.apache.avro.generic.GenericData;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by gpzpati on 1/4/15.
 */
public class Trim extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        if(input == null || input.size() ==0){
            return null;
        }

        Object object = input.get(0);
        if(object == null) {
            return null;
        }
        return ((String)object).trim();

    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
                new Schema.FieldSchema(null, DataType.CHARARRAY))));
        return funcList;
    }
}
