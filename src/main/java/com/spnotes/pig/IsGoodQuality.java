package com.spnotes.pig;

import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;

import java.io.IOException;
import java.util.List;

/**
 * Created by gpzpati on 1/4/15.
 */
public class IsGoodQuality extends FilterFunc {

    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if(tuple == null || tuple.size() ==0){
            return false;
        }

        Object object = tuple.get(0);
        if(object == null){
            return false;
        }
        int i = (Integer)object;

        return i ==0 || i ==1 || i ==4 || i ==5 || i ==9;
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        return super.getArgToFuncMapping();
    }
}
