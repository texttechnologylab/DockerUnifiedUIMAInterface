from cassis import *
import json
import time

with open('dkpro-core-types.xml', 'rb') as f:
    typesystem = load_typesystem(f)
with open('large_xmi.xml','rb') as f2:
    cas = load_cas_from_xmi(f2,typesystem=typesystem)

    print("Start serialize")
    start = time.time_ns()
    cas.to_xmi()
    end = time.time_ns()

    print((end-start)/1000000)
    
    print("Start serialize JSON")
    start = time.time_ns()
    cas.to_json()
    end = time.time_ns()

    print((end-start)/1000000)

    start = time.time_ns()
    tp = typesystem.get_type("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token")
    begin = []
    end = []
    for x in cas.select("de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Token"):
        begin.append(x["begin"])
        end.append(x["end"])

    res = json.dumps([cas.sofa_string,begin,end])
    end = time.time_ns()
    print(len(res))

    print((end-start)/1000000)

