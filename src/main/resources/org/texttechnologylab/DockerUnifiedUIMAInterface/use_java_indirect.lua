serial = luajava.bindClass("org.apache.uima.cas.impl.XmiCasSerializer")
newval = luajava.newInstance("java.lang.String","hello World!")

function serialize(inputCas,outputStream,params)
  outputStream:write('try')
end

function deserialize(inputCas,inputStream)
  inputCas:reset()
  deserial:deserialize(inputStream,inputCas:getCas(),true)
end