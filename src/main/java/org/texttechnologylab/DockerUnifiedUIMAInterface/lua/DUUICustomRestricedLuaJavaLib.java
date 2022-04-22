package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

/*******************************************************************************
 * Copyright (c) 2009 Luaj.org. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 ******************************************************************************/
//Taken from https://github.com/luaj/luaj/blob/daf3da94e3cdba0ac6a289148d7e38bd53d3fe64/src/jse/org/luaj/vm2/lib/jse/LuajavaLib.java
//and adapted to fit our needs by Alexander Leonhardt

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.LibFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.CoerceLuaToJava;

import java.lang.reflect.*;
import java.util.Set;

public class DUUICustomRestricedLuaJavaLib extends VarArgFunction {

    static final int INIT           = 0;
    static final int BINDCLASS      = 1;
    static final int NEWINSTANCE	= 2;
    static final int NEW			= 3;
    static final int CREATEPROXY	= 4;
    static final int LOADLIB		= 5;

    static final String[] NAMES = {
            "bindClass",
            "newInstance",
            "new",
            "createProxy",
            "loadLib",
    };

    static final int METHOD_MODIFIERS_VARARGS = 0x80;

    private Set<String> _allowedClasses;

    public DUUICustomRestricedLuaJavaLib(Set<String> allowedClasses) {
        _allowedClasses = allowedClasses;
    }

    public DUUICustomRestricedLuaJavaLib(String new_name, int new_opcode, Set<String> allowedClasses) {
        _allowedClasses = allowedClasses;
        name = new_name;
        opcode = new_opcode;

    }

    public String get_name() {
        return name;
    }

    public Varargs invoke(Varargs args) {
        try {
            switch ( opcode ) {
                case INIT: {
                    // LuaValue modname = args.arg1();
                    LuaValue env = args.arg(2);
                    LuaTable t = new LuaTable();
                    int var5 = 0;

                    for(int var6 = NAMES.length; var5 < var6; ++var5) {
                        LibFunction var7 = (LibFunction)new DUUICustomRestricedLuaJavaLib(NAMES[var5],BINDCLASS + var5,_allowedClasses);
                        t.set(NAMES[var5], var7);
                    }
                    env.set("luajava", t);
                    if (!env.get("package").isnil()) env.get("package").get("loaded").set("luajava", t);
                    return t;
                }
                case BINDCLASS: {
                    final Class clazz = classForName(args.checkjstring(1));
                    Class o = Class.forName("org.luaj.vm2.lib.jse.JavaClass");
                    Method method = o.getDeclaredMethod("forClass",Class.class);
                    method.setAccessible(true);
                    return (Varargs) method.invoke(null,clazz);
                }
                case NEWINSTANCE:
                case NEW: {
                    // get constructor
                    final LuaValue c = args.checkvalue(1);
                    final Class clazz = (opcode==NEWINSTANCE? classForName(c.tojstring()): (Class) c.checkuserdata(Class.class));
                    final Varargs consargs = args.subargs(2);
                    Class o = Class.forName("org.luaj.vm2.lib.jse.JavaClass");
                    Method method = o.getDeclaredMethod("forClass",Class.class);
                    method.setAccessible(true);
                    Object javaclass = method.invoke(null,clazz);
                    Method constructor = javaclass.getClass().getMethod("getConstructor");
                    constructor.setAccessible(true);
                    return ((LuaValue)constructor.invoke(javaclass)).invoke(consargs);
                }

                case CREATEPROXY: {
                    final int niface = args.narg()-1;
                    if ( niface <= 0 )
                        throw new LuaError("no interfaces");
                    final LuaValue lobj = args.checktable(niface+1);

                    // get the interfaces
                    final Class[] ifaces = new Class[niface];
                    for ( int i=0; i<niface; i++ )
                        ifaces[i] = classForName(args.checkjstring(i+1));

                    // create the invocation handler
                    InvocationHandler handler = new ProxyInvocationHandler(lobj);

                    // create the proxy object
                    Object proxy = Proxy.newProxyInstance(getClass().getClassLoader(), ifaces, handler);

                    // return the proxy
                    return LuaValue.userdataOf( proxy );
                }
                case LOADLIB: {
                    // get constructor
                    String classname = args.checkjstring(1);
                    String methodname = args.checkjstring(2);
                    Class clazz = classForName(classname);
                    Method method = clazz.getMethod(methodname, new Class[] {});
                    Object result = method.invoke(clazz, new Object[] {});
                    if ( result instanceof LuaValue ) {
                        return (LuaValue) result;
                    } else {
                        return NIL;
                    }
                }
                default:
                    throw new LuaError("not yet supported: "+this);
            }
        } catch (LuaError e) {
            throw e;
        } catch (InvocationTargetException ite) {
            throw new LuaError(ite.getTargetException());
        } catch (Exception e) {
            throw new LuaError(e);
        }
    }

    // InfluxDB -> CPU, Memory, Logs
    // Postgres -> Document time, run information etc.
    // Grafana -> Show informations

    // load classes using app loader to allow luaj to be used as an extension
    protected Class classForName(String name) throws ClassNotFoundException {
        if(_allowedClasses.contains(name)) {
            return Class.forName(name, true, ClassLoader.getSystemClassLoader());
        }
        throw new RuntimeException("Trying to load restricted class \""+name+"\"");
    }

    private static final class ProxyInvocationHandler implements InvocationHandler {
        private final LuaValue lobj;

        private ProxyInvocationHandler(LuaValue lobj) {
            this.lobj = lobj;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

            String name = method.getName();
            LuaValue func = lobj.get(name);
            if ( func.isnil() )
                return null;
            boolean isvarargs = ((method.getModifiers() & METHOD_MODIFIERS_VARARGS) != 0);
            int n = args!=null? args.length: 0;
            LuaValue[] v;
            if ( isvarargs ) {
                Object o = args[--n];
                int m = Array.getLength( o );
                v = new LuaValue[n+m];
                for ( int i=0; i<n; i++ )
                    v[i] = CoerceJavaToLua.coerce(args[i]);
                for ( int i=0; i<m; i++ )
                    v[i+n] = CoerceJavaToLua.coerce(Array.get(o,i));
            } else {
                v = new LuaValue[n];
                for ( int i=0; i<n; i++ )
                    v[i] = CoerceJavaToLua.coerce(args[i]);
            }
            LuaValue result = func.invoke(v).arg1();
            return CoerceLuaToJava.coerce(result, method.getReturnType());
        }
    }

}