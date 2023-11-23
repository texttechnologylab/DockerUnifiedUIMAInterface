package org.texttechnologylab.DockerUnifiedUIMAInterface.lua;

import java.util.HashSet;
import java.util.Set;

/**
 * Lua sandbox to ensure proper class usage or its restriction by Lua
 *
 * @author Alexander Leonhardt
 */
public class DUUILuaSandbox {
    public boolean _enable_io;
    public boolean  _enable_coroutines;
    public boolean _enable_os;
    public int _max_instruction_count;

    public Set<String> _allowedJavaClasses;
    public boolean _allowAllJavaClasses;

    public DUUILuaSandbox() {
        _enable_io = false;
        _enable_coroutines = false;
        _enable_os = false;
        _max_instruction_count = -1;
        _allowAllJavaClasses = false;
        _allowedJavaClasses = new HashSet<>();
    }

    public boolean getEnabledIo() {
        return _enable_io;
    }

    public boolean getEnabledCoroutines() {
        return _enable_coroutines;
    }

    public boolean getEnabledOs() {
        return _enable_os;
    }

    public boolean getEnabledAllJavaClasses() {
        return _allowAllJavaClasses;
    }

    public int getMaxInstructionCount() {
        return _max_instruction_count;
    }


    public Set<String> getAllowedJavaClasses() {
        return _allowedJavaClasses;
    }

    public DUUILuaSandbox withIo(boolean enableIo) {
        _enable_io = enableIo;
        return this;
    }

    public DUUILuaSandbox withOs(boolean enableOs) {
        _enable_os = enableOs;
        return this;
    }

    public DUUILuaSandbox withCoroutines(boolean enableCoroutines) {
        _enable_coroutines = enableCoroutines;
        return this;
    }

    public DUUILuaSandbox withAllJavaClasses(boolean enableAllJavaClasses) {
        _allowAllJavaClasses = enableAllJavaClasses;
        return this;
    }

    public DUUILuaSandbox withLimitInstructionCount(int maxInstructionCount) {
        _max_instruction_count = maxInstructionCount;
        return this;
    }


    public DUUILuaSandbox withAllowedJavaClass(String className) {
        _allowedJavaClasses.add(className);
        return this;
    }
}
