package org.dcache.vfs4j.graalvm;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.Feature.DuringSetupAccess;
import static java.lang.foreign.ValueLayout.*;
import org.graalvm.nativeimage.hosted.RuntimeForeignAccess;

/**
 * Descriptors that characterize the functions to which downcalls may be
 * performed at run time.
 */
public class ForeignRegistrationFeature implements Feature {

    public void duringSetup(DuringSetupAccess access) {

        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(ADDRESS, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.ofVoid(ADDRESS, JAVA_LONG));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_LONG));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT, JAVA_INT));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS));
        RuntimeForeignAccess.registerForDowncall(FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG));
    }

}
