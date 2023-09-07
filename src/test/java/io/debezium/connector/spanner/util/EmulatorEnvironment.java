/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class EmulatorEnvironment {
    private boolean isStarted = false;

    public EmulatorEnvironment() {
        isStarted = false;
    }

    public boolean checkCommandSuccess(Process p) {
        BufferedReader stdOutput = new BufferedReader(new InputStreamReader(p.getInputStream()));

        String line;
        ArrayList<String> output = new ArrayList<String>();

        try {
            while ((line = stdOutput.readLine()) != null) {
                System.out.print("output: " + line);
            }
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return true;
    }

    public static void printResults(Process process) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader errors = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String line = "";
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        while ((line = errors.readLine()) != null) {
            System.out.println(line);
        }
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void startLocalEmulator() {
        try {
            System.out.println("about to download docker tar");
            Process p = Runtime.getRuntime()
                    .exec("curl --output ../emulator.tar https://storage.googleapis.com/cloud-spanner-emulator/change-streams-preview/emulator-for-connector.tar");
            printResults(p);
            System.out.println("about to docker load");
            p = Runtime.getRuntime()
                    .exec("docker load -i ../emulator.tar");
            printResults(p);
            System.out.println("about to docker run");
            Runtime.getRuntime()
                    .exec("docker run --name emulator --rm -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator-test/change-streams/emulator-for-connector:latest");
            isStarted = true;
            System.out.println("after docker run");
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutDownLocalEmulator() {
        try {
            System.out.println("about to docker stop");
            Process p = Runtime.getRuntime().exec("docker stop emulator");
            printResults(p);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }
}
