package org.mskcc.lims;

import com.velox.api.datarecord.*;
import com.velox.api.user.User;
import com.velox.sapioutils.client.standalone.VeloxConnection;
import com.velox.sapioutils.client.standalone.VeloxStandalone;
import com.velox.sapioutils.client.standalone.VeloxStandaloneException;
import com.velox.sapioutils.client.standalone.VeloxTask;
import org.apache.log4j.Logger;
import org.mskcc.util.VeloxConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AssignBaitSet {
    //    private static final String limsConnectionFilePath = "/Connection-dev.txt";
    private static final String limsConnectionFilePath = "/Connection.txt";
    private static final Logger LOGGER = Logger.getLogger(AssignBaitSet.class);

    private static final Map<String, String> oldToNewBaitSet = new HashMap<>();

    static {
        oldToNewBaitSet.put("51MB_Human", "Agilent_v4_51MB_Human");
        oldToNewBaitSet.put("51MB_Mouse", "Agilent_v4_51MB_Mouse");
    }

    private User user;
    private DataRecordManager dataRecordManager;

    private Path kapa1Backup = Paths.get("kapa1_bait_set_backup.txt");
    private Path kapa2Backup = Paths.get("kapa2_bait_set_backup.txt");

    public static void main(String[] args) {
        AssignBaitSet assignBaitSet = new AssignBaitSet();
        assignBaitSet.assignBaitSets();
    }

    private void assignBaitSets() {
        VeloxConnection connection = tryToConnectToLims();

        try {
            VeloxStandalone.run(connection, new VeloxTask<Object>() {
                @Override
                public Object performTask() throws VeloxStandaloneException {
                    try {
                        assign();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                    return new Object();
                }
            });

        } catch (VeloxStandaloneException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private void assign() throws Exception {
        Files.deleteIfExists(kapa1Backup);
        Files.createFile(kapa1Backup);
        Files.deleteIfExists(kapa2Backup);
        Files.createFile(kapa2Backup);

        List<DataRecord> kapas1 = dataRecordManager.queryDataRecords(VeloxConstants.KAPA_AGILENT_CAPTURE_PROTOCOL_1, null, user);
        List<DataRecord> kapas2 = dataRecordManager.queryDataRecords(VeloxConstants.KAPA_AGILENT_CAPTURE_PROTOCOL_2, null, user);

        LOGGER.info(String.format("Assigning bait set for: %s", VeloxConstants.KAPA_AGILENT_CAPTURE_PROTOCOL_1));
        assignToKapa(kapas1, kapa1Backup);

        LOGGER.info(String.format("Assigning bait set for: %s", VeloxConstants.KAPA_AGILENT_CAPTURE_PROTOCOL_2));
        assignToKapa(kapas2, kapa2Backup);
    }

    private void assignToKapa(List<DataRecord> kapas, Path kapaBackup) throws RemoteException, NotFound, IoError, InvalidValue {
        for (DataRecord kapa : kapas) {
            try {
                String baitSet = kapa.getStringVal(VeloxConstants.AGILENT_CAPTURE_BAIT_SET, user);
                LOGGER.info(String.format("Bait set found: %s", baitSet));

                if (oldToNewBaitSet.containsKey(baitSet)) {
                    LOGGER.info(String.format("Saving old bait set for record: %s", kapa.getRecordId()));

                    try {
                        Files.write(kapaBackup, String.format("%s %s\n", kapa.getRecordId(), baitSet).getBytes(), StandardOpenOption.APPEND);
                        LOGGER.info(String.format("Changing %s from: %s to: %s for record: %s", VeloxConstants.AGILENT_CAPTURE_BAIT_SET, baitSet, oldToNewBaitSet.get(baitSet), kapa.getRecordId()));
                        kapa.setDataField(VeloxConstants.AGILENT_CAPTURE_BAIT_SET, oldToNewBaitSet.get(baitSet), user);
                    } catch (IOException e) {
                        LOGGER.warn(String.format("Unable to save old bait set for record: %s. Omitting overwriting", kapa.getRecordId()), e);
                    }
                }

            } catch (NoParentRequestException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private VeloxConnection tryToConnectToLims() {
        VeloxConnection connection;

        try {
            connection = initVeloxConnection();
        } catch (Exception e) {
            LOGGER.warn("Cannot connect to LIMS");
            throw new RuntimeException(e);
        }
        return connection;
    }

    private VeloxConnection initVeloxConnection() throws Exception {
        String fullPath = AssignBaitSet.class.getResource(limsConnectionFilePath).getPath();
        VeloxConnection connection = new VeloxConnection(fullPath);
        addShutdownHook(connection);
        connection.open();

        if (connection.isConnected()) {
            user = connection.getUser();
            dataRecordManager = connection.getDataRecordManager();
            return connection;
        }

        throw new RuntimeException("Error while trying to connect to LIMS");
    }

    private void addShutdownHook(VeloxConnection connection) {
        MySafeShutdown sh = new MySafeShutdown(connection);
        Runtime.getRuntime().addShutdownHook(sh);
    }

    private void closeConnection(VeloxConnection connection) {
        if (connection.isConnected()) {
            try {
                connection.close();
            } catch (Throwable e) {
                LOGGER.error("Cannot close LIMS connection", e);
            }
        }
    }

    public class MySafeShutdown extends Thread {
        private VeloxConnection connection;

        public MySafeShutdown(VeloxConnection connection) {
            this.connection = connection;
        }

        @Override
        public void run() {
            closeConnection(connection);
        }
    }

    private class NoParentRequestException extends RuntimeException {
        public NoParentRequestException(String message) {
            super(message);
        }
    }
}
