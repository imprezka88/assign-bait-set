package org.mskcc.lims;

import com.velox.api.datarecord.*;
import com.velox.api.user.User;
import com.velox.sapioutils.client.standalone.VeloxConnection;
import com.velox.sapioutils.client.standalone.VeloxStandalone;
import com.velox.sapioutils.client.standalone.VeloxStandaloneException;
import com.velox.sapioutils.client.standalone.VeloxTask;
import org.mskcc.util.VeloxConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AssignBaitSet {
    private static final String limsConnectionFilePath = "/Connection-dev.txt";
    private static final Logger LOGGER = LoggerFactory.getLogger(AssignBaitSet.class);

    private static final Map<String, String> oldToNewBaitSet = new HashMap<>();

    static  {
        oldToNewBaitSet.put("51MB_Human", "Agilent_v4_51MB_Human");
        oldToNewBaitSet.put("51MB_Mouse", "Agilent_v4_51MB_Mouse");
    }

    private User user;
    private DataRecordManager dataRecordManager;

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
        List<DataRecord> kapas1 = dataRecordManager.queryDataRecords(VeloxConstants.KAPA_AGILENT_CAPTURE_PROTOCOL_1, null, user);
        List<DataRecord> kapas2 = dataRecordManager.queryDataRecords(VeloxConstants.KAPA_AGILENT_CAPTURE_PROTOCOL_2, null, user);

        assignToKapa(kapas1);
        assignToKapa(kapas2);
    }

    private void assignToKapa(List<DataRecord> kapas) throws RemoteException, NotFound, IoError, InvalidValue {
        for (DataRecord kapa : kapas) {
            try {
                DataRecord newestRequest = getNewestRequest(kapa);

                if (!isRequestCompleted(newestRequest)) {
                    String baitSet = kapa.getStringVal(VeloxConstants.AGILENT_CAPTURE_BAIT_SET, user);
                    LOGGER.info(String.format("Bait set found: %s", baitSet));

                    if (oldToNewBaitSet.containsKey(baitSet)) {
                        LOGGER.info(String.format("Changing %s from: %s to: %s", VeloxConstants.AGILENT_CAPTURE_BAIT_SET, baitSet, oldToNewBaitSet.get(baitSet)));
//                    kapa.setDataField(VeloxConstants.AGILENT_CAPTURE_BAIT_SET, oldToNewBaitSet.get(baitSet), user);
                    }
                } else {
                    LOGGER.info(String.format("Request: %s is completed.", newestRequest.getStringVal(VeloxConstants.REQUEST_ID, user)));
                }
            } catch (NoParentRequestException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private DataRecord getNewestRequest(DataRecord kapa) throws NotFound, RemoteException {
        List<DataRecord> requests = kapa.getAncestorsOfType(VeloxConstants.REQUEST, user);

        if(requests.size() == 0)
            throw new NoParentRequestException(String.format("No request parents for kapa protocol: %d", kapa.getRecordId()));

        DataRecord newestRequest = requests.get(0);

        if(requests.size() > 0) {
            LOGGER.info(String.format("Found %d parent request (s).", requests.size()));

            long latestCreationDate = 0;

            for (DataRecord request : requests) {
                long dateCreated = request.getDateVal("DateCreated", user);
                if(dateCreated > latestCreationDate) {
                    latestCreationDate = dateCreated;
                    newestRequest = request;
                }
            }
        }

        LOGGER.info(String.format(String.format("Newest parent requests: %s", newestRequest.getStringVal(VeloxConstants.REQUEST_ID, user))));
        return newestRequest;
    }

    private boolean isRequestCompleted(DataRecord request) throws NotFound, RemoteException {
        Object completedDate = request.getDataField("CompletedDate", user);

        return completedDate != null;
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
