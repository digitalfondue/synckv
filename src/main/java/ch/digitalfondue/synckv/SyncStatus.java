package ch.digitalfondue.synckv;

class SyncStatus {

    final boolean status;
    final long time;

    SyncStatus(boolean status, long time) {
        this.status = status;
        this.time = time;
    }
}
