/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.api;

import com.oltpbenchmark.*;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.featurebench.FeaturebenchAdditionalResults;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static com.oltpbenchmark.types.State.MEASURE;

public abstract class Worker<T extends BenchmarkModule> implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private static final Logger ABORT_LOG = LoggerFactory.getLogger("com.oltpbenchmark.api.ABORT_LOG");
    protected final WorkloadConfiguration configuration;
    protected final TransactionTypes transactionTypes;
    protected final Map<TransactionType, Procedure> procedures = new HashMap<>();
    protected final Map<String, Procedure> name_procedures = new HashMap<>();
    protected final Map<Class<? extends Procedure>, Procedure> class_procedures = new HashMap<>();
    private final Statement currStatement;
    // Interval requests used by the monitor
    private final AtomicInteger intervalRequests = new AtomicInteger(0);
    private final int id;
    private final T benchmark;
    private final Histogram<TransactionType> txnUnknown = new Histogram<>();
    private final Histogram<TransactionType> txnSuccess = new Histogram<>();
    private final Histogram<TransactionType> txnAbort = new Histogram<>();
    private final Histogram<TransactionType> txnRetry = new Histogram<>();
    private final Histogram<TransactionType> txnErrors = new Histogram<>();
    private final Histogram<TransactionType> txnZeroRows = new Histogram<>();
    private final Histogram<TransactionType> txtRetryDifferent = new Histogram<>();
    protected Connection conn = null;
    private WorkloadState workloadState;
    boolean isFeaturebenchWorkload = false;
    private LatencyRecord latencies;
    private boolean seenDone = false;
    public static final FeaturebenchAdditionalResults featurebenchAdditionalResults = new FeaturebenchAdditionalResults();

    public Worker(T benchmark, int id) {
        this.id = id;
        this.benchmark = benchmark;
        this.configuration = this.benchmark.getWorkloadConfiguration();
        this.workloadState = this.configuration.getWorkloadState();
        this.currStatement = null;
        this.transactionTypes = this.configuration.getTransTypes();
        boolean autoCommitVal = false;
        if (this.benchmark.getBenchmarkName().equalsIgnoreCase("featurebench") &&
            this.benchmark.getWorkloadConfiguration().getXmlConfig().containsKey("microbenchmark/properties/setAutoCommit")) {
            autoCommitVal = this.benchmark.getWorkloadConfiguration().getXmlConfig().getBoolean("microbenchmark/properties/setAutoCommit");
            this.isFeaturebenchWorkload = true;
        }
        if (!this.configuration.getNewConnectionPerTxn()) {
            try {
                this.conn = this.benchmark.makeConnection();
                this.conn.setAutoCommit(autoCommitVal);
                this.conn.setTransactionIsolation(this.configuration.getIsolationMode());
            } catch (SQLException ex) {
                throw new RuntimeException("Failed to connect to database", ex);
            }
        }

        // Generate all the Procedures that we're going to need
        this.procedures.putAll(this.benchmark.getProcedures());
        for (Entry<TransactionType, Procedure> e : this.procedures.entrySet()) {
            Procedure proc = e.getValue();
            this.name_procedures.put(e.getKey().getName(), proc);
            this.class_procedures.put(proc.getClass(), proc);
        }
    }

    /**
     * Get the BenchmarkModule managing this Worker
     */
    public final T getBenchmark() {
        return (this.benchmark);
    }

    /**
     * Get the unique thread id for this worker
     */
    public final int getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return String.format("%s<%03d>", this.getClass().getSimpleName(), this.getId());
    }

    public final WorkloadConfiguration getWorkloadConfiguration() {
        return (this.benchmark.getWorkloadConfiguration());
    }

    public final Random rng() {
        return (this.benchmark.rng());
    }

    public final int getRequests() {
        return latencies.size();
    }

    public final int getAndResetIntervalRequests() {
        return intervalRequests.getAndSet(0);
    }

    public final Iterable<LatencyRecord.Sample> getLatencyRecords() {
        return latencies;
    }

    public final Procedure getProcedure(TransactionType type) {
        return (this.procedures.get(type));
    }

    @Deprecated
    public final Procedure getProcedure(String name) {
        return (this.name_procedures.get(name));
    }

    @SuppressWarnings("unchecked")
    public final <P extends Procedure> P getProcedure(Class<P> procClass) {
        return (P) (this.class_procedures.get(procClass));
    }

    public final Histogram<TransactionType> getTransactionSuccessHistogram() {
        return (this.txnSuccess);
    }

    public final Histogram<TransactionType> getTransactionUnknownHistogram() {
        return (this.txnUnknown);
    }

    public final Histogram<TransactionType> getTransactionRetryHistogram() {
        return (this.txnRetry);
    }

    public final Histogram<TransactionType> getTransactionAbortHistogram() {
        return (this.txnAbort);
    }

    public final Histogram<TransactionType> getTransactionErrorHistogram() {
        return (this.txnErrors);
    }

    public final Histogram<TransactionType> getTransactionRetryDifferentHistogram() {
        return (this.txtRetryDifferent);
    }

    public final Histogram<TransactionType> getTransactionZeroRowsHistogram() { return (this.txnZeroRows); }
    /**
     * Stop executing the current statement.
     */
    synchronized public void cancelStatement() {
        try {
            if (this.currStatement != null) {
                this.currStatement.cancel();
            }
        } catch (SQLException e) {
            LOG.error("Failed to cancel statement: {}", e.getMessage());
        }
    }

    @Override
    public final void run() {
        Thread t = Thread.currentThread();
        t.setName(this.toString());

        // In case of reuse reset the measurements
        latencies = new LatencyRecord(workloadState.getTestStartNs());

        // Invoke initialize callback
        try {
            this.initialize();
        } catch (Throwable ex) {
            throw new RuntimeException("Unexpected error when initializing " + this, ex);
        }

        // wait for start
        workloadState.blockForStart();

        while (true) {

            // PART 1: Init and check if done

            State preState = workloadState.getGlobalState();

            // Do nothing
            if (preState == State.DONE) {
                if (!seenDone) {
                    // This is the first time we have observed that the
                    // test is done notify the global test state, then
                    // continue applying load
                    seenDone = true;
                    workloadState.signalDone();
                    break;
                }
            }

            // PART 2: Wait for work

            // Sleep if there's nothing to do.
            workloadState.stayAwake();

            Phase prePhase = workloadState.getCurrentPhase();
            if (prePhase == null) {
                continue;
            }

            // Grab some work and update the state, in case it changed while we
            // waited.

            SubmittedProcedure pieceOfWork = workloadState.fetchWork();

            prePhase = workloadState.getCurrentPhase();
            if (prePhase == null) {
                continue;
            }

            preState = workloadState.getGlobalState();

            switch (preState) {
                case DONE, EXIT, LATENCY_COMPLETE -> {
                    // Once a latency run is complete, we wait until the next
                    // phase or until DONE.
                    LOG.warn("preState is {}? will continue...", preState);
                    continue;
                }
                default -> {
                }
                // Do nothing
            }

            // PART 3: Execute work

            TransactionType transactionType = getTransactionType(pieceOfWork, prePhase, preState, workloadState);

            if (!transactionType.equals(TransactionType.INVALID)) {

                // TODO: Measuring latency when not rate limited is ... a little
                // weird because if you add more simultaneous clients, you will
                // increase latency (queue delay) but we do this anyway since it is
                // useful sometimes

                // Wait before transaction if specified
                long preExecutionWaitInMillis = getPreExecutionWaitInMillis(transactionType);

                if (preExecutionWaitInMillis > 0) {
                    try {
                        LOG.debug("{} will sleep for {} ms before executing", transactionType.getName(), preExecutionWaitInMillis);

                        Thread.sleep(preExecutionWaitInMillis);
                    } catch (InterruptedException e) {
                        LOG.error("Pre-execution sleep interrupted", e);
                    }
                }

                long start = System.nanoTime();

                doWork(configuration.getDatabaseType(), transactionType);

                long end = System.nanoTime();

                // PART 4: Record results

                State postState = workloadState.getGlobalState();

                switch (postState) {
                    case MEASURE:
                        // Non-serial measurement. Only measure if the state both
                        // before and after was MEASURE, and the phase hasn't
                        // changed, otherwise we're recording results for a query
                        // that either started during the warmup phase or ended
                        // after the timer went off.
                        Phase postPhase = workloadState.getCurrentPhase();

                        if (postPhase == null) {
                            // Need a null check on postPhase since current phase being null is used in WorkloadState
                            // and ThreadBench as the indication that the benchmark is over. However, there's a race
                            // condition with postState not being changed from MEASURE to DONE yet, so we entered the
                            // switch. In this scenario, just break from the switch.
                            break;
                        }
                        if (preState == MEASURE && postPhase.getId() == prePhase.getId()) {
                            // Skip latency recording for optimal thread finding workloads to save memory
                            if (!configuration.getIsOptimalThreadsWorkload()) {
                                latencies.addLatency(transactionType.getId(), start, end, this.id, prePhase.getId());
                            }
                            intervalRequests.incrementAndGet();
                        }
                        if (prePhase.isLatencyRun()) {
                            workloadState.startColdQuery();
                        }
                        break;
                    case COLD_QUERY:
                        // No recording for cold runs, but next time we will since
                        // it'll be a hot run.
                        if (preState == State.COLD_QUERY) {
                            workloadState.startHotQuery();
                        }
                        break;
                    default:
                        // Do nothing
                }


                // wait after transaction if specified
                long postExecutionWaitInMillis = getPostExecutionWaitInMillis(transactionType);

                if (postExecutionWaitInMillis > 0) {
                    try {
                        LOG.debug("{} will sleep for {} ms after executing", transactionType.getName(), postExecutionWaitInMillis);

                        Thread.sleep(postExecutionWaitInMillis);
                    } catch (InterruptedException e) {
                        LOG.error("Post-execution sleep interrupted", e);
                    }
                }
            }

            workloadState.finishedWork();
        }


        LOG.debug("worker calling teardown");

        tearDown();
    }


    private TransactionType getTransactionType(SubmittedProcedure pieceOfWork, Phase phase, State state, WorkloadState workloadState) {
        TransactionType type = TransactionType.INVALID;

        try {
            type = transactionTypes.getType(pieceOfWork.getType());
        } catch (IndexOutOfBoundsException e) {
            if (phase.isThroughputRun()) {
                LOG.error("Thread tried executing disabled phase!");
                throw e;
            }
            if (phase.getId() == workloadState.getCurrentPhase().getId()) {
                switch (state) {
                    case WARMUP -> {
                        // Don't quit yet: we haven't even begun!
                        LOG.info("[Serial] Resetting serial for phase.");
                        phase.resetSerial();
                    }
                    case COLD_QUERY, MEASURE -> {
                        // The serial phase is over. Finish the run early.
                        LOG.info("[Serial] Updating workload state to {}.", State.LATENCY_COMPLETE);
                        workloadState.signalLatencyComplete();
                    }
                    default -> throw e;
                }
            }
        }

        return type;
    }

    /**
     * Called in a loop in the thread to exercise the system under test. Each
     * implementing worker should return the TransactionType handle that was
     * executed.
     *
     * Uses time-based retry with exponential backoff for connection errors.
     * This allows the workload to survive node failures, zone outages, and
     * other transient connection issues.
     *
     * @param databaseType    The database type
     * @param transactionType The transaction type to execute
     */
    protected final void doWork(DatabaseType databaseType, TransactionType transactionType) {
        int retryCount = 0;
        int maxRetryCount = configuration.getMaxRetries();
        int retryDurationSeconds = configuration.getRetryDurationSeconds();
        int initialBackoffMs = configuration.getRetryInitialBackoffMs();
        int maxBackoffMs = configuration.getRetryMaxBackoffMs();

        // Track retry start time for time-based retry limit
        long retryStartTime = 0;
        boolean inRetryMode = false;

        boolean autoCommitVal = false;
        if (this.benchmark.getBenchmarkName().equalsIgnoreCase("featurebench") &&
            this.benchmark.getWorkloadConfiguration().getXmlConfig().containsKey("microbenchmark/properties/setAutoCommit")) {
            autoCommitVal = this.benchmark.getWorkloadConfiguration().getXmlConfig().getBoolean("microbenchmark/properties/setAutoCommit");
        }

        while (this.workloadState.getGlobalState() != State.DONE) {
            TransactionStatus status = TransactionStatus.UNKNOWN;

            // Check time-based retry limit
            if (inRetryMode && retryDurationSeconds > 0) {
                long elapsedSeconds = (System.currentTimeMillis() - retryStartTime) / 1000;
                if (elapsedSeconds > retryDurationSeconds) {
                    LOG.error("{} retry duration exceeded ({} seconds) for {}. Giving up.",
                        this, retryDurationSeconds, transactionType);
                    this.txnErrors.put(transactionType);
                    return;
                }
            }

            // Establish connection if needed
            try {
                if (this.conn == null || this.conn.isClosed()) {
                    this.conn = this.benchmark.makeConnection();
                    this.conn.setAutoCommit(autoCommitVal);
                    this.conn.setTransactionIsolation(this.configuration.getIsolationMode());

                    if (inRetryMode) {
                        LOG.info("{} successfully reconnected after {} retries", this, retryCount);
                    }
                }
            } catch (SQLException connEx) {
                // Connection failed - apply backoff and retry
                if (!inRetryMode) {
                    inRetryMode = true;
                    retryStartTime = System.currentTimeMillis();
                }

                long backoffMs = calculateBackoff(retryCount, initialBackoffMs, maxBackoffMs);
                LOG.warn("{} connection failed (attempt {}), retrying in {} ms: {}",
                    this, retryCount + 1, backoffMs, connEx.getMessage());

                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }

                retryCount++;
                continue;
            }

            // Execute transaction
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} {} attempting...", this, transactionType);
                }

                status = this.executeWork(conn, transactionType);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("{} {} completed with status [{}]...", this, transactionType, status.name());
                }

                if (!autoCommitVal) {
                    conn.commit();
                }

                // Success - reset retry state and exit loop
                if (inRetryMode) {
                    LOG.info("{} transaction {} succeeded after {} retries", this, transactionType, retryCount);
                }
                this.txnSuccess.put(transactionType);
                return;

            } catch (UserAbortException ex) {
                safeRollback();
                ABORT_LOG.debug("{} Aborted", transactionType, ex);
                this.txnAbort.put(transactionType);
                return;

            } catch (SQLException ex) {
                safeRollback();

                if (isRetryable(ex)) {
                    // Start retry timer on first retryable error
                    if (!inRetryMode) {
                        inRetryMode = true;
                        retryStartTime = System.currentTimeMillis();
                    }

                    // Force reconnection for connection errors
                    if (isConnectionError(ex)) {
                        safeCloseConnection();
                        LOG.warn("{} connection error during {} (attempt {}), will reconnect: [{}] {}",
                            this, transactionType, retryCount + 1, ex.getSQLState(), ex.getMessage());
                    } else {
                        LOG.debug("{} retryable error during {} (attempt {}): [{}] {}",
                            this, transactionType, retryCount + 1, ex.getSQLState(), ex.getMessage());
                    }

                    // Apply exponential backoff
                    long backoffMs = calculateBackoff(retryCount, initialBackoffMs, maxBackoffMs);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    retryCount++;
                    this.txnRetry.put(transactionType);

                    // Check count-based limit as fallback (for non-connection errors)
                    if (!isConnectionError(ex) && retryCount >= maxRetryCount) {
                        LOG.error("{} max retry count ({}) exceeded for {}. Last error: [{}] {}",
                            this, maxRetryCount, transactionType, ex.getSQLState(), ex.getMessage());
                        this.txnErrors.put(transactionType);
                        return;
                    }
                } else {
                    // Non-retryable error
                    LOG.warn("{} non-retryable error during {}: [{}] {}",
                        this, transactionType, ex.getSQLState(), ex.getMessage());
                    this.txnErrors.put(transactionType);
                    return;
                }

            } finally {
                // Close connection per transaction if configured
                if (this.configuration.getNewConnectionPerTxn()) {
                    safeCloseConnection();
                }
            }
        }
    }

    /**
     * Safely rollback the current transaction, ignoring errors.
     */
    private void safeRollback() {
        try {
            if (this.conn != null && !this.conn.isClosed() && !this.conn.getAutoCommit()) {
                this.conn.rollback();
            }
        } catch (SQLException e) {
            LOG.debug("Error during rollback: {}", e.getMessage());
        }
    }

    /**
     * Safely close the connection, ignoring errors.
     */
    private void safeCloseConnection() {
        try {
            if (this.conn != null && !this.conn.isClosed()) {
                this.conn.close();
            }
        } catch (SQLException e) {
            LOG.debug("Error closing connection: {}", e.getMessage());
        }
        this.conn = null;
    }

    /**
     * Check if an SQLException is retryable (connection error or transient error).
     *
     * @param ex The SQLException to check
     * @return true if the error is retryable
     */
    private boolean isRetryable(SQLException ex) {
        String sqlState = ex.getSQLState();
        int errorCode = ex.getErrorCode();

        LOG.debug("sql state [{}] and error code [{}]", sqlState, errorCode);

        if (sqlState == null) {
            return false;
        }

        // ------------------
        // CONNECTION ERRORS (always retryable) - Class 08 and 57P01
        // https://www.postgresql.org/docs/current/errcodes-appendix.html
        // ------------------
        if (sqlState.startsWith("08")) {
            // Class 08 — Connection Exception
            // 08000 connection_exception
            // 08003 connection_does_not_exist
            // 08006 connection_failure
            // 08001 sqlclient_unable_to_establish_sqlconnection
            // 08004 sqlserver_rejected_establishment_of_sqlconnection
            // 08007 transaction_resolution_unknown
            // 08P01 protocol_violation
            return true;
        }

        if (sqlState.equals("57P01")) {
            // admin_shutdown - server is shutting down (e.g., during zone outage)
            return true;
        }

        if (sqlState.equals("57P02")) {
            // crash_shutdown - server crashed
            return true;
        }

        if (sqlState.equals("57P03")) {
            // cannot_connect_now - server is starting up
            return true;
        }

        // ------------------
        // TRANSACTION ERRORS (retryable)
        // ------------------
        if (sqlState.equals("40001")) {
            // serialization_failure - transaction conflict, retry
            return true;
        }

        if (sqlState.equals("40P01")) {
            // deadlock_detected
            return true;
        }

        // ------------------
        // MYSQL: https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html
        // ------------------
        if (errorCode == 1213 && sqlState.equals("40001")) {
            // MySQL ER_LOCK_DEADLOCK
            return true;
        } else if (errorCode == 1205 && sqlState.equals("41000")) {
            // MySQL ER_LOCK_WAIT_TIMEOUT
            return true;
        }

        // ------------------
        // YUGABYTE/COCKROACH specific errors
        // ------------------
        if (sqlState.equals("40003")) {
            // statement_completion_unknown - common in distributed DBs during failover
            return true;
        }

        // Check for connection-related error messages
        String message = ex.getMessage();
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            if (lowerMessage.contains("connection") ||
                lowerMessage.contains("socket") ||
                lowerMessage.contains("timeout") ||
                lowerMessage.contains("broken pipe") ||
                lowerMessage.contains("reset by peer") ||
                lowerMessage.contains("no route to host") ||
                lowerMessage.contains("network is unreachable")) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check if this is a connection-related error that requires reconnection.
     */
    private boolean isConnectionError(SQLException ex) {
        String sqlState = ex.getSQLState();
        if (sqlState == null) {
            return false;
        }

        // Connection class errors or shutdown errors require reconnection
        if (sqlState.startsWith("08") ||
            sqlState.equals("57P01") ||
            sqlState.equals("57P02") ||
            sqlState.equals("57P03")) {
            return true;
        }

        // Also check message for connection keywords
        String message = ex.getMessage();
        if (message != null) {
            String lowerMessage = message.toLowerCase();
            return lowerMessage.contains("connection") ||
                   lowerMessage.contains("socket") ||
                   lowerMessage.contains("broken pipe") ||
                   lowerMessage.contains("reset by peer");
        }

        return false;
    }

    /**
     * Calculate exponential backoff with jitter.
     *
     * @param attempt Retry attempt number (0-based)
     * @param initialBackoffMs Initial backoff in milliseconds
     * @param maxBackoffMs Maximum backoff in milliseconds
     * @return Backoff duration in milliseconds
     */
    private long calculateBackoff(int attempt, int initialBackoffMs, int maxBackoffMs) {
        // Exponential backoff: initialBackoff * 2^attempt
        long backoff = initialBackoffMs * (1L << Math.min(attempt, 10)); // Cap exponent to avoid overflow
        backoff = Math.min(backoff, maxBackoffMs);

        // Add jitter (±25%)
        double jitter = 0.75 + (Math.random() * 0.5);
        return (long) (backoff * jitter);
    }

    /**
     * Optional callback that can be used to initialize the Worker right before
     * the benchmark execution begins
     */
    protected void initialize() {
        // The default is to do nothing
    }

    /**
     * Invoke a single transaction for the given TransactionType
     *
     * @param conn    TODO
     * @param txnType TODO
     * @return TODO
     * @throws UserAbortException TODO
     * @throws SQLException       TODO
     */
    protected abstract TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException;

    /**
     * Called at the end of the test to do any clean up that may be required.
     */
    public void tearDown() {
        if (!this.configuration.getNewConnectionPerTxn() && this.conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.error("Connection couldn't be closed.", e);
            }
        }
    }

    public void initializeState() {
        this.workloadState = this.configuration.getWorkloadState();
    }

    protected long getPreExecutionWaitInMillis(TransactionType type) {
        return 0;
    }

    protected long getPostExecutionWaitInMillis(TransactionType type) {
        return 0;
    }

}
