package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class AppendToStreamTests extends TestFixture {

    private FDB fdb;

    @BeforeEach
    void clean() throws ExecutionException, InterruptedException {
        fdb = FDB.selectAPIVersion(600);
        TestHelpers.clean(fdb);
    }

    @Test
    void shouldNotAllowAppendingZeroEventsToStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            // TODO: verify message: messages must not be null or empty
            assertThrows(IllegalArgumentException.class, () -> es.appendToStream("test-stream", ExpectedVersion.NO_STREAM, new NewStreamMessage[0]));
        }
    }

    @Test
    void shouldAppendWithNoStreamExpectedVersionOnFirstWriteIfStreamDoesNotYetExist() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            assertEquals(0, es.appendToStream(stream, ExpectedVersion.NO_STREAM, createNewStreamMessage()).get().getCurrentVersion());

            ReadStreamPage read = es.readStreamForwards(stream, 0, 2).get();
            assertEquals(1, read.getMessages().length);
        }
    }

    @Test
    void shouldAppendWithAnyExpectedVersionOnFirstWriteIfStreamDoesNotYetExist() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            assertEquals(0, es.appendToStream(stream, ExpectedVersion.ANY, createNewStreamMessage()).get().getCurrentVersion());

            ReadStreamPage read = es.readStreamForwards(stream, 0, 2).get();
            assertEquals(1, read.getMessages().length);
        }
    }

    // multiple_idempotent_writes
    // multiple_idempotent_writes_with_same_id_bug_case
    // in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id
    // in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id
    // should_fail_writing_with_correct_exp_ver_to_deleted_stream


    @Test
    void shouldReturnPositionWithWriting() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.NO_STREAM, messages).get();

            assertNotNull(appendResult.getCurrentPosition());
            assertEquals(-1, Position.START.compareTo(appendResult.getCurrentPosition()));
        }
    }

    // should_fail_writing_with_any_exp_ver_to_deleted_stream
    // should_fail_writing_with_invalid_exp_ver_to_deleted_stream


    @Test
    void shouldAppendWithCorrectExpectedVersionToExistingStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            String stream = "test-stream";
            es.appendToStream(stream, ExpectedVersion.NO_STREAM, createNewStreamMessage());
            assertDoesNotThrow(() -> es.appendToStream(stream, 0, createNewStreamMessage()));
        }
    }

    @Test
    void shouldAppendWithAnyExpectedVersionToExistingStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertEquals(0, es.appendToStream("test-stream", ExpectedVersion.NO_STREAM, createNewStreamMessage()).get().getCurrentVersion());
            assertEquals(1, es.appendToStream("test-stream", ExpectedVersion.ANY, createNewStreamMessage()).get().getCurrentVersion());
        }
    }

    @Test
    void shouldFailAppendingWithWrongExpectedVersionToExistingStream() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            assertEquals(0, es.appendToStream("test-stream", ExpectedVersion.NO_STREAM, createNewStreamMessage()).get().getCurrentVersion());
            // TODO: is there a way to have WrongExpectedVersionException bubble up instead of being wrapped in an ExecutionException?
            // What is the standard practice for this?
            //assertThrows(WrongExpectedVersionException.class, () -> es.appendToStream("test-stream", 1, createNewStreamMessage()));
            try {
                es.appendToStream("test-stream", 1, createNewStreamMessage()).get();
                fail("should throw exception");
            } catch (ExecutionException ex) {
                assertEquals(WrongExpectedVersionException.class, ex.getCause().getClass());
            } catch (Exception ex) {
                fail("wrong exception was thrown", ex);
            }
        }
    }

    // should_append_with_stream_exists_exp_ver_to_existing_stream
    // should_append_with_stream_exists_exp_ver_to_stream_with_multiple_events
    // should_append_with_stream_exists_exp_ver_if_metadata_stream_exists
    // should_fail_appending_with_stream_exists_exp_ver_and_stream_does_not_exist
    // should_fail_appending_with_stream_exists_exp_ver_to_hard_deleted_stream
    // should_fail_appending_with_stream_exists_exp_ver_to_soft_deleted_stream


    @Test
    void canAppendMultipleEventsAtOnce() throws ExecutionException, InterruptedException {
        try (Database db = fdb.open()) {
            EventStoreLayer es = EventStoreLayer.getDefault(db);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages).get();

            assertEquals(4, appendResult.getCurrentVersion());
            // TODO: nextExpectedVersion?
        }
    }

    // returns_failure_status_when_conditionally_appending_with_version_mismatch
    // returns_success_status_when_conditionally_appending_with_matching_version
    // returns_failure_status_when_conditionally_appending_to_a_deleted_stream

}
