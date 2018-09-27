package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AppendToStreamTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    @Test
    public void conflict() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.EMPTY_STREAM, messages);
            assertNotNull(appendResult.getCurrentPosition());
            assertEquals(-1, Position.START.compareTo(appendResult.getCurrentPosition()));

            AppendResult append2 = es.appendToStream("test-stream", ExpectedVersion.EMPTY_STREAM, createNewStreamMessages(7));


        }
    }

    // should_allow_appending_zero_events_to_stream_with_no_problems
    // should_create_stream_with_no_stream_exp_ver_on_first_write_if_does_not_exist
    // should_create_stream_with_any_exp_ver_on_first_write_if_does_not_exist
    // multiple_idempotent_writes
    // multiple_idempotent_writes_with_same_id_bug_case
    // in_wtf_multiple_case_of_multiple_writes_expected_version_any_per_all_same_id
    // in_slightly_reasonable_multiple_case_of_multiple_writes_with_expected_version_per_all_same_id
    // should_fail_writing_with_correct_exp_ver_to_deleted_stream
    // should_return_log_position_when_writing
    @Test
    public void shouldReturnPositionWithWriting() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // TODO: ExpectedVersion.EMPTY_STREAM doesnt work
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.NO_STREAM, messages);

            assertNotNull(appendResult.getCurrentPosition());
            assertEquals(-1, Position.START.compareTo(appendResult.getCurrentPosition()));
        }
    }

    // should_fail_writing_with_any_exp_ver_to_deleted_stream
    // should_fail_writing_with_invalid_exp_ver_to_deleted_stream
    // should_append_with_correct_exp_ver_to_existing_stream
    // should_append_with_any_exp_ver_to_existing_stream
    @Test
    public void shouldAppendWithAnyExpectedVersionToExistingStream() {
//        const string stream = "should_append_with_any_exp_ver_to_existing_stream";
//        using (var store = BuildConnection(_node))
//        {
//            store.ConnectAsync().Wait();
//            Assert.AreEqual(0, store.AppendToStreamAsync(stream, ExpectedVersion.EmptyStream, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
//            Assert.AreEqual(1, store.AppendToStreamAsync(stream, ExpectedVersion.Any, TestEvent.NewTestEvent()).Result.NextExpectedVersion);
//        }

        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            // TODO: appendToStream with EmptyStream
            // TODO: appendToStream with Any
            // NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            // AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            // assertEquals(4, appendResult.getCurrentVersion());
            // TOOD: nextExpectedVersion?
        }
    }
    // should_fail_appending_with_wrong_exp_ver_to_existing_stream
    @Test
    public void shouldFailAppendingWithWrongExpectedVersionToExistingStream() {

    }
    // should_append_with_stream_exists_exp_ver_to_existing_stream
    // should_append_with_stream_exists_exp_ver_to_stream_with_multiple_events
    // should_append_with_stream_exists_exp_ver_if_metadata_stream_exists
    // should_fail_appending_with_stream_exists_exp_ver_and_stream_does_not_exist
    // should_fail_appending_with_stream_exists_exp_ver_to_hard_deleted_stream
    // should_fail_appending_with_stream_exists_exp_ver_to_soft_deleted_stream
    // can_append_multiple_events_at_once
    @Test
    public void canAppendMultipleEventsAtOnce() {
        FDB fdb = FDB.selectAPIVersion(520);
        try (Database db = fdb.open()) {
            DirectorySubspace eventStoreSubspace = createEventStoreSubspace(db);
            EventStoreLayer es = new EventStoreLayer(db, eventStoreSubspace);

            NewStreamMessage[] messages = createNewStreamMessages(1, 2, 3, 4, 5);
            AppendResult appendResult = es.appendToStream("test-stream", ExpectedVersion.ANY, messages);

            assertEquals(4, appendResult.getCurrentVersion());
            // TOOD: nextExpectedVersion?
        }
    }
    // returns_failure_status_when_conditionally_appending_with_version_mismatch
    // returns_success_status_when_conditionally_appending_with_matching_version
    // returns_failure_status_when_conditionally_appending_to_a_deleted_stream

}
