package com.seancarroll.foundationdb.es;

import com.apple.foundationdb.FDB;
import org.junit.jupiter.api.BeforeEach;

public class ReadEventTests extends TestFixture {

    @BeforeEach
    public void clean() {
        FDB fdb = FDB.selectAPIVersion(520);
        TestHelpers.clean(fdb);
    }

    // throw_if_stream_id_is_null
    // throw_if_stream_id_is_empty
    // throw_if_event_number_is_less_than_minus_one
    // notify_using_status_code_if_stream_not_found
    // return_no_stream_if_requested_last_event_in_empty_stream
    // notify_using_status_code_if_stream_was_deleted
    // notify_using_status_code_if_stream_does_not_have_event
    // return_existing_event
    // retrieve_the_is_json_flag_properly
    // return_last_event_in_stream_if_event_number_is_minus_one

}
