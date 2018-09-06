package com.seancarroll;

// TODO: do we want to enforce json?
// I could see the need for json, bjson, binary, proto, etc
// in foundationdb is there a specific baked in concept for this type of thing?
public interface Serializer<T> {

    T to(Object o);


    <R> R from(T s, Class<R> type);

}
