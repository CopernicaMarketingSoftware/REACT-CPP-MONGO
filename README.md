REACT-CPP-MONGO
===============

C++ asynchronous mongo library on top of the REACT-CPP library. Uses lambdas and callbacks to return query results.

This library works mostly the same as the regular mongo c++ library, except all functions accept a lambda argument
that will be called when results (or failure) are available.

```c
React::Mongo::Connection mongo("mongodb.example.org", [](const char *error) {
    // if no error occured, we will receive a null pointer
    if (!error) std::cout << "Connected successfully" << std::endl;

    // otherwise, we will get a description of what exactly went wrong
    else std::cout << "Connection error: " << error << std::endl;
});

/**
 *  The mongo object will be created immediately, even though the connection
 *  might not have been established. It is safe, however, to immediately run
 *  the next command on the object. They will be executed after a connection
 *  was established. Should the library fail to connect, all registered calls
 *  will receive an error.
 */

// build a query to find a specific document
Variant::Value query;
query["_id"] = "documentid";

// retrieve the document
mongo.query("database.collection", std::move(query), [](Variant::Value&& result, const char *error) {
    // check for an error
    if (error)
    {
        std::cout << "Something went wrong querying: " << error << std::endl;
        return;
    }

    // since we search for an exact id, we will get a maximum of one result
    // however, this result will always be an array
    if (result.size() == 0)
    {
        std::cout << "Could not find any document with that ID" << std::endl;
        return;
    }

    // assume that the document has a string field named 'firstname'
    std::string firstname = result[0]["firstname"];
});
```

Inserting a document is just as easy:

```c
Variant::Value document;
document["name"] = "Example User";
document["email"] = "info@example.org";
document["address"]["city"] = "Gotham City";
document["address"]["phone"] = "555-BATMAN";

mongo.insert("database.collection", std::move(document), [](const char *error) {
    // check for an error
    if (!error) std::cout << "Document inserted" << std::endl;

    // otherwise, we will get a description of what exactly went wrong
    std::cout << "Could not insert document: " << error << std::endl;
});
```

All functions, with the exception of the query function, can also be called
without a callback. This way, the library will do its very best to make sure
the action is carried out, but there is no feedback, and failure is silent.

This can be useful when e.g. storing cached data, that could easily be created
again in case the insert failed. They are significantly faster because checking
for an error takes an extra roundtrip to the mongodb server.

When we are not interested in the result of an insert operation, we could have
written the previous example like this:

```c
Variant::Value document;
document["name"] = "Example User";
document["email"] = "info@example.org";
document["address"]["city"] = "Gotham City";
document["address"]["phone"] = "555-BATMAN";

mongo.insert("database.collection", std::move(document));
```

MOVING DATA
===========
You might have noticed that when we pass a document to the library, we have used
std::move in all examples so far. While this may not have a big impact in these
small examples, working with bigger documents will be noticeably faster using
move semantics.

In practice, it is always best to move the object, unless you wish to use the
same document again later on. Similarly, all callback functions receiving a
document will always receive a movable object. When storing this object, e.g.
as a member variable, you should use std::move for this.
