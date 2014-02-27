/**
 *  Connection.cpp
 *
 *  Class representing a connection to a mongo daemon,
 *  a replica set or a mongos instance.
 *
 *  @copyright 2014 Copernica BV
 */

#include "includes.h"

/**
 *  Set up namespace
 */
namespace React { namespace Mongo {

/**
 *  Establish a connection to a mongo daemon or mongos instance.
 *
 *  The hostname may be postfixed with a colon, followed by the port number
 *  to connect to. If no port number is given, the default port of 27017 is
 *  assumed instead.
 *
 *  @param  loop        the event loop to bind to
 *  @param  host        single server to connect to
 */
Connection::Connection(React::Loop *loop, const std::string& host) :
    _loop(loop),
    _worker(loop),
    _master()
{
    // connect to mongo
    _worker.execute([this, host]() {
        // try to establish a connection to mongo
        try
        {
            // connect throws an exception on failure
            _mongo.connect(host);
        }
        catch (const mongo::DBException& exception)
        {
            // no callback to report the error
        }
    });
}

/**
 *  Establish a connection to a mongo daemon or mongos instance.
 *
 *  The hostname may be postfixed with a colon, followed by the port number
 *  to connect to. If no port number is given, the default port of 27017 is
 *  assumed instead.
 *
 *  @param  loop        the event loop to bind to
 *  @param  host        single server to connect to
 *  @param  callback    callback that will be executed when the connection is established or an error occured
 */
Connection::Connection(React::Loop *loop, const std::string& host, const std::function<void(Connection *connection, const char *error)>& callback) :
    _loop(loop),
    _worker(loop),
    _master()
{
    // connect to mongo
    _worker.execute([this, host, callback]() {
        // try to establish a connection to mongo
        try
        {
            // connect throws an exception on failure
            _mongo.connect(host);

            // so if we get here we are connected
            _master.execute([this, callback]() {
                callback(this, NULL);
            });
        }
        catch (const mongo::DBException& exception)
        {
            // something went awry, notify the listener
            _master.execute([this, callback, exception]() {
                callback(this, exception.toString().c_str());
            });
        }
    });
}

/**
 *  Convert a Variant object to a bson object
 *  used by the underlying mongo driver
 *
 *  @param  value   the value to convert
 */
mongo::BSONObj Connection::convert(const Variant::Value& value)
{
    // are we dealing with an array?
    if (value.type() == Variant::ValueVectorType)
    {
        // retrieve the entries in the value
        std::vector<Variant::Value> items = value;

        // the array object to fill
        mongo::BSONArrayBuilder builder;

        // iterate over all entries
        for (auto item : items)
        {
            // check type of item
            switch (item.type())
            {
                case Variant::ValueNullType:    builder.appendNull();               break;
                case Variant::ValueIntType:     builder.append((int) item);         break;
                case Variant::ValueStringType:  builder.append((std::string) item); break;
                case Variant::ValueVectorType:  builder.append(convert(item));      break;
                case Variant::ValueMapType:     builder.append(convert(item));      break;
                default:
                    break;
            }
        }

        // convert to an object and return
        return builder.obj();
    }

    // or with a map?
    if (value.type() == Variant::ValueMapType)
    {
        // retrieve the members of the value
        std::map<std::string, Variant::Value> members = value;

        // we need an object builder to add elements to
        mongo::BSONObjBuilder builder;

        // iterate over all members
        for (auto member : members)
        {
            // check type of the member
            switch (member.second.type())
            {
                case Variant::ValueNullType:    builder.appendNull(member.first);                           break;
                case Variant::ValueIntType:     builder.append(member.first, (int) member.second);          break;
                case Variant::ValueStringType:  builder.append(member.first, (std::string) member.second);  break;
                case Variant::ValueVectorType:  builder.append(member.first, convert(member.second));       break;
                case Variant::ValueMapType:     builder.append(member.first, convert(member.second));       break;
            }
        }

        // return the finished object
        return builder.obj();
    }

    // the value should be a vector or a map, this is invalid
    return mongo::BSONObj();
}

/**
 *  Convert a mongo bson object used in the
 *  underlying library to a Variant object
 *
 *  @param  value   the value to convert
 */
Variant::Value Connection::convert(const mongo::BSONObj& value)
{
    // retrieve element value
    auto convertElement = [this](const mongo::BSONElement& element) -> Variant::Value {
        // check the element type
        switch (element.type())
        {
            case mongo::String:     return Variant::Value(element.str());
            case mongo::Object:     return convert(element.Obj());
            case mongo::Array:      return convert(element.Obj());
            case mongo::jstNULL:    return Variant::Value(nullptr);
            case mongo::NumberInt:  return Variant::Value(element.numberInt());
            default:
                // unsupported type
                return Variant::Value{};
        }
    };

    // is this an array object?
    if (value.couldBeArray())
    {
        // the vector to construct the result from
        std::vector<Variant::Value> result;

        // "iterate" over all the values
        for (auto iter = value.begin(); iter.more(); )
        {
            // retrieve the element
            auto element = iter.next();

            // add element to the result
            result.push_back(convertElement(element));
        }

        // return the result
        return result;
    }
    else
    {
        // the map to construct the result from
        std::map<std::string, Variant::Value> result;

        // "iterate" over all the values
        for (auto iter = value.begin(); iter.more(); )
        {
            // retrieve the element
            auto element = iter.next();

            // add element to the result
            result[element.fieldName()] = convertElement(element);
        }

        // return the result
        return result;
    }
}

/**
 *  Get whether we are connected to mongo?
 *
 *  @param  callback    the callback that will be informed of the connection status
 */
void Connection::connected(const std::function<void(bool connected)>& callback)
{
    // check in worker
    _worker.execute([this, callback]() {
        // retrieve the result
        auto result = !_mongo.isFailed();

        // execute the callback in master thread
        _master.execute([result, callback]() {
            callback(result);
        });
    });
}

/**
 *  Query a collection
 *
 *  @param  collection  database name and collection
 *  @param  query       the query to execute
 *  @param  callback    the callback that will be called with the results
 */
void Connection::query(const std::string& collection, Variant::Value&& query, const std::function<void(Variant::Value&& result, const char *error)>& callback)
{
    // move the query to a pointer to avoid needless copying
    auto *request = new Variant::Value(std::move(query));

    // run the query in the worker
    _worker.execute([this, collection, callback, request]() {
        try
        {
            // execute query
            auto cursor = _mongo.query(collection, convert(*request));

            // clean up the query object
            delete request;

            /**
             *  Even though mongo can throw exceptions for the query
             *  function, it communicates connection failures not by
             *  throwing an exception, but instead returning 0 when
             *  a connection failure occurs, so we check for this.
             */
            if (cursor.get() == NULL)
            {
                // notify listener that a connection failure occured
                _master.execute([callback]() {
                    callback(Variant::Value{}, "Unspecified connection error");
                });

                // don't try to read from the cursor
                return;
            }

            // build the result value
            auto *result = new std::vector<Variant::Value>;

            // process all results
            while (cursor->more()) result->push_back(convert(cursor->next()));

            // we now have all results, execute callback in master thread
            _master.execute([result, callback]() {
                // execute callback
                callback(*result, NULL);

                // clean up the result object
                delete result;
            });
        }
        catch (mongo::DBException& exception)
        {
            // clean up the query object
            delete request;

            // something went awry, notify listener
            _master.execute([callback, exception]() {
                // run callback with an empty result object and an error message
                callback(Variant::Value{}, exception.toString().c_str());
            });
        }
    });
}

/**
 *  Query a collection
 *
 *  Note:   This function will make a copy of the query object. This
 *          can be useful when you want to reuse the given query object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  database name and collection
 *  @param  query       the query to execute
 *  @param  callback    the callback that will be called with the results
 */
void Connection::query(const std::string& collection, const Variant::Value& query, const std::function<void(Variant::Value&& result, const char *error)>& callback)
{
    // move a copy to the implementation
    this->query(collection, Variant::Value(query), callback);
}

/**
 *  Insert a document into a collection
 *
 *  @param  collection  database name and collection
 *  @param  document    document to insert
 *  @param  callback    the callback that will be informed when insert is complete or failed
 */
void Connection::insert(const std::string& collection, Variant::Value&& document, const std::function<void(const char *error)>& callback)
{
    // move the document to a pointer to avoid needless copying
    auto *insert = new Variant::Value(std::move(document));

    // run the insert in the worker
    _worker.execute([this, collection, insert, callback]() {
        try
        {
            // execute the insert
            _mongo.insert(collection, convert(*insert));

            // free up the document
            delete insert;

            // the error that could have occured
            auto error = _mongo.getLastError();

            // check whether an error occured
            if (error.empty())
            {
                // inform the listener the insert is done
                _master.execute([callback]() {
                    callback(NULL);
                });
            }
            else
            {
                // something freaky just happened, inform the listener
                _master.execute([callback, error]() {
                    callback(error.c_str());
                });
            }
        }
        catch (mongo::DBException& exception)
        {
            // free up the document
            delete insert;

            // inform the listener of the specific failure
            _master.execute([callback, exception]() {
                callback(exception.toString().c_str());
            });
        }
    });
}

/**
 *  Insert a document into a collection
 *
 *  Note:   This function will make a copy of the document object. This
 *          can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  database name and collection
 *  @param  document    document to insert
 *  @param  callback    the callback that will be informed when insert is complete or failed
 */
void Connection::insert(const std::string& collection, const Variant::Value& document, const std::function<void(const char *error)>& callback)
{
    // move a copy to the implementation
    insert(collection, Variant::Value(document), callback);
}

/**
 *  Insert a document into a collection
 *
 *  This function does not report on whether the insert was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  @param  collection  database name and collection
 *  @param  document    document to insert
 */
void Connection::insert(const std::string& collection, Variant::Value&& document)
{
    // move the document to a pointer to avoid needless copying
    auto *insert = new Variant::Value(std::move(document));

    // run the insert in the worker
    _worker.execute([this, collection, insert]() {
        try
        {
            // execute the insert
            _mongo.insert(collection, convert(*insert));

            // free up the document
            delete insert;
        }
        catch (mongo::DBException& exception)
        {
            // free up the document
            delete insert;
        }
    });
}

/**
 *  Insert a document into a collection
 *
 *  This function does not report on whether the insert was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  Note:   This function will make a copy of the document object. This
 *          can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  database name and collection
 *  @param  document    document to insert
 */
void Connection::insert(const std::string& collection, const Variant::Value& document)
{
    // move a copy to the implementation
    insert(collection, Variant::Value(document));
}

/**
 *  Update an existing document in a collection
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  document    the new document to replace existing document with
 *  @param  callback    the callback that will be informed when update is complete or failed
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, Variant::Value&& query, Variant::Value&& document, const std::function<void(const char *error)>& callback, bool upsert, bool multi)
{
    // move the query and document to a pointer to avoid needless copying
    auto request = new Variant::Value(std::move(query));
    auto *update = new Variant::Value(std::move(document));

    // run the update in the worker
    _worker.execute([this, collection, request, update, callback, upsert, multi]() {
        try
        {
            // execute the update
            _mongo.update(collection, convert(*request), convert(*update), upsert, multi);

            // clean up
            delete request;
            delete update;

            // the error that could have occured
            auto error = _mongo.getLastError();

            // check whether an error occured
            if (error.empty())
            {
                // inform the listener the insert is done
                _master.execute([callback]() {
                    callback(NULL);
                });
            }
            else
            {
                // something freaky just happened, inform the listener
                _master.execute([callback, error]() {
                    callback(error.c_str());
                });
            }
        }
        catch (const mongo::DBException& exception)
        {
            // clean up
            delete request;
            delete update;

            // inform the listener of the failure
            _master.execute([callback, exception]() {
                callback(exception.toString().c_str());
            });
        }
    });
}

/**
 *  Update an existing document in a collection
 *
 *  Note:   This function will make a copy of the query object.
 *          This can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  query       the query to find the document(s) to update
 *  @param  document    the new document to replace existing document with
 *  @param  callback    the callback that will be informed when update is complete or failed
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, const Variant::Value& query, Variant::Value&& document, const std::function<void(const char *error)>& callback, bool upsert, bool multi)
{
    // move copies to the implementation
    update(collection, Variant::Value(query), std::move(document), callback, upsert, multi);
}

/**
 *  Update an existing document in a collection
 *
 *  Note:   This function will make a copy of the document object.
 *          This can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  query       the query to find the document(s) to update
 *  @param  document    the new document to replace existing document with
 *  @param  callback    the callback that will be informed when update is complete or failed
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, Variant::Value&& query, const Variant::Value& document, const std::function<void(const char *error)>& callback, bool upsert, bool multi)
{
    // move copies to the implementation
    update(collection, std::move(query), Variant::Value(document), callback, upsert, multi);
}

/**
 *  Update an existing document in a collection
 *
 *  Note:   This function will make a copy of the query and document object.
 *          This can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  query       the query to find the document(s) to update
 *  @param  document    the new document to replace existing document with
 *  @param  callback    the callback that will be informed when update is complete or failed
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, const Variant::Value& query, const Variant::Value& document, const std::function<void(const char *error)>& callback, bool upsert, bool multi)
{
    // move copies to the implementation
    update(collection, Variant::Value(query), Variant::Value(document), callback, upsert, multi);
}

/**
 *  Update an existing document in a collection
 *
 *  This function does not report on whether the insert was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  query       the query to find the document(s) to update
 *  @param  document    the new document to replace existing document with
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, Variant::Value&& query, Variant::Value&& document, bool upsert, bool multi)
{
    // move the query and document to a pointer to avoid needless copying
    auto request = new Variant::Value(std::move(query));
    auto *update = new Variant::Value(std::move(document));

    // run the update in the worker
    _worker.execute([this, collection, request, update, upsert, multi]() {
        try
        {
            // execute the update
            _mongo.update(collection, convert(*request), convert(*update), upsert, multi);

            // clean up
            delete request;
            delete update;
        }
        catch (const mongo::DBException& exception)
        {
            // clean up
            delete request;
            delete update;
        }
    });
}

/**
 *  Update an existing document in a collection
 *
 *  This function does not report on whether the insert was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  Note:   This function will make a copy of the query object.
 *          This can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  query       the query to find the document(s) to update
 *  @param  document    the new document to replace existing document with
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, const Variant::Value& query, Variant::Value&& document, bool upsert, bool multi)
{
    // move copies to the implementation
    update(collection, Variant::Value(query), std::move(document), upsert, multi);
}

/**
 *  Update an existing document in a collection
 *
 *  This function does not report on whether the insert was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  Note:   This function will make a copy of the document object.
 *          This can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  query       the query to find the document(s) to update
 *  @param  document    the new document to replace existing document with
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, Variant::Value&& query, const Variant::Value& document, bool upsert, bool multi)
{
    // move copies to the implementation
    update(collection, std::move(query), Variant::Value(document), upsert, multi);
}

/**
 *  Update an existing document in a collection
 *
 *  This function does not report on whether the insert was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  Note:   This function will make a copy of the query and document object.
 *          This can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  query       the query to find the document(s) to update
 *  @param  document    the new document to replace existing document with
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */

void Connection::update(const std::string& collection, const Variant::Value& query, const Variant::Value& document, bool upsert, bool multi)
{
    // move copies to the implementation
    update(collection, Variant::Value(query), Variant::Value(document), upsert, multi);
}

/**
 *  Remove one or more existing documents from a collection
 *
 *  @param  collection  collection holding the document(s) to be removed
 *  @param  query       the query to find the document(s) to remove
 *  @param  callback    the callback that will be informed once the delete is complete or failed
 *  @param  limitToOne  limit the removal to a single document
 */
void Connection::remove(const std::string& collection, Variant::Value&& query, const std::function<void(const char *error)>& callback, bool limitToOne)
{
    // move the query to a pointer to avoid needless copying
    auto *request = new Variant::Value(std::move(query));

    // run the remove in the worker
    _worker.execute([this, collection, request, callback, limitToOne]() {
        try
        {
            // execute remove query
            _mongo.remove(collection, convert(*request), limitToOne);

            // free the request
            delete request;

            // the error that could have occured
            auto error = _mongo.getLastError();

            // check whether an error occured
            if (error.empty())
            {
                // inform the listener the insert is done
                _master.execute([callback]() {
                    callback(NULL);
                });
            }
            else
            {
                // something freaky just happened, inform the listener
                _master.execute([callback, error]() {
                    callback(error.c_str());
                });
            }
        }
        catch (const mongo::DBException& exception)
        {
            // free the request
            delete request;

            // inform the listener of the failure
            _master.execute([callback, exception]() {
                callback(exception.toString().c_str());
            });
        }
    });
}

/**
 *  Remove one or more existing documents from a collection
 *
 *  Note:   This function will make a copy of the query object.
 *          This can be useful when you want to reuse the given query object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection holding the document(s) to be removed
 *  @param  query       the query to find the document(s) to remove
 *  @param  callback    the callback that will be informed once the delete is complete or failed
 *  @param  limitToOne  limit the removal to a single document
 */
void Connection::remove(const std::string& collection, const Variant::Value& query, const std::function<void(const char *error)>& callback, bool limitToOne)
{
    // move copy to the implementation
    remove(collection, Variant::Value(query), callback, limitToOne);
}

/**
 *  Remove one or more existing documents from a collection
 *
 *  This function does not report on whether the remove was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  @param  collection  collection holding the document(s) to be removed
 *  @param  query       the query to find the document(s) to remove
 *  @param  limitToOne  limit the removal to a single document
 */
void Connection::remove(const std::string& collection, Variant::Value&& query, bool limitToOne)
{
    // move the query to a pointer to avoid needless copying
    auto *request = new Variant::Value(std::move(query));

    // run the remove in the worker
    _worker.execute([this, collection, request, limitToOne]() {
        try
        {
            // execute remove query
            _mongo.remove(collection, convert(*request), limitToOne);

            // free the request
            delete request;
        }
        catch (const mongo::DBException& exception)
        {
            // free the request
            delete request;
        }
    });
}

/**
 *  Remove one or more existing documents from a collection
 *
 *  This function does not report on whether the remove was successful
 *  or not. It avoids a little bit of overhead from context switches
 *  and a roundtrip to mongo to retrieve the last eror, and is
 *  therefore a little faster.
 *
 *  It is best used for non-critical data, like cached data that can
 *  easily be reconstructed if the data somehow does not reach mongo.
 *
 *  Note:   This function will make a copy of the query object.
 *          This can be useful when you want to reuse the given query object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  collection  collection holding the document(s) to be removed
 *  @param  query       the query to find the document(s) to remove
 *  @param  limitToOne  limit the removal to a single document
 */
void Connection::remove(const std::string& collection, const Variant::Value& query, bool limitToOne)
{
    // move copy to the implementation
    remove(collection, Variant::Value(query), limitToOne);
}

/**
 *  End namespace
 */
}}
