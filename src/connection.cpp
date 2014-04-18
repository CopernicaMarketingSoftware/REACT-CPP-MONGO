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

            // do we have anyone watching the connect callback
            if (_connectCallback) _master.execute([this]() { _connectCallback(nullptr); });
        }
        catch (const mongo::DBException& exception)
        {
            // do we have anyone watching the connect callback
            if (_connectCallback) _master.execute([this, exception]() { _connectCallback(exception.toString().c_str()); });
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
                case Variant::ValueBoolType:    builder.append((bool) item);        break;
                case Variant::ValueIntType:     builder.append((int) item);         break;
                case Variant::ValueDoubleType:  builder.append((double) item);      break;
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
                case Variant::ValueBoolType:    builder.append(member.first, (bool) member.second);         break;
                case Variant::ValueIntType:     builder.append(member.first, (int) member.second);          break;
                case Variant::ValueDoubleType:  builder.append(member.first, (double) member.second);       break;
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
            case mongo::NumberDouble:   return Variant::Value(element.numberDouble());
            case mongo::String:         return Variant::Value(element.str());
            case mongo::Object:         return convert(element.Obj());
            case mongo::Array:          return convert(element.Obj());
            case mongo::Bool:           return Variant::Value(element.boolean());
            case mongo::jstNULL:        return Variant::Value(nullptr);
            case mongo::NumberInt:      return Variant::Value(element.numberInt());
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
 *  Get a call when the connection succeeds or fails
 *
 *  @param  callback    the callback that will be informed of the connection status
 */
void Connection::onConnected(const std::function<void(const char *error)>& callback)
{
    // register the callback
    _connectCallback = callback;
}

/**
 *  Query a collection
 *
 *  @param  collection  database name and collection
 *  @param  query       the query to execute
 *
 *  The returned deferred object has an onSuccess method that
 *  will give the result as an rvalue-reference. It can thus
 *  be used something like this:
 *
 *  connection.query("collection", Variant::Value()).onSuccess([](Variant::Value&& result) {
 *      // do something with result here
 *  });
 */
DeferredQuery& Connection::query(const std::string& collection, Variant::Value&& query)
{
    // move the query to a pointer to avoid needless copying
    auto request = std::make_shared<Variant::Value>(std::move(query));

    // create the deferred handler
    auto deferred = std::make_shared<DeferredQuery>();

    // run the query in the worker
    _worker.execute([this, collection, request, deferred]() {
        try
        {
            // execute query
            auto cursor = _mongo.query(collection, convert(*request));

            /**
             *  Even though mongo can throw exceptions for the query
             *  function, it communicates connection failures not by
             *  throwing an exception, but instead returning 0 when
             *  a connection failure occurs, so we check for this.
             */
            if (cursor.get() == NULL)
            {
                // notify listener that a connection failure occured
                _master.execute([deferred]() { deferred->failure("Unspecified connection error"); });
                return;
            }

            // build the result value
            auto result = std::make_shared<std::vector<Variant::Value>>();

            // process all results
            while (cursor->more()) result->push_back(convert(cursor->next()));

            // we now have all results, execute callback in master thread
            _master.execute([result, deferred]() { deferred->success(std::move(*result)); });
        }
        catch (mongo::DBException& exception)
        {
            // something went awry, notify listener
            _master.execute([deferred, exception]() { deferred->failure(exception.toString().c_str()); });
        }
    });

    // return the deferred handler
    return *deferred;
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
 *
 *  The returned deferred object has an onSuccess method that
 *  will give the result as an rvalue-reference. It can thus
 *  be used something like this:
 *
 *  connection.query("collection", Variant::Value()).onSuccess([](Variant::Value&& result) {
 *      // do something with result here
 *  });
 */
DeferredQuery& Connection::query(const std::string& collection, const Variant::Value& query)
{
    // throw a copy to the implementation
    return this->query(collection, Variant::Value(query));
}

/**
 *  Insert a document into a collection
 *
 *  @param  collection  database name and collection
 *  @param  document    document to insert
 */
DeferredInsert& Connection::insert(const std::string& collection, Variant::Value&& document)
{
    // move the document to a pointer to avoid needless copying
    auto insert = std::make_shared<Variant::Value>(std::move(document));

    // create the deferred handler
    auto deferred = std::make_shared<DeferredInsert>();

    // run the insert in the worker
    _worker.execute([this, collection, insert, deferred]() {
        try
        {
            // execute the insert
            _mongo.insert(collection, convert(*insert));

            // is anybody interested in the result?
            if (!deferred->requireStatus())
            {
                // inform the listener we are done
                _master.execute([deferred]() { deferred->complete(); });
                return;
            }

            // the error that could have occured
            auto error = _mongo.getLastError();

            // check whether an error occured
            if (error.empty()) _master.execute([deferred]() { deferred->success(); });
            else _master.execute([deferred, error]() { deferred->failure(error.c_str()); });
        }
        catch (mongo::DBException& exception)
        {
            // inform the listener of the specific failure
            _master.execute([deferred, exception]() { deferred->failure(exception.toString().c_str()); });
        }
    });

    // return the deferred handler
    return *deferred;
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
 */
DeferredInsert& Connection::insert(const std::string& collection, const Variant::Value& document)
{
    // move a copy to the implementation
    return insert(collection, Variant::Value(document));
}

/**
 *  Insert a batch of documents into a collection
 *
 *  @param  collection  database name and collection
 *  @param  documents   documents to insert
 */
DeferredInsert& Connection::insert(const std::string& collection, const std::vector<Variant::Value>& documents)
{
    // create a new vector with the mongo objects
    // we use a pointer to avoid needless copying
    auto insert = std::make_shared<std::vector<mongo::BSONObj>>();

    // create the deferred handler
    auto deferred = std::make_shared<DeferredInsert>();

    // allocate memory for the objects
    insert->reserve(documents.size());

    // insert all documents
    for (auto &document : documents) insert->push_back(convert(document));

    // run the insert in the worker
    _worker.execute([this, collection, insert, deferred]() {
        try
        {
            // execute the insert
            _mongo.insert(collection, *insert);

            // is anybody interested in the result?
            if (!deferred->requireStatus())
            {
                // inform the listener we are done
                _master.execute([deferred]() { deferred->complete(); });
                return;
            }

            // the error that could have occured
            auto error = _mongo.getLastError();

            // check whether an error occured
            if (error.empty()) _master.execute([deferred]() { deferred->success(); });
            else _master.execute([deferred, error]() { deferred->failure(error.c_str()); });
        }
        catch (mongo::DBException& exception)
        {
            // inform the listener of the specific failure
            _master.execute([deferred, exception]() { deferred->failure(exception.toString().c_str()); });
        }
    });

    // return the deferred handler
    return *deferred;
}

/**
 *  Update an existing document in a collection
 *
 *  @param  collection  collection keeping the document to be updated
 *  @param  document    the new document to replace existing document with
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */
DeferredUpdate& Connection::update(const std::string& collection, Variant::Value&& query, Variant::Value&& document, bool upsert, bool multi)
{
    // move the query and document to a pointer to avoid needless copying
    auto request = std::make_shared<Variant::Value>(std::move(query));
    auto update  = std::make_shared<Variant::Value>(std::move(document));

    // create the deferred handler
    auto deferred = std::make_shared<DeferredUpdate>();

    // run the update in the worker
    _worker.execute([this, collection, request, update, deferred, upsert, multi]() {
        try
        {
            // execute the update
            _mongo.update(collection, convert(*request), convert(*update), upsert, multi);

            // is anybody interested in the result?
            if (!deferred->requireStatus())
            {
                // inform the listener we are done
                _master.execute([deferred]() { deferred->complete(); });
                return;
            }

            // the error that could have occured
            auto error = _mongo.getLastError();

            // check whether an error occured
            if (error.empty()) _master.execute([deferred]() { deferred->success(); });
            else _master.execute([deferred, error]() { deferred->failure(error.c_str()); });
        }
        catch (const mongo::DBException& exception)
        {
            // inform the listener of the failure
            _master.execute([deferred, exception]() { deferred->failure(exception.toString().c_str()); });
        }
    });

    // return the deferred handler
    return *deferred;
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
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */
DeferredUpdate& Connection::update(const std::string& collection, const Variant::Value& query, Variant::Value&& document, bool upsert, bool multi)
{
    // move copies to the implementation
    return update(collection, Variant::Value(query), std::move(document), upsert, multi);
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
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */
DeferredUpdate& Connection::update(const std::string& collection, Variant::Value&& query, const Variant::Value& document, bool upsert, bool multi)
{
    // move copies to the implementation
    return update(collection, std::move(query), Variant::Value(document), upsert, multi);
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
 *  @param  upsert      if no matching document was found, create one instead
 *  @param  multi       if multiple matching documents are found, update them all
 */
DeferredUpdate& Connection::update(const std::string& collection, const Variant::Value& query, const Variant::Value& document, bool upsert, bool multi)
{
    // move copies to the implementation
    return update(collection, Variant::Value(query), Variant::Value(document), upsert, multi);
}

/**
 *  Remove one or more existing documents from a collection
 *
 *  @param  collection  collection holding the document(s) to be removed
 *  @param  query       the query to find the document(s) to remove
 *  @param  limitToOne  limit the removal to a single document
 */
DeferredRemove& Connection::remove(const std::string& collection, Variant::Value&& query, bool limitToOne)
{
    // move the query to a pointer to avoid needless copying
    auto request = std::make_shared<Variant::Value>(std::move(query));

    // create the deferred handler
    auto deferred = std::make_shared<DeferredRemove>();

    // run the remove in the worker
    _worker.execute([this, collection, request, deferred, limitToOne]() {
        try
        {
            // execute remove query
            _mongo.remove(collection, convert(*request), limitToOne);

            // is anybody interested in the result?
            if (!deferred->requireStatus())
            {
                // inform the listener we are done
                _master.execute([deferred]() { deferred->complete(); });
                return;
            }

            // the error that could have occured
            auto error = _mongo.getLastError();

            // check whether an error occured
            if (error.empty()) _master.execute([deferred]() { deferred->success(); });
            else _master.execute([deferred, error]() { deferred->failure(error.c_str()); });
        }
        catch (const mongo::DBException& exception)
        {
            // inform the listener of the failure
            _master.execute([deferred, exception]() { deferred->failure(exception.toString().c_str()); });
        }
    });

    // return the deferred handler
    return *deferred;
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
 *  @param  limitToOne  limit the removal to a single document
 */
DeferredRemove& Connection::remove(const std::string& collection, const Variant::Value& query, bool limitToOne)
{
    // move copy to the implementation
    return remove(collection, Variant::Value(query), limitToOne);
}

/**
 *  Run a command on the connection.
 *
 *  This is the general way to run commands on the database that are
 *  not (yet) part of the driver. This allows you to run new commands
 *  available in the mongodb daemon.
 *
 *  @param  database    the database to run the command on (not including the collection name)
 *  @param  command     the command to execute
 */
DeferredCommand& Connection::runCommand(const std::string& database, Variant::Value&& query)
{
    // move the query to a pointer to avoid needless copying
    auto request = std::make_shared<Variant::Value>(std::move(query));

    // create the deferred handler
    auto deferred = std::make_shared<DeferredCommand>();

    // run the command in the worker
    _worker.execute([this, database, request, deferred]() {
        try
        {
            // create a new mongo object, because for some reason
            // the mongo library does not return one here, it wants
            // it to be passed in by reference so that it can modify
            // it for us. sort of like we're back in plain C.
            auto result = std::make_shared<mongo::BSONObj>();

            // execute the command
            _mongo.runCommand(database, convert(*request), *result);

            // is anybody interested in the result
            if (!deferred->requireStatus())
            {
                // inform the listener we are done
                _master.execute([deferred]() { deferred->complete(); });
                return;
            }

            // is this a hidden error muffled away
            if (!result->getField("ok").numberDouble())
            {
                // there should be an error string
                _master.execute([deferred, result]() { deferred->failure(result->getField("error").str().c_str()); });
                return;
            }

            // convert the result to a Variant
            auto output = std::make_shared<Variant::Value>(convert(*result));

            // and execute the callback
            _master.execute([deferred, output]() { deferred->success(std::move(*output)); });
        }
        catch (const mongo::DBException& exception)
        {
            // inform the listener of the failure
            _master.execute([deferred, exception]() { deferred->failure(exception.toString().c_str()); });
        }
    });

    // return the deferred handler
    return *deferred;
}

/**
 *  Run a command on the connection.
 *
 *  This is the general way to run commands on the database that are
 *  not (yet) part of the driver. This allows you to run new commands
 *  available in the mongodb daemon.
 *
 *  Note:   This function will make a copy of the query and document object.
 *          This can be useful when you want to reuse the given document object,
 *          otherwise it is best to pass in an rvalue and avoid the copy.
 *
 *  @param  database    the database to run the command on (not including the collection name)
 *  @param  command     the command to execute
 */
DeferredCommand& Connection::runCommand(const std::string& database, const Variant::Value& query)
{
    // move copy to the implementation
    return runCommand(database, Variant::Value(query));
}

/**
 *  End namespace
 */
}}
