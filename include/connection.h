/**
 *  Connection.h
 *
 *  Class representing a connection to a mongo daemon,
 *  a replica set or a mongos instance.
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace React { namespace Mongo {

/**
 *  Connection class
 */
class Connection
{
private:
    /**
     *  The loop to bind to
     */
    React::Loop *_loop;

    /**
     *  The worker operating on mongo
     */
    React::Worker _worker;

    /**
     *  Worker for main thread
     */
    React::Worker _master;

    /**
     *  Underlying connection to mongo
     */
    mongo::DBClientConnection _mongo;

    /**
     *  Convert a Variant object to a bson object
     *  used by the underlying mongo driver
     *
     *  @param  value   the value to convert
     */
    mongo::BSONObj convert(const Variant::Value& value);

    /**
     *  Convert a mongo bson object used in the
     *  underlying library to a Variant object
     *
     *  @param  value   the value to convert
     */
    Variant::Value convert(const mongo::BSONObj& value);
public:
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
    Connection(React::Loop *loop, const std::string& host);

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
    Connection(React::Loop *loop, const std::string& host, const std::function<void(Connection *connection, const char *error)>& callback);

    /**
     *  Get whether we are connected to mongo?
     *
     *  @param  callback    the callback that will be informed of the connection status
     */
    void connected(const std::function<void(bool connected)>& callback);

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
    DeferredQuery& query(const std::string& collection, Variant::Value&& query);

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
    DeferredQuery& query(const std::string& collection, const Variant::Value& query);

    /**
     *  Insert a document into a collection
     *
     *  @param  collection  database name and collection
     *  @param  document    document to insert
     */
    DeferredInsert& insert(const std::string& collection, Variant::Value&& document);

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
    DeferredInsert& insert(const std::string& collection, const Variant::Value& document);

    /**
     *  Insert a batch of documents into a collection
     *
     *  @param  collection  database name and collection
     *  @param  documents   documents to insert
     */
    DeferredInsert& insert(const std::string& collection, const std::vector<Variant::Value>& documents);

    /**
     *  Update an existing document in a collection
     *
     *  @param  collection  collection keeping the document to be updated
     *  @param  query       the query to find the document(s) to update
     *  @param  document    the new document to replace existing document with
     *  @param  upsert      if no matching document was found, create one instead
     *  @param  multi       if multiple matching documents are found, update them all
     */
    DeferredUpdate& update(const std::string& collection, Variant::Value&& query, Variant::Value&& document, bool upsert = false, bool multi = false);

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
    DeferredUpdate& update(const std::string& collection, const Variant::Value& query, Variant::Value&& document, bool upsert = false, bool multi = false);

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
    DeferredUpdate& update(const std::string& collection, Variant::Value&& query, const Variant::Value& document, bool upsert = false, bool multi = false);

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
    DeferredUpdate& update(const std::string& collection, const Variant::Value& query, const Variant::Value& document, bool upsert = false, bool multi = false);

    /**
     *  Remove one or more existing documents from a collection
     *
     *  @param  collection  collection holding the document(s) to be removed
     *  @param  query       the query to find the document(s) to remove
     *  @param  limitToOne  limit the removal to a single document
     */
    DeferredRemove& remove(const std::string& collection, Variant::Value&& query, bool limitToOne = false);

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
    DeferredRemove& remove(const std::string& collection, const Variant::Value& query, bool limitToOne = false);

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
    DeferredCommand& runCommand(const std::string& database, const Variant::Value& query, const std::function<void(Variant::Value&& result)>& callback);

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
    DeferredCommand& runCommand(const std::string& database, Variant::Value&& query, const std::function<void(Variant::Value&& result)>& callback);
};

/**
 *  End namespace
 */
}}
