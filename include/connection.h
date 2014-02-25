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
namespace Mongo {

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
     *  @param  callback    callback that will be executed when the connection is established or an error occured
     */
    Connection(React::Loop *loop, const std::string& host, const std::function<void(bool connected, const std::string& error)>& callback);

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
     *  @param  callback    the callback that will be called with the results
     */
    void query(const std::string& collection, const Variant::Value& query, std::function<void(Variant::Value&& result, const std::string& error)>& callback);
};

/**
 *  End namespace
 */
}
