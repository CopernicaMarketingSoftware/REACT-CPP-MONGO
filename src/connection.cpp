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
namespace Mongo {

/**
 *  Establish a connection to a mongo daemon or mongos instance.
 *
 *  The hostname may be postfixed with a colon, followed by the port number
 *  to connect to. If no port number is given, the default port of 27017 is
 *  assumed instead.
 *
 *  @param  host        single server to connect to
 *  @param  callback    callback that will be executed when the connection is established or an error occured
 */
Connection::Connection(React::Loop *loop, const std::string& host, const std::function<void(bool connected, const std::string& error)>& callback) :
    _loop(loop),
    _worker(loop)
{
    // connect to mongo
    _worker.execute([this, host, callback]() {
        // try to establish a connection to mongo
        try
        {
            // connect throws an exception on failure
            _mongo.connect(host);

            // so if we get here we are connected
            callback(true, "");
        }
        catch (const mongo::DBException& exception)
        {
            // something went awry, notify the listener
            callback(false, exception.what());
        }
    });
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
        // execute the callback
        callback(!_mongo.isFailed());
    });
}

/**
 *  End namespace
 */
}
