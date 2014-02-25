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
            _master.execute([callback]() {
                callback(true, "");
            });
        }
        catch (const mongo::DBException& exception)
        {
            // something went awry, notify the listener
            _master.execute([callback, exception]() {
                callback(false, exception.what());
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
void Connection::query(const std::string& collection, const Variant::Value& query, const std::function<void(Variant::Value&& result, const std::string& error)>& callback)
{
    // run the query in the worker
    _worker.execute([this, collection, callback, query]() {
        // execute query
        auto cursor = _mongo.query(collection, convert(query));

        // build the result value
        std::vector<Variant::Value> result;

        // process all results
        while (cursor->more()) result.push_back(convert(cursor->next()));

        // we now have all results, execute callback in master thread
        _master.execute([result, callback]() {
            callback(result, "");
        });
    });
}

/**
 *  End namespace
 */
}
