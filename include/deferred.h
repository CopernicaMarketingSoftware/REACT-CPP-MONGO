/**
 *  Deferred.h
 *
 *  Object used for registering callbacks to be executed
 *  at a later point in time.
 *
 *  @copyright 2014 Copernica BV
 */

/**
 *  Set up namespace
 */
namespace React { namespace Mongo {

// forward declaration
class Connection;

/**
 *  Deferred class
 */
template <typename... Arguments>
class Deferred
{
private:
    /**
     *  Callback to execute on success
     */
    std::function<void(Arguments ...parameters)> _successCallback;

    /**
     *  Callback to execute on failure
     */
    std::function<void(const char *error)> _failureCallback;

    /**
     *  Callback to execute on completion
     */
    std::function<void()> _completeCallback;

    /**
     *  Do we have to go through the trouble of checking for
     *  success or error? If not, we can save a round-trip
     *  to the server.
     */
    bool requireStatus()
    {
        // status is only relevant if a success- or failure callback
        // has been installed, the complete callback is irrelevant.
        return _successCallback || _failureCallback;
    }

    /**
     *  Signal that the command finished successfully
     *
     *  @param  parameters  relevant parameters
     */
    void success(Arguments ...parameters)
    {
        // execute the callbacks
        if (_successCallback)   _successCallback(std::forward<Arguments>(parameters)...);
        if (_completeCallback)  _completeCallback();
    }

    /**
     *  Signal that the operation resulted in failure
     *
     *  @param  error       description of the failure reason
     */
    void failure(const char *error)
    {
        // execute the callbacks
        if (_failureCallback)   _failureCallback(error);
        if (_completeCallback)  _completeCallback();
    }

    /**
     *  Signal that the operation completed, but without
     *  indicating whether it resulted in success
     */
    void complete()
    {
        // execute the callback
        if (_completeCallback) _completeCallback();
    }
public:
    /**
     *  Constructor
     */
    Deferred() {}

    /**
     *  We cannot be copied
     */
    Deferred(const Deferred& that) = delete;

    /**
     *  Nor can we be moved
     */
    Deferred(Deferred&& that) = delete;

    /**
     *  Register a callback to be executed when the operation succeeds
     *
     *  @param  callback    the callback to execute on success
     */
    Deferred& onSuccess(const std::function<void(Arguments ...parameters)>& callback)
    {
        // store callback
        _successCallback = callback;
        return *this;
    }

    /**
     *  Register a callback to be executed when the operation fails
     *
     *  @param  callback    the callback to execute on failure
     */
    Deferred& onFailure(const std::function<void(const char *error)>& callback)
    {
        // store callback
        _failureCallback = callback;
        return *this;
    }

    /**
     *  Register a callback to be executed when the operation is finished,
     *  whether successful or not.
     *
     *  @param  callback    the callback to execute when the operation completes
     */
    Deferred& onComplete(const std::function<void()>& callback)
    {
        // store callback
        _completeCallback = callback;
        return *this;
    }

    // the connection class may call private methods
    friend class Connection;
};

/**
 *  Deferred type for queries
 *
 *  Queries will get the result in their onSuccess
 *  method as an rvalue reference
 */
using DeferredQuery = Deferred<Variant::Value&&>;

/**
 *  Deferred type for inserts, updates and removes
 *
 *  Their callbacks don't get any extra parameters
 */
using DeferredInsert  = Deferred<>;
using DeferredUpdate  = Deferred<>;
using DeferredRemove  = Deferred<>;

/**
 *  Deferred type for runCommand
 *
 *  Commands will get the result in their onSuccess
 *  method as an rvalue reference
 */
using DeferredCommand = Deferred<Variant::Value&&>;

/**
 *  End namespace
 */
}}
