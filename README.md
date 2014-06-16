High Volume Data Feed
=====================

The HVDF project is a framework for efficiently validating, storing, indexing, querying 
and purging time series data in MongoDB. It allows the user to define and configure channels
to perform various operations on the datastream as it moves through the system.
  
HVDF exposes a simple interface implemented using [dropwizard](http://www.dropwizard.io).

REST API
--------

    POST   /feed/{feed}/{channel}/data       push a sample (or batch) to a data channel on a given feed
    GET    /feed/{feed}/{channel}/data       query a data feed for a time range of data
    PUT    /feed/{feed}/{channel}/config     configure a channel
    DELETE /feed/{feed}/{channel}/data/{id}  delete a sample from a channel by id


Using the API
==================

Samples are pushed to and retrieved from channels. Each sample is a document with the following
recommended structure :

    {
        ts      : 100,     // 64 bit numeric timestamp
        source  : "s1",    // An opaque value that represents the source of the data
        data    : {x : 5}  // A document containing actual data
    }                  

In addition to the required timestamp and source, any other custom metadata may be added at the 
root level of the sample document.


Configure channels
------------------

    $ curl -X PUT "localhost:8080/feed/stocks/MSFT/config?value=%7Bcollection_period:200000%7D"

Pushing data to channels
------------------------

    // Post the sample {source:"reuters", ts:100, data:{price:1.25}} to stocks/MSFT channel
    $ curl -X POST "localhost:8080/feed/stocks/MSFT/data?sample=%7Bsource%3A%22reuters%22%2Cts%3A100%2Cdata%3A%7Bprice%3A1.25%7D%7D"
    [{ "$oid" : "0000000000643f6ebab4fe0d"}]
    
    // Post the sample {source:"reuters", ts:200, data:{price:10.25}} to stocks/MNGO channel
    $ curl -X POST "localhost:8080/feed/stocks/MNGO/data?sample=%7Bsource%3A%22reuters%22%2Cts%3A200%2Cdata%3A%7Bprice%3A10.25%7D%7D"
    [{ "$oid" : "000000c8a0ee16502b62ca3e"}]

    // Post the sample {source:"reuters", ts:300, data:{price:15.25}} to stocks/AAPL channel
    $ curl -X POST "localhost:8080/feed/stocks/AAPL/data?sample=%7Bsource%3A%22reuters%22%2Cts%3A300%2Cdata%3A%7Bprice%3A15.25%7D%7D"
    [{ "$oid" : "00000000012c3f6ebab4fe0e"}]

Sending batches of samples 
--------------------------

Samples can also be posted in batches. For example a series of three samples can be posted in a single request by
listing them in a top level array as follows :

    [    {source:"reuters", ts:100, data:{price:1.25}} ,
         {source:"reuters", ts:200, data:{price:1.27}} ,
         {source:"reuters", ts:300, data:{price:1.32}}
    ]

Posting samples always returns an array of ObjectID's, one for each sample successfully sent to the channel.


Query for a channel time range
--------------------------------

    // Source may be omitted (all sources), a specific sourceId, or an array of sourcesIds
    curl "localhost:8080/feed/stocks/MNGO/data?ts=500&range=400&limit=10&source=%22reuters%22%"
    [{"data":{"price":10.25},"date":200000,"source":"reuters","_id":"000000c8a0ee16502b62ca3e"}]


Channel Plugins
===============

Various aspects of how data is processed through a channel can controlled via channel plugins.
Some plugins affect the way data is physically stored (e.g. time slicing of collections) while
others can be installed to perform operations on samples before (interceptors) and after (listeners) 
they are persisted to the raw feed collection. 

All plugins have a similar format in the channel configuration. Each plugin instance will 
denote a "type" which can be a simplified name (e.g. "batching") for built in plugins or a 
full class name for user defined types. In addition, each plugin will have a "config" document
containing all configuration parameters that are specific to that plugin.

The individual configuration blocks for various built-in plugins are discussed below, however
these may be assembled together in various ways ([see example](src/test/resources/plugin_config/complex_config_example.json))
to create a set of complex channel configurations tailors to the needs of each data stream.



Time Slicing
------------

Time slicing is the process of organizing samples into time partitioned collections in MongoDB. There
are several performance advantages to doing this, especially when old data is being deleted periodically.
By default, time slicing is off and all raw data for a channel flows into a single collection, however
this can be changed by configuring the channel as follows.

    {
        "time_slicing" : 
        {
            "type"   : "periodic",
            "config" : { "period" : {"weeks" : 4} }
        }
    }

This configuration will arrange for a new collection to be used for each 4 week period of data. The 
"period" may be specified may be specified in years, weeks, hours, minutes, seconds, milliseconds or
any combination, for example 

        "config" : { "period" : {"days" : 1, "hours" : 12} }

Note that time slicing the channel does not affect queries, the channel will ensure that
queries spanning time ranges that are larger than a slice will operate across slices as necessary.

Interceptors
============

Interceptors are a chain of plugins that process samples in order before they persisted. 
Interceptors can be used for validation, augmentation and batching of samples.

Plugins are specified as part of the channel configuration, for example the following configuration
will install a single "SampleValidation" interceptor which will limit the value of a specific 
field to a maximum of 50.

    {
        "interceptors": 
        [
            {
                "type"   : "com.mongodb.hvdf.examples.SampleValidation",
                "config" : 
                {
                    "max_value" : 50
                }
            }
        ]
    }

Custom interceptors can be written by extending the com.mongodb.hvdf.channels.ChannelInterceptor
class. See the com.mongodb.hvdf.examples.SampleValidation class for an example.

Batching Samples
----------------

A popular approach to improving insert performance with MongoDB is to insert batches of
documents in a single database operation. As of MongoDB 2.6, there is a native api to
allow collection of a batch of operations on the client and execute it as a whole. 

While a HVDF client can send a batch of samples directly via a PUT operation, it is often
more convenient (especially in a multi-client scenario) to have the server group batches 
for a channel automatically across all clients.

The HVDF platform includes a built-in interceptor plugin for this purpose. It can be configured
into any channel interceptor chain to provide a batching service as follows :

    {
        "time_slicing" : 
        {
            "type"   : "periodic",
            "config" : { "period" : {"hours" : 12} }
        },
        "interceptors": 
        [
            {
                "type"   : "com.mongodb.hvdf.examples.SampleValidation",
                "config" : 
                {
                    "max_value" : 50
                }
            }
            {
                "type" : "batching",
                "config" : 
                {
                    "target_batch_size"  : 500,
                    "max_batch_age"      : 100,
                    "thread_count"       : 4,
                    "max_queued_batches" : 50
                }
            }
        ]
    }

In this example the channel will attempt to collect and process batches of 500 samples
at a time even if they arrive at the API individually. If there are not enough samples
arrived within 100 milliseconds to fill a batch, a partial batch will be processed. A 
pool of 4 threads will be allocated to process batches through to the database for 
this channel and the maximum number of queued batches is 50, after which backpressure
is applied to the inserting clients.

Note that the order of interceptors is important, in this example all incoming samples
will pass through the custom validation plugin first and may not even reach the batching
process. If it was preferable that validation occurred in batches, this order can simply
be reversed.

Retry 
-----

The `retry` interceptor can be added to the chain for handling and retrying upon insert 
errors. For example the following interceptor chain will both created batches and retry 
inserts when the database cannot be written to.

    "interceptors": 
    [
        {
            "type"   : "batching",
            "config" : 
            {
                "target_batch_size"  : 1000,
                "max_batch_age"      : 200,
                "thread_count"       : 2,
                "max_queued_batches" : 100
            }
        },
        {
            "type" : "retry",
            "config" : 
            {
                "max_retries" : 3,
                "retry_period" : {"seconds" : 5}
            }
        }
    ],


Tasks
=====

A channel may be configured to run periodic tasks to maintain or manipulate its data.
All tasks must specify a type, period and config and will be executed internally on
the given schedule. Tasks are useful for performing operations on complete time slices
of data after they have been written or for performing pre-emptive work on collections 
prior to data arriving for that time slice.

Indexing
--------

The built-in `ensure_indexes` task can be used to create indexes on time sliced collections 
as they are created. The task will periodically monitor the channel for new collections and 
create specified indexes. For example the following task block can be added to a channel's
configuration :

    "tasks": 
    [
        {
            "type"   : "ensure_indexes",
            "period" : {"seconds" : 3},
            "config" : 
            {
                "indexes": 
                [
                    {
                        "keys"    : {"data.v" : 1},
                        "options" : {"unique" : true},
                        "skips"   : 2
                    }
                ]
            }
        }
    ]

The `indexes` section specifies details of the indexes to be created. The `keys` and `options`
use the same format as the MongoDB [ensureIndex](http://docs.mongodb.org/manual/reference/method/db.collection.ensureIndex/#db.collection.ensureIndex)
collection method. In this example, we are adding an index on the field `v` which is nested inside
the top level `data` document.

The `skips` field is optional and it allows the user to specify that the latest (2 in this case) 
collections will not be indexed, allowing the index build to be deferred until the collections
are no longer being actively written to.

Capping Channel by Time
-----------------------

For some channels, it may be desirable to maintain a fixed width window of data as a rolling
series of time sliced collections. The built-in `limit_slices` task can be used in conjunction
with the `time_slicing` plugin to create such a channel setup. The following configuration 
creates a channel that maintains daily collections for a 30 day period :

    {
        "time_slicing" : 
        {
            "type"   : "periodic",
            "config" : { "period" : {"days" : 1} }
        },
        "tasks": 
        [
            {
                "type" : "limit_slices",
                "period" : {"seconds" : 3},
                "config" : 
                {
                    "by_count" : 30
                }
            }
        ]
    }



Running the service
===================

Downloading HVDF
----------------

- $ sudo yum install git
- $ git clone https://github.com/10gen-labs/hvdf.git

Compiling HVDF
--------------

- download and install Maven from http://maven.apache.org/download.cgi
- install Java: sudo yum install java-1.7.0-openjdk-devel
- $ cd hvdf
- $ mvn package -Dmaven.test.skip=true

Running HVDF
------------

- Copy sample-config.yml to config.yml and modify the configuration file as needed
- Run "java -jar ./target/hvdf-0.0.1-SNAPSHOT.jar server sample-config.yml". Use a Screen session if possible.
- Service is now running on port 8080


Configuration Options
=====================

    mongodb:
        default_database_uri : Each service that requires a database may be configured individually with a
                               URI which represents its specific connection. If a service is not configured
                               with a specific URI, this default is used.

    services:

        <common to all services>
            database_uri                  : override the default uri for this specific service
            database_name                 : used only if there is no database name specified by configured uri

        async_service:
            model: DefaultAsyncService    : implementation to use for Async Service (default: null (none))
            service_signature             : unique identifier for this service instance (generated by default)
            recovery_collection_name      : name of collection for storing asyncronous work (default: "async_recovery") 
            persist_rejected_tasks        : when tasks cannot be processed or queued, persist for later processing (default: true)
            processing_thread_pool_size   : size of thread pool for processing async tasks (default: 4)
            async_tasks_max_queue_size    : size of in memory task queue (default: 1000)
            recovery_poll_time            : poll time (ms) for finding persisted async tasks to process (default: 3000)
                                            use -1 to never process persistently queued tasks or recover failed ones
            failure_recovery_timeout      : time after which the async processor will declared a processing task to
                                            be hung/failed and attempt to reprocess it. Use -1 (default) to disable.
            max_task_failures             : maximum times a task can fail before recovery attempts stop (default: 3)

        channel_service:
            model: DefaultChannelService  : implementation to use for Channel Service (default: "DefaultChannelService")

