---
title: "Subset Index"
permalink: /docs/subsetindex-extension/
excerpt: "Subset Index"
last_modified_at: 2020-12-08
toc: true
---

## A Secondary Index With Nonunique Keys
The FASTER SubsetIndex is based upon the PSFs (Predicate Subset Functions) defined in the [FishStore](https://github.com/microsoft/FishStore) prototype; they allow defining predicates that records will match, possibly non-uniquely, for secondary indexing. The SubsetIndex is designed to be used by any data provider. Currently there is only an implementation using FasterKV as the provider, so this document will mostly focus on their use as a secondary index (implemented using "secondary FasterKVs") for a primary FasterKV store, with occasional commentary on other possible stores.

Recall that FasterKV is essentially a hash table; as such, it has a single primary key for a given record, and there are zero or one records available for a given key.
- An Upsert (insert or blind update) will replace an identical key, or insert a new record if an exact key match is not found
- An RMW (Read-Modify-Write) will find an exact key match and update the record, or insert a new record if an identical key match is not found
- A Read will find either a single record matching the key, or no records

The FasterKV Key and Value may be [blittable](https://docs.microsoft.com/en-us/dotnet/framework/interop/blittable-and-non-blittable-types) fixed-length, blittable variable length, or .NET objects.

The FASTER SubsetIndex implements a secondary index by allowing the user to register one or more Predicates that return an alternate key for the record. For example, a record might be { Id: 42, Species: "cat" }. The "primary index" is the key inserted into the primary FasterKV; in this example it is Id, and there will be only one record with an Id of 42. A SubsetIndex might be created for such records by defining a Predicate that returns the Species property.

### Predicates
A Predicate is a function (in C# terms, a delegate, or a lambda such as "v => v.Species;") that operates on the Value of a record and returns a Key that is distinct from the Key in the primary FasterKV instance.

The return from a Predicate is nullable, reflecting the "predicate" and "subset" terminology, which means that the record may or may not "match" the Predicate. For example, a record with no pets would return null, and the record would not be stored for that Predicate. On the other hand, dogs and cats are quite common. This design allows zero, one, or more records to be stored for a single Predicate key, entirely depending on the Predicate definition.

### Predicate Groups
Predicates are registered with the SubsetIndex in Groups. For space efficiency, Predicates should be organized into groups such that it is expected that a record will match all or none of the Predicates in that Group. That is, if a record results in a non-null key for one Predicate in the Group, it results in a non-null key for all Predicates in the Group; and if a record results in a null key for one Predicate in the Group, it results in a null key for all Predicates in the Group).

### Assemblies

As shown in this diagram, there are 3 assemblies involved when defining a SubsetIndex in a FasterKV app:
![Assembly Overview](/FASTER/assets/subsetindex/assembly-overview.png "Assembly Overview")

Note the direction of arrows in the diagram. For a FasterKV app that uses a SubsetIndex, the app talks to the `FASTER.indexes.SubsetIndex` assembly, and indirectly to the `FASTER.libraries.SubsetIndex` assembly because it holds data members of its types, in particular `IPredicate` which is used for queries. `FASTER.indexes.SubsetIndex` then talks to `FASTER.core` and `FASTER.libraries.SubsetIndex`. However, `FASTER.core` is completely unaware of the SubsetIndex.

Similarly, a non-FasterKV app will use `FASTER.libraries.SubsetIndex` directly, and no other `FASTER` assemblies (except indirectly, because FasterKV instances are used to implement the SubsetIndex).

#### `FASTER.core`
This is the core FASTER assembly, and has no code that is specific to the SubsetIndex (some enhancements, such as `ReadAtAddress` and `IAdvancedFunctions`, were motivated initially by the SubsetIndex, but are core FASTER features usable by any FASTER app). An app that uses a SubsetIndex will mostly still make the same FASTER calls, except when defining or querying the SubsetIndex.

#### `FASTER.indexes.SubsetIndex`
This can be thought of as an extension to core FASTER; it uses extension functions to provide the SubsetIndex functionality (this is described further below). It has no storage itself; rather, it is a redirection and coordination layer between `FASTER.core` and `FASTER.libraries.SubsetIndex`. It provides extension wrappers for:
  - Registering Predicate groups
  - Obtaining SubsetIndex-enabled new sessions (other sessions are not allowed)
  - Providing wrappers for some FasterKV operations such as `Flush`, `Checkpoint`, and `Recover` to coordinate with the same operations on the SubsetIndex
  - Providing a session wrapper that:
    - Exposes all the usual `ClientSession` operations such as `Read`, `Upsert`, `RMW`, and `Delete`, wrapping them to keep the SubsetIndex updated.
    - Adds methods for querying Predicates singly or in groups

#### `FASTER.libraries.SubsetIndex`
This is the actual implementation of the SubsetIndex. Each Group has its own FasterKV instance (Using different Keys and Values than 'FASTER.core`), but these are an implementation detail and are not seen by the client of this assembly other than the Predicate registration options that are passed through to the FasterKV constructor. For an app that uses the SubsetIndex over a different store than FasterKV, this is the only FASTER assembly that app will use.

### Limitations
The current implementation of PSFs has some limitations.

#### No Range Indexes
Because PSFs use hash indexes (and conceptually store multiple records for a given key as that key's collision chain), we have only equality comparisons, not ranges. This can be worked around in some cases by specifying "binned" keys; for example, a date range can be represented as bins of one minute each. In this case, the query will pass an enumeration of keys and all records for all keys will be queried; the caller must post-process each record to ensure it is within the desired range. The caller must decide the bin size, trading off the inefficiency of multiple key enumerations with the inefficiency of returning unwanted values (including the lookuup of the logical address in the primary FasterKV).

#### Fixed-Length `TPKey`
PSFs currently use fixed-length keys; the `TPKey` type returned by a PSF execution has a type constraint of being `struct`. It must be a blittable type; the PSF API does not provide for `IVariableLengthStruct` or `SerializerSettings`, nor does it accept strings as keys. Rather than passing a string, the caller must pass some sort of string identifier, such as a hash (but do not use string.GetHashCode() for this, because it is AppDomain-specific (and also dependent on .NET version)). In this case, the hashcode becomes a "bin" of all strings (or string prefixes) corresponding to that hash code.

## FasterKV Client Public API
This section discusses the SubsetIndex public API. As mentioned above, there are two levels: The interface for FASTER clients to add SubsetIndexing to their apps, and the interface for non-FASTER clients.

### FasterKV Client Changes to Use the SubsetIndex
There are very few changes required to enable the SubsetIndex in a FasterKV app.

#### Creating a SubsetIndex-enabled FasterKV
A FASTER app that uses a SubsetIndex must not instantiate a `FasterKV<K, V>` directly; instead must obtain a SubsetIndex-enabled subclass using `SubsetIndexExtensions.NewFasterKV<K, V>(...)`. This returns an instance of `FasterKVForSI<K, V>`, which is an internal subclass of `FasterKV<K, V>`. Thus it is identical for non-SubsetIndex operations; the application talks to a `FasterKV<K, V>` instance as usual.

#### Creating a SubsetIndex-enabled Session
A FASTER app that uses a SubsetIndex must not use the `FasterKV<K, V>` methods for obtaining a session; instead it must call the `SubsetIndexExtensions` method `.ForSI` on the `FasterKVForSI<K, V>` to obtain a `ClientSessionForSI<I, O, C, F>` object. This does not inherit from `(Advanced)ClientSession` but presents an identical interface, including Read, Upsert, RMW, and Delete operations; internally it manages these calls to update both the primary `FasterKV<K, V>` and the SubsetIndex. 

The SubsetIndex API to create a new session parallels that of the normal FasterKV API, but due to C# overloading rules, it is not possible to overload the `For` methods on return type. Therefore the name `ForSI` is used for the corresponding method that returns a new `ClientSessionBuilderForSI`; however, the usual NewSession naming is used on this object to obtain a `ClientSessionForSI`. An exception is thrown if `.For` is called on a `FasterKVForSI<K, V>` instance. This is one of the very few API differences in non-SubsetIndex operations.

### Example Apps

FASTER provides two levels of example apps: `cs/samples` provides simple examples of basic functionality, and `/cs/playground` provides a more comprehensive exercise of functionality.

For the SubsetIndex examples, an easy way to see the differences from a non-SubsetIndex FASTER app is to compare the sample apps in the `cs/samples/SubsetIndex` with other samples.

Following are the SubsetIndex-specific examples.
- The[ __`BasicPredicate`__](https://github.com/TedHartMS/FASTER/tree/PSF-dll/cs/samples/SubsetIndex/BasicPredicate) Sample App: This illustrates the simplest form of defining and querying a single Predicate, with the Predicate using a lambda that simply returns a property of the object rather than defining a key struct.

- The [__`SingleGroup`__](https://github.com/TedHartMS/FASTER/tree/PSF-dll/cs/samples/SubsetIndex/SingleGroup) Sample App: This illustrates defining two Predicates in a single group, using a key struct that knows which property of the value it should use as the secondary key. It uses the synchronous `Query` method and illustrates simple boolean AND/OR operations.

- The [__`MultiGroupAsync`__](https://github.com/TedHartMS/FASTER/tree/PSF-dll/cs/samples/SubsetIndex/MultiGroupAsync) Sample App: This illustrates defining two Predicates each in their own group, using two separate key structs, each dedicated to a single property of the value to be used as the secondary key. It uses the asynchronous `QueryAsync` method and illustrates simple boolean AND/OR operations.

- The [__`SubsetIndex`__](https://github.com/TedHartMS/FASTER/tree/PSF-dll/cs/playground/SubsetIndex) Playground App: This app demonstrates much more comprehensive (and complex) registration and querying of the SubsetIndex, using all overloads of the `Query` API.

### Registering the SubsetIndex
First obtain a SubsetIndex-enabled `FasterKV<K, V>` as described [above](#creating-a-subsetindex-enabled-fasterkv).

This `FasterKV<K, V>` instance enables `SubsetIndexExtensions.Register(...)` for registering the Predicate groups. Each `Register` call is forwarded to the [`SubsetIndex`](#public-subsetindex) implementation, which creates a [`Group`](#internal-group) internally; this `Group` contains its own `FasterKV` instance, using the Predicate Key type and a RecordId that is the logical address of the record in the Primary `FasterKV` for its Value. All non-null keys returned from the Predicate are linked in chains within that FasterKV instance. 

There are a number of overloads of `Register` that take various forms of predicate specification:
- Simple lambdas that merely return a property of the Value, or complex lambdas that calculate and return a Key type. Lambdas are wrapped in an instance of [`FasterKVPredicateDefinition`](#public-fasterkvpredicatedefinition), wrapping the lambda in its delegate.
- A [`FasterKVPredicateDefinition`](#public-fasterkvpredicatedefinition) instance with the delegate already created.

All `Predicate`s in a `Group` have the same Key type and same form for specifying the predicate logic (lambda vs. [`FasterKVPredicateDefinition`](#public-fasterkvpredicatedefinition)). Creating Predicates in groups has the following advantages:
- A single hashtable can be used for all Predicates in the group, using the ordinal of the Predicate as part of the hashing logic. This can save space.
- Predicates should be registered in groups where it is expected that a record will match all or none of the Predicates (that is, if a record results in a non-null key for one Predicate in the group, it results in a non-null key for all Predicates in the group, and if a record results in a null key for one Predicate in the group, it results in a null key for all Predicates in the group). This saves some overhead in processing variable-length composite keys in the secondary FasterKV; this [KeyPointer](#the-structure-of-a-record-in-a-groups-fasterkv) structure is described more fully below.
- All Predicates in a group have the same `TPKey` type, but different groups can have different `TPKey` types.

`Groups` membership is immutable; they are defined on a single `Register` call, and `Predicates` cannot be added to or removed from a `Group`. Similarly, there is no method to remove a `Group`. A `Group` can in effect be dropped simply by not re-registering it when the FasterKV is recovered.

#### How Predicates are Called
For `VarLen` (variable-length blittable) types and large fixed-length blittable types, it is not feasible to pass the entire object; this is why "ref Key" and "ref Value" are prevalent in the `FasterKV<K, V>` API. Similarly, `Predicate` execution must take a "ref Key" and "ref Value". However, in many cases (such as [pending operations](#data-update-operations)), the SubsetIndex must store the Key and Value pass on the `FasterKV<K, V>` call, and eventually pass them to the `Predicate` call.

Because of this, the SubsetIndex extension has a `FasterKVProviderData` which holds the Key and Value until it is ready to be executed. This execution happens in each [`Group`](#internal-group) when [`ClientSessionForSI`](#public-clientsessionforsi) calls [`ClientSessionSI`](#public-clientsessionsi)'s update methods.

### Updating Data in the SubsetIndex
A FASTER app that uses a SubsetIndex must not use the `FasterKV<K, V>` methods for obtaining a session; instead it must call the `SubsetIndexExtensions` method `.ForSI` on the `FasterKVForSI<K, V>` to obtain a [`ClientSessionForSI<I, O, C, F>`](#public-clientsessionforsi) object. This presents an identical interface to the `(Advanced)ClientSession` for Upsert, RMW, or Delete operations, but internally it passes these calls through to the same methods on its contained `(Advanced)ClientSession` instance, then calls the Upsert, Update, or Delete methods on the [`SubsetIndex`](#public-subsetindex) to update the index.

When a record is inserted into the primary FasterKV (via Upsert or RMW), it is inserted at a unique "logical address" (essentially a page/offset combination). Predicates are used to implement secondary indexes in Faster by allowing the user to register a delegate that returns an alternate key for the record; then the logical address of the record in the primary FasterKV is the value that is inserted into the secondary FasterKV instance using the alternate key. This value is referred to as a RecordId; note that this is *not* the actual record value in the primary FasterKV, only its address. The type is termed `TRecordId` and for the primary FasterKV it is a long integer; other data providers may use a different type, as long as it is blittable.

The distinction between the Key and Value defined for the primary FasterKV and the Key and RecordId (Value) in the secondary FasterKV is critical; the secondary FasterKV has no idea of the primary datastore's Value type, nor does it know if the primary datastore even has a Key.

Unlike the primary FasterKV's keys, the Predicate keys may chain multiple records. (The primary FasterKV can support this by returning false from `InPlaceUpdater` and `ConcurrentWriter` methods on the `I(Advanced)Functions` implementations, then using the session's `ReadAtAddress` variants, but the SubsetIndex suports this automatically). To query a Predicate, the user passes a value for the alternate key; all RecordIds that were inserted with that key are returned, and then `ReadAtAddress` is called on the primary FasterKV instance to retrieve the actual records for those RecordIds. Whereas the primary FasterKV returns only a single record (if found) via the `I(Advanced)Functions` implementation on `Read` operations, queries on the SubsetIndex return an `IEnumerable<TRecordId>` or `IAsyncEnumerable<TRecordId>`.

### Querying the SubsetIndex
The `ClientSessionForSI<I, O, C, F>` instance contains several overloads of `Query` and corresponding `QueryAsync` methods, taking various combinations of [`IPredicate`](https://github.com/TedHartMS/FASTER/tree/PSF-dll/cs/src/libraries/SubsetIndex/IPredicate.cs), `TPKey` types and individual keys, and, for queries across multiple `IPredicates`, `matchPredicate`s (lambdas that are called for each RecordId returned from the Predicates' chains, take boolean parameters that indicate which Predicate(s) the RecordId is present in, and return a boolean indicating whether that RecordId is to be included in the query result).

The simplest query takes only a single [`IPredicate`](https://github.com/TedHartMS/FASTER/tree/PSF-dll/cs/src/libraries/SubsetIndex/IPredicate.cs) and `TPKey` instance and returns all records that match that key for that Predicate. More complicated forms of `Query` allow specifying multiple Predicates, multiple keys per Predicate, and multiple `TPKey` types (each possibly with multiple Predicates, each possibly with multiple keys).

Because the SubsetIndex stores `TRecordId`s, the SubsetIndex client (that is, the data provider, such as a primary FasterKV) must wrap the `Query` call with its own translation layer to map the `TRecordId` to the actual data record; see `CreateProviderData` in [`ClientSessionForSI`](#public-clientsessionforsi).

#### Liveness Checking
Some `FasterKV<K, V>` update operations do not guarantee that the old values are available:
- `Upsert` is designed for speed, and if the record is not in the mutable region, it inserts a new record. If the data is not in memory (is below HeadAddress), it does not issue an IO to retrieve the old value from disk.
- Similarly, `Delete` does not issue an IO to retrieve the old value; it inserts a new record with the Tombstone bit set.
  - An `RMW` does issue an IO to obtain the old record.

For an `Upsert` or `Delete` where the record was in the mutable region, the SubsetIndex is able to mark the old record as "deleted", and it is not returned by the query. However, if the record is not retrieved from disk, the SubsetIndex could not even know if it was for the current Key, or was a colliding Key.

Therefore, it is necessary that for each record returned by a SubsetIndex query, we check for "liveness":
- The `ClientSessionForSI` contains a separate session whose Functions is an instance of `LivenessFunctions` (also implementing `IAdvancedFunctions`. Its purpose is to support issuing a ReadAtAddress on the primary `FasterKV<K, V>` using the returned `TRecordId` as its address, and store the Key.
- Once the Key for the `TRecordId`'s address has been obtained, a Read is done on that Key to verify that the address returned is the same as the `TRecordId`. If it is not, then the record is no longer live, and it is not returned from the query.

## Non-FasterKV Client Public API
As discussed above, the `FASTER.libraries.SubsetIndex` API is intended to be used by any data provider needing a hash-based index that is capable of storing a `TRecordId` from which the provider can extract its full record. However, SubsetIndex update operations must be able to execute the Predicate, which requires knowledge of the provider's Key and Value types. Therefore, [`SubsetIndex`](#public-subsetindex) has two generic types, both of which are opaque to the SubsetIndex:
- `TProviderData`, which is the data passed to PSF execution (the PSF must, of course, know how to operate on the provider data and form a `TPKey` key from it)
- `TRecordId`, which is the record identifier stored as the Value in the secondary FasterKV.

The `TRecordId` has a type constraint of being `struct`; it must be blittable.

### Re-Registering the SubsetIndex on `Recover`
TODO: This section is not current; code TBD
[IFasterKV](../Interfaces/IFasterKV.cs) provides a `GetRegisteredPSFNames` method that returns the names of all PSFs that were registered. Another provider would have to expose similar functionality. At `Restore` time, before any operations are done on the Primary FasterKV, the aplication must call `RegisterPSF` on those names for those groups; it *must not* change a group definition by adding, removing, or supplying a different name or functionality (lambda or definition) for a PSF in the group; doing so will break access to existing records in the group.

If an application creates a new version of a PSF, it should encode the version information into the PSF's name, e.g. "Dog v1.1". The application must keep track internally of all PSF names and functionality (lambda or definition) for any groups it has created.

Dropping a group is done by omitting it from the `RegisterPSF` calls done at `Restore` time. This is the only supported versioning mechanism: "drop" a group (by not registering it) and then create a new group with the updated definitions (and possibly changed PSF membership).

## Faster SubsetIndex Technical Details
This section describes implementation details of the FASTER SubsetIndex.

### `FASTER.indexes.SubsetIndex` Implementation
This assembly is a mostly-thin layer over `FasterKV` that redirects between normal FasterKV operations and the SubsetIndex. This assembly uses the naming convention that a public subclass, wrapper class, or function is named the same as the corresponding FasterKV element, with the suffix `ForSI`.

A basic principle of this implementation is that it minimizes differences from the core FasterKV API (`ForSI` being one of the few exceptions). This is done by a combination of subclasses, wrapper classes, and C# extension functions.

#### Internal: `FasterKVForSI`
This is a subclass of `FasterKV`. Rather than exposing this class externally, it is an internal subclass; the application still talks to a `FasterKV` instance.

As a subclass, it overrides operations such as growth, checkpointing, and recovery to ensure the corresponding SubsetIndex methods are called as well.

Other `FasterKVForSI` methods are provided via C# extension methods.

##### Public: `SubsetIndexExtensions`
This is a static C# extension class whose methods take a `FasterKV` instance as the `this` parameter. Internally it casts this to `FasterKVForSI` to call internal methods to implement SubsetIndex capabilities:
- [Predicate registration](#registering-the-subsetindex)
- New Session operations

#### Public: `ClientSessionForSI`
`ClientSessionForSI` is a wrapper class (not a subclass) that implements all `ClientSession` methods, including operations such as Read, Upsert, RMW, and Delete, which are intercepted to update the SubsetIndex after the operation completes.

`ClientSessionForSI` contains an internal `AdvancedClientSession` on the primary FasterKV; its Functions instance is an instance of the [`IndexingFunctions`](#internal-indexingfunctions) class, which implements `IAdvancedFunctions`. [`IndexingFunctions`](#internal-indexingfunctions) is the layer that manages data storage between the core FasterKV and the SubsetIndex, as well as containing the client Functions instance (wrapped in `BasicFunctionsWrapper` if it does not implement `IAdvancedFunctions`).

`ClientSessionForSI` also holds an instance of `ClientSessionSI`, which is a "session" on the [`SubsetIndex`](#public-subsetindex) top-level object in the [SubsetIndex Implementation](#fasterlibrariessubsetindex-implementation) hierarchy. This allows it to function as a "super-session", coordinating both the FasterKV session and the SubsetIndex session, so operations such as pending RMWs are consistent between the FasterKV and SubsetIndex.

#### Data Update Operations
The flow of operations through the `ClientSessionForSI` during a non-pending data-update operation is:
- Call the corresponding method on the contained client session (on the primary FasterKV), which:
  - Calls methods on the [`IndexingFunctions`](#internal-indexingfunctions), which:
    - Call the methods on the `IAdvancedFunctions` passed in from the user, or on the `BasicFunctionsWrapper` around the `IFunctions` passed in from the user.
    - Store the data and other information related to the operation. Normally this is stored in a [`ChangeTracker`](#public-changetracker), but for Upsert there is a fast path that does not do heap allocations.
- Call methods on the [`SubsetIndex`](#public-subsetindex) instance to update the index.

In the event of pending data-update operations, the data is stored in a [`ChangeTracker`](#public-changetracker) instance during the completion callback, which [`IndexingFunctions`](#internal-indexingfunctions) stores in a queue. When the `ClientSessionForSI`'s `CompletePending` method is called, all enqueued [`ChangeTracker`](#public-changetracker) instances are retrieved and sent to the [`SubsetIndex`](#public-subsetindex) instance to update the index.

#### Public: `FasterKVPredicateDefinition`
Each [`SubsetIndex`](#public-subsetindex) client will have its own implementation of `IPredicateDefinition` to create the `Predicate` definition. For `FasterKV<K, V>`, that is `FasterKVPredicateDefinition`. This is one of the very few places where both the primary `FasterKV<K, V>`'s Key and the `Predicate`'s Key types are present.

`FasterKVPredicateDefinition` carries the predicate to be executed, and has methods to perform that execution on the "provider data" (the stored data from the primary `FasterKV<K, V>`).

##### Public: `FasterKVProviderData`
Each [`SubsetIndex`](#public-subsetindex) client will have its own data provider specification. For `FasterKV<K, V>`, that is `FasterKVProviderData`. This carries the data from the primary `FasterKV<K, V>` operation. It uses the primary `FasterKV<K, V>`'s Key and Value types; it is unaware of the `Predicate`'s Key type.

### `FASTER.libraries.SubsetIndex` Implementation
This assembly is the actual implementation of the SubsetIndex. This assembly uses the naming convention that a public subclass, wrapper class, or function is named the same as the corresponding FasterKV element, with the suffix `SI`.

#### Public: `SubsetIndex`
This is the highest-level class in the hierarchy. It is the central point from which all group operations are initiated, both data updates and queries, and is the class that is instantiated by the caller to register `Predicate`s and create [`ClientSessionSI`](#public-clientsessionsi)s.

#### Public: `ClientSessionSI`
Similar to a `ClientSession` on a `FasterKV<K, V>`, the `ClientSessionSI` is a session on the SubsetIndex. This enables clean coordination between FasterKV and its SubsetIndex, in particular for pending operations; essentially, the `ClientSessionSI` is an extension of the [`ClientSessionForSI`](#public-clientsessionforsi) into the [`SubsetIndex`](#public-subsetindex).

The `ClientSessionSI` holds a FasterKV session for each `Group`'s `FasterKV<K, V` instance. These provide the "endpoints" for [`ClientSessionForSI`](#public-clientsessionforsi) coordination of sessions with the primary `FasterKV<K, V>`.

The `ClientSessionSI` includes methods for both data update operations and `Predicate` `Query` operations. It is a very thin wrapper for these, handing them off to [`SubsetIndex`](#public-subsetindex) with itself as the FasterKV session source.

##### Data Update Flow
When the `ClientSessionSI` receives a data update request, it calls appropriate methods on the [`SubsetIndex`](#public-subsetindex), which coordinates the operation across groups. Taking Upsert as an example:
- `ClientSessionSI` calls the corresponding update method on [`SubsetIndex`](#public-subsetindex), passing itself as the source for the internal FasterKV `AdvancedClientSession`.
- [`SubsetIndex`](#public-subsetindex) iterates its groups and for each group:
  - Obtains the `AdvancedClientSession` for that `Group`'s FasterKV from the `ClientSessionSI`.
  - Calls an update method on the `Group` (for Upsert, it is `ExecuteAndStore`).
  - The `Group`:
    - Executes the `Predicate` using the passed data from the primary `FasterKV<K, V>`.
    - Determines what update operation must be done on the `AdvancedClientSession` for its contained `FasterKV<K, V>`.
    - Calls that update method.

##### Query Flow
Queries are done at a per-`Predicate` level, again coordinated by the [`SubsetIndex`](#public-subsetindex). Similarly to data update operations, when the `ClientSessionSI` receives a query request, it calls appropriate methods on the [`SubsetIndex`](#public-subsetindex), which coordinates the operation across `Predicates`.

Unlike `FasterKV<K, V>`'s Read operations, `Query` results are returned via `IEnumerable<>` or `IAsyncEnumerable<>`.
The flow of a query is:
- `ClientSessionSI` calls the corresponding query method on [`SubsetIndex`](#public-subsetindex), passing itself as the source for the internal FasterKV `AdvancedClientSession`.
- [`SubsetIndex`](#public-subsetindex) opens an enumerable over each `Predicate` in the query. 
  - The `Predicate` calls back to its `Group` to execute this query, passing the `AdvancedClientSession` for that group (obtained from the `ClientSessionSI`).
- [`SubsetIndex`](#public-subsetindex) then passes these to a `QueryRecordIterator` instance, which coordinates the sorting and merging of the `TRecordId` streams, and finally issues the boolean lambda on each `TRecordId`.

#### Internal: `Group`
A `Group` maintain the secondary `FasterKV<K, V>` that stores the index.

For a data update operation, it executes the `Predicate`, creates a `CompositeKey` of the Key data returned by the `Predicate`(s), and calls the appropriate update operation in the `FasterKV`.

For a query operation, it issues the Read operation and traverses backward along the returned `KeyPointer` previous addresses.

##### The Structure of a Record in a `Group`'s FasterKV
The records in a `Group`'s FasterKV instance have the following structure:
![Record Structure](/FASTER/assets/subsetindex/record-structure.png "Record Structure")
- The usual RecordInfo header. Note that its `PreviousAddress` is not used for `Predicate` key chaining.
- A `CompositeKey` that consists of one or more `KeyPointer` structures (one for each `Predicate` in the `Group`).
  - Each `KeyPointer` contains:
    - The 8-byte address of the `KeyPointer` in the previous record in the chain for this Predicate Key's value. Two things are different here than from the usual FasterKV record:
      - The `RecordInfo`'s `PreviousAddress` is not used; chain traversal is done via a `KeyPointer`'s `PreviousAddress`.
      - The address does *not* point to the start of the previous `RecordInfo`; instead, it points to the start of the previous `KeyPointer`.
    - A two-byte offset to the start of the Key portion of the record.
    - A one-byte ordinal, which is the index of this `Predicate` in its containing `Group`'s list of `Predicates`. 
      - There is an enforced maximum of 255 `Predicates` per `Group`.
      - The size of the `CompositeKey` is fixed; all `Predicates` (or their are `KeyPointers`) are always in the list. (I.e., if there are 3 `Predicates` in a `Group`, then there will always be 3 `KeyPointers` in this list.)
      - A one-byte Flags field
        - One bit is for a null indicator
        - Other bits are reserved for a possible future enhancement to update records in-place.
      - The Key value. If this is 4 bytes it fits within the 8-byte alignment of the basic `KeyPointer` record.
- The record Value, which is a `TRecordId`.

![KeyPointer Chains](/FASTER/assets/subsetindex/keypointer-chains.png "KeyPointer Chains")

#### Internal: `IndexingFunctions`
`IndexingFunctions` are the `IAdvancedFunctions` implementation held by the [`ClientSessionForSI`](#public-clientsessionforsi) opened on the primary `FasterKV<K, V>`.

Its responsibility is:
- Hold onto the users's `IAdvancedFunctions<>` object (or a `BasicFunctionsWrapper` around the user's `IFunctions<>` object), and call this object's callback methods when an `IndexingFunctions` callback is called by the primary `FasterKV<K, V>`.
- Hold onto data sent to the callbacks, so they can later be sent to the [`SubsetIndex`](#public-subsetindex).
  - For Upserts, a "fast path" is employed where no heap allocations are done. In this case, `IndexingFunctions` simply holds the logical address passed to the `SingleWriter` callback.
    - When the Upsert is complete, [`ClientSessionForSI`](#public-clientsessionforsi) will call `Upsert` on the [`SubsetIndex`](#public-subsetindex), which calls `ExecuteAndStore` with this logicalAddress, bypassing the allocation of a [`ChangeTracker`](#public-changetracker).
  - For other operations, `IndexingFunctions` creates and holds a [`ChangeTracker`](#public-changetracker).
    - If the operation goes pending, then in the `*CompletionCallback` callback, `IndexingFunctions` places its [`ChangeTracker`](#public-changetracker) into a queue. In this way, multiple pending operations can be done.
      - Because there a `IndexingFunctions` instance is created for each [`ClientSessionForSI`](#public-clientsessionforsi), and because sessions are single-threaded, so the use of instance data in the `IndexingFunctions` is safe.
      - When one of the `CompletePending` calls is made on the [`ClientSessionForSI`](#public-clientsessionforsi), it will drain the [`ChangeTracker`](#public-changetracker) queue in `IndexingFunctions` and pass each [`ChangeTracker`](#public-changetracker) to the [`SubsetIndex`](#public-subsetindex) to be processed.

#### Public: `ChangeTracker`
The `ChangeTracker` tracks changes data changes from the primary `FasterKV<K, V>` across all groups. It carries the data copied from the primary `FasterKV<K, V>` operation for application across groups. It also carries an indicator of which operation was done on the primary `FasterKV<K, V>`, as well as the Key data from `Predicate` execution.

When a [`Group`](#internal-group) performs a data update operation, it executes its `Predicate`s on the `ChangeTracker`'s data to obtain the new keys and determines whether any Key data has changed for any `Predicate` (for an RMW, some `Predicate`'s may see changes while others do not; if no `Predicate` in the group reports a change, no index update is needed).

The `ChangeTracker` carries both "BeforeData" (before an update was applied) and "AfterData" (after the update was applied, for applicable operations). Normally, the `ChangeTracker` carries only the data until the primary `FasterKV<K, V>` operation is complete. However, if the Value type in the primary `FasterKV` has objects, then an in-place RMW to the data in that object will also affect BeforeData, and thus "no changes" would erroneously be reported. Therefore, in this case the `Predicate`s are executed and their "Before" Key data stored *during the `FasterKV<K, V>` operation*, because that is the only time the Before data is available. This is done during primary `FasterKV<K, V>` epoch protection so should be as fast as possible. This is the only time [`SubsetIndex`](#public-subsetindex) operations are done during epoch protection of the primary `FasterKV<K, V>`.

## Future Items
- Add an "IPUCache". There are several places in the code that have a TODOcache label marking this, as well as some of the `KeyPointer` flags. TODO fill in details.
