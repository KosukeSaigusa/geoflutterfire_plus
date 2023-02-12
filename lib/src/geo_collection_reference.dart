import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:rxdart/rxdart.dart';
import 'package:async/async.dart' show StreamGroup;

import '../geoflutterfire_plus.dart';
import 'math.dart';

/// Extended cloud_firestore [CollectionReference] for geo query features.
class GeoCollectionReference<T> {
  GeoCollectionReference(CollectionReference<T> collectionReference)
      : _collectionReference = collectionReference;

  /// [CollectionReference] of target collection.
  final CollectionReference<T> _collectionReference;

  /// Detection range buffer when not strict mode.
  static const _detectionRangeBuffer = 1.02;

  /// Creates a document with provided [data].
  Future<DocumentReference<T>> add(T data) => _collectionReference.add(data);

  /// Sets the provided [data] on the document.
  Future<void> setDocument({
    required String id,
    required T data,
    bool merge = false,
  }) =>
      _collectionReference.doc(id).set(data, SetOptions(merge: merge));

  /// Deletes the document from the collection.
  Future<void> delete(String id) => _collectionReference.doc(id).delete();

  /// Sets or updates the pair of ([latitude], [longitude]) as cloud_firestore
  /// [GeoPoint] and geohash string on the document's given [field].
  Future<void> setPoint({
    required String id,
    required String field,
    required double latitude,
    required double longitude,
  }) =>
      // Note: Remove type to enable to set the values as `Map`.
      // ignore: unnecessary_cast
      (_collectionReference.doc(id) as DocumentReference).set(
        <String, dynamic>{field: GeoFirePoint(latitude, longitude).data},
        SetOptions(merge: true),
      );

  /// Subscribes geo query results by given conditions.
  ///
  /// * [center] Center point of detection.
  /// * [radiusInKm] Detection range in kilometers.
  /// * [field] Field name of cloud_firestore document.
  /// * [geopointFrom] Function to get cloud_firestore [GeoPoint] instance from
  /// the object (type T).
  /// * [queryBuilder] Specifies query if you would like to give additional
  /// conditions.
  /// * [strictMode] Whether to filter documents strictly within the bound of
  /// given radius.
  Stream<List<DocumentSnapshot<T>>> subscribeWithin({
    required GeoFirePoint center,
    required double radiusInKm,
    required String field,
    required GeoPoint Function(T obj) geopointFrom,
    Query<T>? Function(Query<T> query)? queryBuilder,
    bool strictMode = false,
  }) =>
      subscribeWithinWithDistance(
        center: center,
        radiusInKm: radiusInKm,
        field: field,
        geopointFrom: geopointFrom,
        queryBuilder: queryBuilder,
        strictMode: strictMode,
      ).map(
        (snapshots) =>
            snapshots.map((snapshot) => snapshot.documentSnapshot).toList(),
      );

  /// Subscribes geo query results with distance from center in kilometers
  /// ([GeoDocumentSnapshot]) by given conditions.
  ///
  /// * [center] Center point of detection.
  /// * [radiusInKm] Detection range in kilometers.
  /// * [field] Field name of cloud_firestore document.
  /// * [geopointFrom] Function to get cloud_firestore [GeoPoint] instance from
  /// the object (type T).
  /// * [queryBuilder] Specifies query if you would like to give additional
  /// conditions.
  /// * [strictMode] Whether to filter documents strictly within the bound of
  /// given radius.
  Stream<List<GeoDocumentSnapshot<T>>> subscribeWithinWithDistance({
    required GeoFirePoint center,
    required double radiusInKm,
    required String field,
    required GeoPoint Function(T obj) geopointFrom,
    Query<T>? Function(Query<T> query)? queryBuilder,
    bool strictMode = false,
  }) {
    final collectionStreams = _collectionStreams(
      center: center,
      radiusInKm: radiusInKm,
      field: field,
      queryBuilder: queryBuilder,
    );

    final mergedCollectionStreams = _mergeCollectionStreams(collectionStreams);

    final filteredGeoDocumentSnapshots =
        mergedCollectionStreams.map((queryDocumentSnapshots) {
      final geoDocumentSnapshots = queryDocumentSnapshots
          .map(
            (queryDocumentSnapshot) =>
                _nullableGeoDocumentSnapshotFromQueryDocumentSnapshot(
              queryDocumentSnapshot: queryDocumentSnapshot,
              geopointFrom: geopointFrom,
              center: center,
            ),
          )
          // Removes null values.
          .whereType<GeoDocumentSnapshot<T>>();

      // Filter fetched geoDocumentSnapshots by distance from center point on
      // client side if strict mode.
      final filteredList = geoDocumentSnapshots.where(
        (geoDocumentSnapshot) =>
            !strictMode ||
            geoDocumentSnapshot.distanceFromCenterInKm <=
                radiusInKm * _detectionRangeBuffer,
      );

      // Returns sorted list by distance from center point.
      return filteredList.toList()
        ..sort(
          (a, b) =>
              (a.distanceFromCenterInKm * 1000).toInt() -
              (b.distanceFromCenterInKm * 1000).toInt(),
        );
    });
    return filteredGeoDocumentSnapshots.asBroadcastStream();
  }

  /// Fetches geo query results by given conditions.
  ///
  /// * [center] Center point of detection.
  /// * [radiusInKm] Detection range in kilometers.
  /// * [field] Field name of cloud_firestore document.
  /// * [geopointFrom] Function to get cloud_firestore [GeoPoint] instance from
  /// the object (type T).
  /// * [queryBuilder] Specifies query if you would like to give additional
  /// conditions.
  /// * [strictMode] Whether to filter documents strictly within the bound of
  /// given radius.
  Future<List<DocumentSnapshot<T>>> fetchWithin({
    required GeoFirePoint center,
    required double radiusInKm,
    required String field,
    required GeoPoint Function(T obj) geopointFrom,
    Query<T>? Function(Query<T> query)? queryBuilder,
    bool strictMode = false,
  }) async {
    final geoDocumentSnapshots = await fetchWithinWithDistance(
      center: center,
      radiusInKm: radiusInKm,
      field: field,
      geopointFrom: geopointFrom,
      queryBuilder: queryBuilder,
      strictMode: strictMode,
    );
    return geoDocumentSnapshots
        .map((snapshot) => snapshot.documentSnapshot)
        .toList();
  }

  /// Fetches geo query results with distance from center in kilometers
  /// ([GeoDocumentSnapshot]) by given conditions.
  ///
  /// * [center] Center point of detection.
  /// * [radiusInKm] Detection range in kilometers.
  /// * [field] Field name of cloud_firestore document.
  /// * [geopointFrom] Function to get cloud_firestore [GeoPoint] instance from
  /// the object (type T).
  /// * [queryBuilder] Specifies query if you would like to give additional
  /// conditions.
  /// * [strictMode] Whether to filter documents strictly within the bound of
  /// given radius.
  Future<List<GeoDocumentSnapshot<T>>> fetchWithinWithDistance({
    required GeoFirePoint center,
    required double radiusInKm,
    required String field,
    required GeoPoint Function(T obj) geopointFrom,
    Query<T>? Function(Query<T> query)? queryBuilder,
    bool strictMode = false,
  }) async {
    final collectionFutures = _collectionFutures(
      center: center,
      radiusInKm: radiusInKm,
      field: field,
      queryBuilder: queryBuilder,
    );

    final mergedCollections = await _mergeCollectionFutures(collectionFutures);

    final geoDocumentSnapshots = mergedCollections
        .map(
          (queryDocumentSnapshot) =>
              _nullableGeoDocumentSnapshotFromQueryDocumentSnapshot(
            queryDocumentSnapshot: queryDocumentSnapshot,
            geopointFrom: geopointFrom,
            center: center,
          ),
        ) // Removes null values.
        .whereType<GeoDocumentSnapshot<T>>();

    // Filter fetched geoDocumentSnapshots by distance from center point on
    // client side if strict mode.
    final filteredList = geoDocumentSnapshots
        .where(
          (geoDocumentSnapshot) =>
              !strictMode ||
              geoDocumentSnapshot.distanceFromCenterInKm <=
                  radiusInKm * _detectionRangeBuffer,
        )
        .toList()
      // sort list by distance from center point.
      ..sort(
        (a, b) =>
            (a.distanceFromCenterInKm * 1000).toInt() -
            (b.distanceFromCenterInKm * 1000).toInt(),
      );
    return filteredList;
  }

  /// Returns stream of [QueryDocumentSnapshot]s of neighbor and center
  /// Geohashes.
  List<Stream<List<QueryDocumentSnapshot<T>>>> _collectionStreams({
    required double radiusInKm,
    required GeoFirePoint center,
    required String field,
    Query<T>? Function(Query<T> query)? queryBuilder,
  }) {
    return _geohashes(radiusInKm: radiusInKm, center: center)
        .map(
          (geohash) => _query(queryBuilder)
              .orderBy('$field.geohash')
              .startAt([geohash])
              .endAt(['$geohash~'])
              .snapshots()
              .map((querySnapshot) => querySnapshot.docs),
        )
        .toList();
  }

  /// Returns future of [QueryDocumentSnapshot]s of neighbor and center
  /// Geohashes.
  List<Future<List<QueryDocumentSnapshot<T>>>> _collectionFutures({
    required double radiusInKm,
    required GeoFirePoint center,
    required String field,
    Query<T>? Function(Query<T> query)? queryBuilder,
  }) {
    return _geohashes(radiusInKm: radiusInKm, center: center).map(
      (geohash) async {
        final querySnapshot = await _query(queryBuilder)
            .orderBy('$field.geohash')
            .startAt([geohash]).endAt(['$geohash~']).get();
        return querySnapshot.docs;
      },
    ).toList();
  }

  /// Returns neighbor and center geohash strings.
  List<String> _geohashes({
    required double radiusInKm,
    required GeoFirePoint center,
  }) {
    final precisionDigits = geohashDigitsFromRadius(radiusInKm);
    final centerGeohash = center.geohash.substring(0, precisionDigits);
    return [
      ...neighborGeohashesOf(geohash: centerGeohash),
      centerGeohash,
    ];
  }

  /// Add query conditions, if queryBuilder parameter is given.
  Query<T> _query(Query<T>? Function(Query<T> query)? queryBuilder) {
    Query<T> query = _collectionReference;
    if (queryBuilder != null) {
      query = queryBuilder(query)!;
    }
    return query;
  }

  /// Merge given list of collection streams by `Rx.combineLatest`.
  ///
  /// Note:
  ///
  /// ```dart
  /// final stream1 = Stream.value([1, 2, 3]);
  /// final stream2 = Stream.value([11, 12, 13]);
  /// final streams = [stream1, stream2];
  ///
  /// Rx.combineLatest<List<int>, List<int>>(
  ///   streams,
  ///   (values) => [
  ///     for (final numbers in values) ...numbers,
  ///   ],
  /// ).listen(print);
  ///
  /// // [1, 2, 3, 11, 12, 13]
  /// ```
  // Stream<List<QueryDocumentSnapshot<T>>> _mergeCollectionStreams(
  //   List<Stream<List<QueryDocumentSnapshot<T>>>> collectionStreams,
  // ) =>
  //     Rx.combineLatest<List<QueryDocumentSnapshot<T>>,
  //         List<QueryDocumentSnapshot<T>>>(
  //       collectionStreams,
  //       (values) => [
  //         for (final queryDocumentSnapshots in values)
  //           ...queryDocumentSnapshots,
  //       ],
  //     );

  Stream<List<QueryDocumentSnapshot<T>>> _mergeCollectionStreams(
    List<Stream<List<QueryDocumentSnapshot<T>>>> collectionStreams,
  ) async* {
    List<QueryDocumentSnapshot<T>> mergedStream = [];
    await for (final stream in StreamGroup.merge(collectionStreams)) {
      mergedStream.addAll(stream);
      yield mergedStream;
    }
  }

  /// Merge given list of collection futures.
  Future<List<QueryDocumentSnapshot<T>>> _mergeCollectionFutures(
      List<Future<List<QueryDocumentSnapshot<T>>>> collectionFutures) async {
    final mergedQueryDocumentSnapshots = <QueryDocumentSnapshot<T>>[];
    await Future.forEach<Future<List<QueryDocumentSnapshot<T>>>>(
        collectionFutures, (values) async {
      final queryDocumentSnapshots = await values;
      queryDocumentSnapshots.forEach((queryDocumentSnapshot) {
        mergedQueryDocumentSnapshots.add(queryDocumentSnapshot);
      });
    });
    return mergedQueryDocumentSnapshots;
  }

  /// Returns nullable [GeoDocumentSnapshot] from given [QueryDocumentSnapshot].
  GeoDocumentSnapshot<T>?
      _nullableGeoDocumentSnapshotFromQueryDocumentSnapshot({
    required QueryDocumentSnapshot<T> queryDocumentSnapshot,
    required GeoPoint Function(T obj) geopointFrom,
    required GeoFirePoint center,
  }) {
    final exists = queryDocumentSnapshot.exists;
    if (!exists) {
      return null;
    }
    final fetchedData = queryDocumentSnapshot.data();
    final fetchedGeopoint = geopointFrom(fetchedData);
    final distanceFromCenterInKm = center.distanceBetweenInKm(
      latitude: fetchedGeopoint.latitude,
      longitude: fetchedGeopoint.longitude,
    );
    return GeoDocumentSnapshot(
      documentSnapshot: queryDocumentSnapshot,
      distanceFromCenterInKm: distanceFromCenterInKm,
    );
  }
}

/// A model to handle cloud_firestore [DocumentSnapshot] with distance from
/// given center in kilometers.
class GeoDocumentSnapshot<T> {
  GeoDocumentSnapshot({
    required this.documentSnapshot,
    required this.distanceFromCenterInKm,
  });

  /// Fetched [DocumentSnapshot].
  final DocumentSnapshot<T> documentSnapshot;

  /// Distance from center in kilometers.
  final double distanceFromCenterInKm;
}
