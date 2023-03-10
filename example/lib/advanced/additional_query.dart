import 'dart:async';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';
import 'package:geoflutterfire_plus/geoflutterfire_plus.dart';
import 'package:google_maps_flutter/google_maps_flutter.dart';

import '../add_location.dart';
import '../set_or_delete_location.dart';
import 'entity.dart';
import 'utils.dart';

/// Tokyo Station location for demo.
const _tokyoStation = LatLng(35.681236, 139.767125);

class AdditionalQueryExample extends StatefulWidget {
  const AdditionalQueryExample({super.key});

  @override
  AdditionalQueryExampleState createState() => AdditionalQueryExampleState();
}

/// Example page using [GoogleMap].
class AdditionalQueryExampleState extends State<AdditionalQueryExample> {
  /// Camera position on Google Maps.
  /// Used as center point when running geo query.
  CameraPosition _cameraPosition = _initialCameraPosition;

  /// Detection radius (km) from the center point when running geo query.
  double _radiusInKm = _initialRadiusInKm;

  /// [Marker]s on Google Maps.
  Set<Marker> _markers = {};

  /// Geo query [StreamSubscription].
  late StreamSubscription<List<DocumentSnapshot<Location>>> _subscription;

  /// Returns geo query [StreamSubscription] with listener.
  StreamSubscription<List<DocumentSnapshot<Location>>> _geoQuerySubscription({
    required GeoPoint centerGeoPoint,
    required double radiusInKm,
  }) =>
      GeoCollectionReference(typedCollectionReference)
          .subscribeWithin(
        center: GeoFirePoint(centerGeoPoint),
        radiusInKm: radiusInKm,
        field: 'geo',
        geopointFrom: (location) => location.geo.geopoint,
        queryBuilder: (query) => query.where('isVisible', isEqualTo: true),
        strictMode: true,
      )
          .listen((documentSnapshots) {
        final markers = <Marker>{};
        for (final ds in documentSnapshots) {
          final id = ds.id;
          final location = ds.data();
          if (location == null) {
            continue;
          }
          final name = location.name;
          final geoPoint = location.geo.geopoint;
          markers.add(
            Marker(
              markerId:
                  MarkerId('(${geoPoint.latitude}, ${geoPoint.longitude})'),
              position: LatLng(geoPoint.latitude, geoPoint.longitude),
              infoWindow: InfoWindow(title: name),
              onTap: () async {
                final geoFirePoint = GeoFirePoint(
                  GeoPoint(geoPoint.latitude, geoPoint.longitude),
                );
                await showDialog<void>(
                  context: context,
                  builder: (context) => SetOrDeleteLocationDialog(
                    id: id,
                    name: name,
                    geoFirePoint: geoFirePoint,
                  ),
                );
              },
            ),
          );
        }
        debugPrint('???? markers (${markers.length}): $markers');
        setState(() {
          _markers = markers;
        });
      });

  /// Initial geo query detection radius in km.
  static const double _initialRadiusInKm = 1;

  /// Google Maps initial camera zoom level.
  static const double _initialZoom = 14;

  /// Google Maps initial target position.
  static final LatLng _initialTarget = LatLng(
    _tokyoStation.latitude,
    _tokyoStation.longitude,
  );

  /// Google Maps initial camera position.
  static final _initialCameraPosition = CameraPosition(
    target: _initialTarget,
    zoom: _initialZoom,
  );

  @override
  void initState() {
    _subscription = _geoQuerySubscription(
      centerGeoPoint: GeoPoint(
        _cameraPosition.target.latitude,
        _cameraPosition.target.longitude,
      ),
      radiusInKm: _radiusInKm,
    );
    super.initState();
  }

  @override
  void dispose() {
    _subscription.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Stack(
        children: [
          GoogleMap(
            zoomControlsEnabled: false,
            myLocationButtonEnabled: false,
            initialCameraPosition: _initialCameraPosition,
            markers: _markers,
            circles: {
              Circle(
                circleId: const CircleId('value'),
                center: LatLng(
                  _cameraPosition.target.latitude,
                  _cameraPosition.target.longitude,
                ),
                // multiple 1000 to convert from kilometers to meters.
                radius: _radiusInKm * 1000,
                fillColor: Colors.black12,
                strokeWidth: 0,
              ),
            },
            onCameraMove: (cameraPosition) {
              debugPrint('???? lat: ${cameraPosition.target.latitude}, '
                  'lng: ${cameraPosition.target.latitude}');
              _cameraPosition = cameraPosition;
              _subscription = _geoQuerySubscription(
                centerGeoPoint: GeoPoint(
                  _cameraPosition.target.latitude,
                  _cameraPosition.target.longitude,
                ),
                radiusInKm: _radiusInKm,
              );
            },
            onLongPress: (latLng) => showDialog<void>(
              context: context,
              builder: (context) => AddLocationDialog(latLng: latLng),
            ),
          ),
          Container(
            width: double.infinity,
            margin: const EdgeInsets.only(top: 64, left: 16, right: 16),
            padding: const EdgeInsets.all(16),
            decoration: const BoxDecoration(
              color: Colors.black38,
              borderRadius: BorderRadius.all(Radius.circular(8)),
            ),
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              crossAxisAlignment: CrossAxisAlignment.start,
              mainAxisSize: MainAxisSize.min,
              children: [
                const Text(
                  'Debug window',
                  style: TextStyle(
                    color: Colors.white,
                    fontWeight: FontWeight.bold,
                  ),
                ),
                const SizedBox(height: 8),
                Text(
                  'Currently detected count: '
                  '${_markers.length}',
                  style: const TextStyle(color: Colors.white),
                ),
                const SizedBox(height: 8),
                Text(
                  'Current radius: '
                  '${_radiusInKm.toStringAsFixed(1)} (km)',
                  style: const TextStyle(color: Colors.white),
                ),
                const SizedBox(height: 8),
                Slider(
                  value: _radiusInKm,
                  min: 1,
                  max: 100,
                  divisions: 99,
                  label: _radiusInKm.toStringAsFixed(1),
                  onChanged: (value) {
                    _radiusInKm = value;
                    _subscription = _geoQuerySubscription(
                      centerGeoPoint: GeoPoint(
                        _cameraPosition.target.latitude,
                        _cameraPosition.target.longitude,
                      ),
                      radiusInKm: _radiusInKm,
                    );
                    setState(() {});
                  },
                ),
              ],
            ),
          ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => showModalBottomSheet<void>(
          context: context,
          builder: (context) => const AddLocationDialog(),
        ),
        child: const Icon(Icons.add),
      ),
    );
  }
}
