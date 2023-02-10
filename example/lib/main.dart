import 'dart:async';

import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:flutter/material.dart';
import 'package:geoflutterfire_plus/geoflutterfire_plus.dart';
import 'package:google_maps_flutter/google_maps_flutter.dart';

import 'add_location.dart';
import 'firebase_options.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Firebase.initializeApp(options: DefaultFirebaseOptions.currentPlatform);
  runApp(const App());
}

class App extends StatelessWidget {
  const App({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Flutter Demo',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        primarySwatch: Colors.blue,
        sliderTheme: SliderThemeData(
          overlayShape: SliderComponentShape.noOverlay,
        ),
      ),
      home: const Example(),
      // See using with converter example by removing comment below:
      // home: const WithConverterExample(),
      // See adding custom queries example by removing comment below:
      // home: const AdditionalQueryExample(),
    );
  }
}

/// Tokyo Station location for demo.
const tokyoStation = LatLng(35.681236, 139.767125);

/// Reference to the collection where the location data is stored.
/// `withConverter` is available to type-safely define [CollectionReference].
final collectionReference = FirebaseFirestore.instance.collection('locations');

class Example extends StatefulWidget {
  const Example({super.key});

  @override
  ExampleState createState() => ExampleState();
}

/// Example page using [GoogleMap].
class ExampleState extends State<Example> {
  GoogleMapController? mapController;

  /// Camera position on Google Maps.
  /// Used as center point when running geo query.
  CameraPosition _cameraPosition = _initialCameraPosition;

  /// Detection radius (km) from the center point when running geo query.
  double _radiusInKm = _initialRadiusInKm;
  double _zoom = _initialZoom;

  /// [Marker]s on Google Maps.
  Set<Marker> _markers = {};

  /// Geo query [StreamSubscription].
  late StreamSubscription<List<DocumentSnapshot<Map<String, dynamic>>>>
      _subscription;

  /// Returns geo query [StreamSubscription] with listener.
  StreamSubscription<List<DocumentSnapshot<Map<String, dynamic>>>>
      _geoQuerySubscription({
    required double latitude,
    required double longitude,
    required double radiusInKm,
  }) =>
          GeoCollectionReference(collectionReference)
              .subscribeWithin(
            center: GeoFirePoint(latitude, longitude),
            radiusInKm: radiusInKm,
            field: 'geo',
            geopointFrom: (data) =>
                (data['geo'] as Map<String, dynamic>)['geopoint'] as GeoPoint,
            strictMode: true,
          )
              .listen((documentSnapshots) {
            final markers = <Marker>{};
            for (final ds in documentSnapshots) {
              final data = ds.data();
              if (data == null) {
                continue;
              }
              final name = data['name'] as String;
              final geoPoint =
                  (data['geo'] as Map<String, dynamic>)['geopoint'] as GeoPoint;
              markers.add(
                Marker(
                  markerId:
                      MarkerId('(${geoPoint.latitude}, ${geoPoint.longitude})'),
                  position: LatLng(geoPoint.latitude, geoPoint.longitude),
                  infoWindow: InfoWindow(title: name),
                ),
              );
            }
            debugPrint('📍 markers (${markers.length}): $markers');
            setState(() {
              _markers = markers;
            });
          });

  /// Initial geo query detection radius in km.
  static const double _initialRadiusInKm = 1;

  /// Google Maps initial camera zoom level.
  static const double _initialZoom = 14;

  /// Google Maps initial camera position.
  static final _initialCameraPosition = CameraPosition(
    target: LatLng(tokyoStation.latitude, tokyoStation.longitude),
    zoom: _initialZoom,
  );

  @override
  void initState() {
    _subscription = _geoQuerySubscription(
      latitude: _cameraPosition.target.latitude,
      longitude: _cameraPosition.target.longitude,
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
            onMapCreated: (controller) {
              setState(() {
                mapController = controller;
              });
            },
            onCameraMove: (cameraPosition) {
              debugPrint('📷 lat: ${cameraPosition.target.latitude}, '
                  'lng: ${cameraPosition.target.latitude}');
              _cameraPosition = cameraPosition;
              _subscription = _geoQuerySubscription(
                latitude: cameraPosition.target.latitude,
                longitude: cameraPosition.target.longitude,
                radiusInKm: _radiusInKm,
              );
              setState(() {});
            },
          ),
          Positioned(
            child: Align(
              alignment: Alignment.topCenter,
              child: Container(
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
                      onChanged: (double value) {
                        _radiusInKm = value;
                        _subscription = _geoQuerySubscription(
                          latitude: _cameraPosition.target.latitude,
                          longitude: _cameraPosition.target.longitude,
                          radiusInKm: _radiusInKm,
                        );
                        setState(() {});
                      },
                    ),
                    const SizedBox(height: 8),
                    Text(
                      'Current zoom: '
                      '${(_initialZoom - _zoom + 1).toStringAsFixed(1)} (level)',
                      style: const TextStyle(color: Colors.white),
                    ),
                    const SizedBox(height: 8),
                    Slider(
                      value: _initialZoom - _zoom,
                      min: 0,
                      max: 6,
                      // divisions: 9,
                      label: _zoom.toStringAsFixed(1),
                      onChanged: (double value) {
                        _zoom = _initialZoom - value;
                        mapController?.animateCamera(
                          CameraUpdate.newCameraPosition(
                            CameraPosition(
                              target: LatLng(
                                _cameraPosition.target.latitude,
                                _cameraPosition.target.longitude,
                              ),
                              zoom: _zoom,
                            ),
                            //17 is new zoom level
                          ),
                        );
                        print(_cameraPosition.zoom);
                        setState(() {});
                      },
                    ),
                  ],
                ),
              ),
            ),
          ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: () => showDialog<void>(
          context: context,
          builder: (context) => const AddLocationDialog(),
        ),
        child: const Icon(Icons.add),
      ),
    );
  }
}
