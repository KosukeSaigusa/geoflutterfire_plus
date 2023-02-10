import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';
import 'package:geoflutterfire_plus/geoflutterfire_plus.dart';

/// AlertDialog widget to add location data to Cloud Firestore.
class AddLocationDialog extends StatefulWidget {
  const AddLocationDialog({super.key});

  @override
  AddLocationDialogState createState() => AddLocationDialogState();
}

class AddLocationDialogState extends State<AddLocationDialog> {
  final _nameEditingController = TextEditingController();
  final _latitudeEditingController = TextEditingController();
  final _longitudeEditingController = TextEditingController();

  @override
  void dispose() {
    _latitudeEditingController.dispose();
    _longitudeEditingController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('Enter location data'),
      content: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        mainAxisSize: MainAxisSize.min,
        children: [
          TextField(
            controller: _nameEditingController,
            keyboardType: TextInputType.name,
            decoration: InputDecoration(
              border: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
              ),
              label: const Text('name'),
            ),
          ),
          const SizedBox(height: 16),
          TextField(
            controller: _latitudeEditingController,
            keyboardType: const TextInputType.numberWithOptions(decimal: true),
            decoration: InputDecoration(
              border: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
              ),
              label: const Text('latitude'),
            ),
          ),
          const SizedBox(height: 16),
          TextField(
            controller: _longitudeEditingController,
            keyboardType: const TextInputType.numberWithOptions(decimal: true),
            decoration: InputDecoration(
              border: OutlineInputBorder(
                borderRadius: BorderRadius.circular(10),
              ),
              label: const Text('longitude'),
            ),
          ),
          const SizedBox(height: 16),
          ElevatedButton(
            onPressed: () async {
              final navigator = Navigator.of(context);
              try {
                await _addLocation();
              } on Exception catch (e) {
                debugPrint('🚨 An exception occurred when adding location data'
                    '${e.toString()}');
              }
              navigator.pop();
            },
            child: const Text('Add location data'),
          ),
        ],
      ),
    );
  }

  /// Add location data to Cloud Firestore.
  Future<void> _addLocation() async {
    final name = _nameEditingController.value.text;
    if (name.isEmpty) {
      throw Exception('Enter valid name');
    }
    final latitude = double.tryParse(_latitudeEditingController.value.text);
    final longitude = double.tryParse(_longitudeEditingController.value.text);
    if (latitude == null || longitude == null) {
      throw Exception('Enter valid values as latitude and longitude.');
    }
    final geoFirePoint = GeoFirePoint(latitude, longitude);
    await GeoCollectionReference<Map<String, dynamic>>(
      FirebaseFirestore.instance.collection('locations'),
    ).add(<String, dynamic>{
      'geo': geoFirePoint.data,
      'name': name,
      'isVisible': true,
    });
    debugPrint('🌍 Location data is successfully added: '
        'name: $name'
        'lat: $latitude, '
        'lng: $longitude, '
        'geohash: ${geoFirePoint.geohash}');
  }
}
