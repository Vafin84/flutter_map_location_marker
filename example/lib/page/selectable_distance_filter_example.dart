import 'dart:async';

import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:flutter_map_location_marker/flutter_map_location_marker.dart';
import 'package:latlong2/latlong.dart';

class SelectableDistanceFilterExample extends StatefulWidget {
  @override
  State<SelectableDistanceFilterExample> createState() => _SelectableDistanceFilterExampleState();
}

class _SelectableDistanceFilterExampleState extends State<SelectableDistanceFilterExample> {
  static const _distanceFilters = [0, 5, 10, 30, 50];
  int _selectedIndex = 0;

  late final StreamController<LocationMarkerPosition?> _positionStream;
  late StreamSubscription<LocationMarkerPosition?> _streamSubscription;

  @override
  void initState() {
    super.initState();
    _positionStream = StreamController();
  }

  @override
  void dispose() {
    _positionStream.close();
    _streamSubscription.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Selectable Distance Filter Example'),
      ),
      body: FlutterMap(
        options: const MapOptions(
          initialCenter: LatLng(0, 0),
          initialZoom: 1,
          minZoom: 0,
          maxZoom: 19,
        ),
        children: [
          TileLayer(
            urlTemplate: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
            userAgentPackageName: 'net.tlserver6y.flutter_map_location_marker.example',
            maxZoom: 19,
          ),
          CurrentLocationLayer(
            positionStream: _positionStream.stream,
          ),
          Align(
            alignment: Alignment.bottomCenter,
            child: Container(
              color: Theme.of(context).scaffoldBackgroundColor,
              padding: const EdgeInsets.all(8),
              child: Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  const Text("Distance Filter:"),
                  ToggleButtons(
                    isSelected: List.generate(
                      _distanceFilters.length,
                      (index) => index == _selectedIndex,
                      growable: false,
                    ),
                    onPressed: (index) {
                      setState(() => _selectedIndex = index);
                      _streamSubscription.cancel();
                    },
                    children: _distanceFilters.map((distance) => Text(distance.toString())).toList(growable: false),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }
}
