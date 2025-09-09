import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';

import '../data/data.dart';
import '../data/tween.dart';
import '../options/align_on_update.dart';
import '../options/focal_point.dart';
import '../options/indicators.dart';
import '../options/style.dart';
import 'location_marker_layer.dart';

/// A layer for current location marker in [FlutterMap].
class CurrentLocationLayer extends StatefulWidget {
  /// The style to use for this location marker.
  final LocationMarkerStyle style;

  /// A stream that provide position data for this marker. Defaults to
  /// [LocationMarkerDataStreamFactory.fromGeolocatorPositionStream].
  final Stream<LocationMarkerPosition?>? positionStream;

  /// A stream that provide heading data for this marker. Defaults to
  /// [LocationMarkerDataStreamFactory.fromCompassHeadingStream].
  final Stream<LocationMarkerHeading?>? headingStream;

  /// A screen point to align the marker when an 'align position event' is
  /// emitted. An 'align position event' is emitted under the following
  /// circumstances:
  /// 1. The first location update occurs, and [alignPositionOnUpdate] is set
  ///   to [AlignOnUpdate.once] or [AlignOnUpdate.always].
  /// 2. Any subsequent location update occurs, and [alignPositionOnUpdate] is
  ///   set to [AlignOnUpdate.always].
  /// 3. An event from [alignPositionStream] is received.
  /// Defaults to the center of the map widget.
  final FocalPoint focalPoint;

  /// A stream that emits an 'align position event'. Emit an event with a
  /// optional zoom level to this stream to align the marker position to the
  /// focal point at the specified zoom level. If null is emitted, the zoom
  /// level remains unchanged. Defaults to null.
  ///
  /// For more details, see
  /// [CenterFabExample](https://github.com/tlserver/flutter_map_location_marker/blob/master/example/lib/page/center_fab_example.dart).
  final Stream<double?>? alignPositionStream;

  /// When should the map follow current location. Default to
  /// [AlignOnUpdate.never].
  final AlignOnUpdate alignPositionOnUpdate;

  /// The duration of the animation of following the map to the current
  /// location. Default to 200ms.
  final Duration alignPositionAnimationDuration;

  /// The curve of the animation of following the map to the current location.
  /// Default to [Curves.fastOutSlowIn].
  final Curve alignPositionAnimationCurve;

  /// A stream that emits an 'align direction event'. Emit an event to this
  /// stream to align the marker direction upwards. Defaults to null.
  final Stream<void>? alignDirectionStream;

  /// When should the plugin rotate the map to keep the heading upward. Default
  /// to [AlignOnUpdate.never].
  final AlignOnUpdate alignDirectionOnUpdate;

  /// The duration of the animation of turning the map to align the heading.
  /// Default to 50ms.
  final Duration alignDirectionAnimationDuration;

  /// The curve of the animation of turning the map to align the heading.
  /// Default to [Curves.easeInOut].
  final Curve alignDirectionAnimationCurve;

  /// The duration of the marker's move animation. Default to 200ms.
  final Duration moveAnimationDuration;

  /// The curve of the marker's move animation. Default to
  /// [Curves.fastOutSlowIn].
  final Curve moveAnimationCurve;

  /// The duration of the heading sector rotate animation. Default to 50ms.
  final Duration rotateAnimationDuration;

  /// The curve of the heading sector rotate animation. Default to
  /// [Curves.easeInOut].
  final Curve rotateAnimationCurve;

  /// The indicators which will display when in special status.
  final LocationMarkerIndicators indicators;

  /// Create a CurrentLocationLayer.
  const CurrentLocationLayer({
    super.key,
    this.style = const LocationMarkerStyle(),
    this.positionStream,
    this.headingStream,
    this.focalPoint = const FocalPoint(),
    this.alignPositionStream,
    this.alignPositionOnUpdate = AlignOnUpdate.never,
    this.alignDirectionStream,
    this.alignDirectionOnUpdate = AlignOnUpdate.never,
    this.alignPositionAnimationDuration = const Duration(milliseconds: 200),
    this.alignPositionAnimationCurve = Curves.fastOutSlowIn,
    this.alignDirectionAnimationDuration = const Duration(milliseconds: 120),
    this.alignDirectionAnimationCurve = Curves.easeOut,
    this.moveAnimationDuration = const Duration(milliseconds: 200),
    this.moveAnimationCurve = Curves.fastOutSlowIn,
    this.rotateAnimationDuration = const Duration(milliseconds: 120),
    this.rotateAnimationCurve = Curves.easeOut,
    this.indicators = const LocationMarkerIndicators(),
  });
  @override
  State<CurrentLocationLayer> createState() => _CurrentLocationLayerState();

  @override
  void debugFillProperties(DiagnosticPropertiesBuilder properties) {
    super.debugFillProperties(properties);
    properties
      ..add(DiagnosticsProperty('style', style))
      ..add(DiagnosticsProperty('positionStream', positionStream))
      ..add(DiagnosticsProperty('headingStream', headingStream))
      ..add(DiagnosticsProperty('focalPoint', focalPoint))
      ..add(DiagnosticsProperty('alignPositionStream', alignPositionStream))
      ..add(DiagnosticsProperty('alignDirectionStream', alignDirectionStream))
      ..add(EnumProperty('alignPositionOnUpdate', alignPositionOnUpdate))
      ..add(EnumProperty('alignDirectionOnUpdate', alignDirectionOnUpdate))
      ..add(
        DiagnosticsProperty(
          'alignPositionAnimationDuration',
          alignPositionAnimationDuration,
        ),
      )
      ..add(
        DiagnosticsProperty(
          'alignPositionAnimationCurve',
          alignPositionAnimationCurve,
        ),
      )
      ..add(
        DiagnosticsProperty(
          'alignDirectionAnimationDuration',
          alignDirectionAnimationDuration,
        ),
      )
      ..add(
        DiagnosticsProperty(
          'alignDirectionAnimationCurve',
          alignDirectionAnimationCurve,
        ),
      )
      ..add(DiagnosticsProperty('moveAnimationDuration', moveAnimationDuration))
      ..add(DiagnosticsProperty('moveAnimationCurve', moveAnimationCurve))
      ..add(
        DiagnosticsProperty(
          'rotateAnimationDuration',
          rotateAnimationDuration,
        ),
      )
      ..add(DiagnosticsProperty('rotateAnimationCurve', rotateAnimationCurve))
      ..add(DiagnosticsProperty('indicators', indicators));
  }
}

class _CurrentLocationLayerState extends State<CurrentLocationLayer> with TickerProviderStateMixin {
  final _Status _status = _Status.initialing;
  LocationMarkerPosition? _currentPosition;
  LocationMarkerHeading? _currentHeading;
  double? _followingZoom;

  late bool _isFirstLocationUpdate;
  late bool _isFirstHeadingUpdate;

  late StreamSubscription<LocationMarkerPosition?> _positionStreamSubscription;
  StreamSubscription<LocationMarkerHeading?>? _headingStreamSubscription;

  /// Subscription to a stream for following single that also include a zoom
  /// level.
  StreamSubscription<double?>? _alignPositionStreamSubscription;

  /// Subscription to a stream for single indicate turning the heading up.
  StreamSubscription<void>? _alignDirectionStreamSubscription;

  AnimationController? _moveMapAnimationController;
  AnimationController? _moveMarkerAnimationController;
  AnimationController? _rotateMapAnimationController;
  AnimationController? _rotateMarkerAnimationController;

  @override
  void initState() {
    super.initState();
    _isFirstLocationUpdate = true;
    _isFirstHeadingUpdate = true;
  }

  @override
  void didUpdateWidget(CurrentLocationLayer oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (widget.positionStream != oldWidget.positionStream) {
      _positionStreamSubscription.cancel();
    }
    if (_status == _Status.ready) {
      if (widget.headingStream != oldWidget.headingStream) {
        _headingStreamSubscription?.cancel();
      }
      if (widget.alignPositionStream != oldWidget.alignPositionStream) {
        _alignPositionStreamSubscription?.cancel();
        _subscriptAlignPositionStream();
      }
      if (widget.alignDirectionStream != oldWidget.alignDirectionStream) {
        _alignDirectionStreamSubscription?.cancel();
        _subscriptAlignDirectionStream();
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    switch (_status) {
      case _Status.initialing:
        return const SizedBox.shrink();
      case _Status.ready:
        if (_currentPosition != null) {
          return LocationMarkerLayer(
            position: _currentPosition!,
            heading: _currentHeading,
            style: widget.style,
          );
        } else {
          return const SizedBox.shrink();
        }
      case _Status.incorrectSetup:
        if (kDebugMode) {
          return SizedBox.expand(
            child: ColoredBox(
              color: Colors.red.withAlpha(0x80),
              child: const Padding(
                padding: EdgeInsets.all(8),
                child: Text(
                  'LocationMarker plugin has not been setup correctly. '
                  'Please follow the instructions in the documentation.',
                  style: TextStyle(fontSize: 26),
                ),
              ),
            ),
          );
        } else {
          return const SizedBox.shrink();
        }
      case _Status.permissionRequesting:
        if (widget.indicators.permissionRequesting != null) {
          return widget.indicators.permissionRequesting!;
        }
        if (kDebugMode) {
          return const Align(
            alignment: Alignment.topRight,
            child: Padding(
              padding: EdgeInsets.all(12),
              child: Text(
                'Location Access Permission Requesting\n'
                '(Debug Mode Only)',
                textAlign: TextAlign.right,
              ),
            ),
          );
        } else {
          return const SizedBox.shrink();
        }
      case _Status.permissionDenied:
        if (widget.indicators.permissionDenied != null) {
          return widget.indicators.permissionDenied!;
        }
        if (kDebugMode) {
          return const Align(
            alignment: Alignment.topRight,
            child: Padding(
              padding: EdgeInsets.all(12),
              child: Text(
                'Location Access Permission Denied\n'
                '(Debug Mode Only)',
                textAlign: TextAlign.right,
              ),
            ),
          );
        } else {
          return const SizedBox.shrink();
        }
      case _Status.serviceDisabled:
        if (widget.indicators.serviceDisabled != null) {
          return widget.indicators.serviceDisabled!;
        }
        if (kDebugMode) {
          return const Align(
            alignment: Alignment.topRight,
            child: Padding(
              padding: EdgeInsets.all(12),
              child: Text(
                'Location Service Disabled\n'
                '(Debug Mode Only)',
                textAlign: TextAlign.right,
              ),
            ),
          );
        } else {
          return const SizedBox.shrink();
        }
    }
  }

  @override
  void dispose() {
    _positionStreamSubscription.cancel();
    _headingStreamSubscription?.cancel();
    _alignPositionStreamSubscription?.cancel();
    _alignDirectionStreamSubscription?.cancel();
    _moveMapAnimationController?.dispose();
    _moveMapAnimationController = null;
    _moveMarkerAnimationController?.dispose();
    _moveMarkerAnimationController = null;
    _rotateMapAnimationController?.dispose();
    _rotateMapAnimationController = null;
    _rotateMarkerAnimationController?.dispose();
    _rotateMarkerAnimationController = null;
    super.dispose();
  }

  void _subscriptAlignPositionStream() {
    if (_alignPositionStreamSubscription != null) {
      return;
    }
    _alignPositionStreamSubscription = widget.alignPositionStream?.listen((zoom) {
      if (!mounted) {
        return;
      }
      if (_currentPosition != null) {
        _followingZoom = zoom;
        _moveMap(
          LatLng(_currentPosition!.latitude, _currentPosition!.longitude),
          zoom,
        ).whenComplete(() => _followingZoom = null);
      }
    });
  }

  void _subscriptAlignDirectionStream() {
    if (_alignDirectionStreamSubscription != null) {
      return;
    }
    _alignDirectionStreamSubscription = widget.alignDirectionStream?.listen((_) {
      if (!mounted) {
        return;
      }
      if (_currentHeading != null) {
        _rotateMap(-_currentHeading!.heading % (2 * pi));
      }
    });
  }

  TickerFuture _moveMap(LatLng latLng, [double? zoom]) {
    final camera = MapCamera.of(context);
    final options = MapOptions.of(context);
    zoom ??= camera.zoom;

    final projectedFocalPoint = widget.focalPoint.project(camera.nonRotatedSize);

    final LatLng beginLatLng;
    if (projectedFocalPoint == Offset.zero) {
      beginLatLng = camera.center;
    } else {
      final crs = options.crs;
      final mapCenter = crs.latLngToOffset(camera.center, camera.zoom);
      final followPoint = camera.rotatePoint(
        mapCenter,
        mapCenter + projectedFocalPoint,
      );
      beginLatLng = crs.offsetToLatLng(followPoint, camera.zoom);
    }

    _moveMapAnimationController?.dispose();
    _moveMapAnimationController = AnimationController(
      duration: widget.alignPositionAnimationDuration,
      vsync: this,
    );
    final animation = CurvedAnimation(
      parent: _moveMapAnimationController!,
      curve: widget.alignPositionAnimationCurve,
    );
    final latTween = Tween(
      begin: beginLatLng.latitude,
      end: latLng.latitude,
    );
    final lngTween = Tween(
      begin: beginLatLng.longitude,
      end: latLng.longitude,
    );
    final zoomTween = Tween(
      begin: camera.zoom,
      end: zoom,
    );

    _moveMapAnimationController!.addListener(() {
      final evaluatedLatLng = LatLng(
        latTween.evaluate(animation),
        lngTween.evaluate(animation),
      );
      final evaluatedZoom = zoomTween.evaluate(animation);

      final halfMapSize = camera.nonRotatedSize * 0.5;
      final projectedFocalPoint = widget.focalPoint.project(halfMapSize);

      if (projectedFocalPoint == halfMapSize) {
        MapController.of(context).move(
          evaluatedLatLng,
          evaluatedZoom,
        );
      } else {
        MapController.of(context).move(
          evaluatedLatLng,
          evaluatedZoom,
          offset: projectedFocalPoint,
        );
      }
    });

    _moveMapAnimationController!.addStatusListener((status) {
      if (status == AnimationStatus.completed || status == AnimationStatus.dismissed) {
        _moveMapAnimationController!.dispose();
        _moveMapAnimationController = null;
      }
    });

    return _moveMapAnimationController!.forward();
  }

  TickerFuture _rotateMap(double angle) {
    final camera = MapCamera.maybeOf(context)!;

    _rotateMapAnimationController?.dispose();
    if ((camera.rotationRad - angle).abs() < 0.006) {
      _rotateMapAnimationController = null;
      return TickerFuture.complete();
    }
    _rotateMapAnimationController = AnimationController(
      duration: widget.alignDirectionAnimationDuration,
      vsync: this,
    );
    final animation = CurvedAnimation(
      parent: _rotateMapAnimationController!,
      curve: widget.alignDirectionAnimationCurve,
    );
    final angleTween = RadiusTween(
      begin: camera.rotationRad,
      end: angle,
    );

    _rotateMapAnimationController!.addListener(() {
      final evaluatedAngle = angleTween.evaluate(animation) / pi * 180;

      final halfMapSize = camera.nonRotatedSize * 0.5;
      final projectedFocalPoint = widget.focalPoint.project(halfMapSize);

      if (projectedFocalPoint == halfMapSize) {
        MapController.of(context).rotate(evaluatedAngle);
      } else {
        MapController.of(context).rotateAroundPoint(
          evaluatedAngle,
          offset: projectedFocalPoint,
        );
      }
    });

    _rotateMapAnimationController!.addStatusListener((status) {
      if (status == AnimationStatus.completed || status == AnimationStatus.dismissed) {
        _rotateMapAnimationController!.dispose();
        _rotateMapAnimationController = null;
      }
    });

    return _rotateMapAnimationController!.forward();
  }

  @override
  void debugFillProperties(DiagnosticPropertiesBuilder properties) {
    super.debugFillProperties(properties);
    properties
      ..add(EnumProperty('_status', _status))
      ..add(DiagnosticsProperty('_currentPosition', _currentPosition))
      ..add(DiagnosticsProperty('_currentHeading', _currentHeading))
      ..add(DoubleProperty('_followingZoom', _followingZoom))
      ..add(
        DiagnosticsProperty(
          '_isFirstLocationUpdate',
          _isFirstLocationUpdate,
        ),
      )
      ..add(DiagnosticsProperty('_isFirstHeadingUpdate', _isFirstHeadingUpdate))
      ..add(
        DiagnosticsProperty(
          '_positionStreamSubscription',
          _positionStreamSubscription,
        ),
      )
      ..add(
        DiagnosticsProperty(
          '_headingStreamSubscription',
          _headingStreamSubscription,
        ),
      )
      ..add(
        DiagnosticsProperty(
          '_alignPositionStreamSubscription',
          _alignPositionStreamSubscription,
        ),
      )
      ..add(
        DiagnosticsProperty(
          '_alignDirectionStreamSubscription',
          _alignDirectionStreamSubscription,
        ),
      )
      ..add(
        DiagnosticsProperty(
          '_moveMapAnimationController',
          _moveMapAnimationController,
        ),
      )
      ..add(
        DiagnosticsProperty(
          '_moveMarkerAnimationController',
          _moveMarkerAnimationController,
        ),
      )
      ..add(
        DiagnosticsProperty(
          '_rotateMapAnimationController',
          _rotateMapAnimationController,
        ),
      )
      ..add(
        DiagnosticsProperty(
          '_rotateMarkerAnimationController',
          _rotateMarkerAnimationController,
        ),
      );
  }
}

enum _Status {
  initialing,
  incorrectSetup,
  serviceDisabled,
  permissionRequesting,
  permissionDenied,
  ready,
}
