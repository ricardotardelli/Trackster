import {
  AfterViewInit,
  Component,
  ElementRef,
  Input,
  OnChanges,
  OnDestroy,
  SimpleChanges,
  ViewChild
} from '@angular/core';
import { CommonModule } from '@angular/common';
import * as L from 'leaflet';

interface RoutePoint {
  lat: number;
  lng: number;
  label?: string;
}

interface RoutePayload {
  start: RoutePoint | null;
  waypoints: Record<string, RoutePoint>;
  destination: RoutePoint | null;
}

@Component({
  selector: 'app-gpsplotter',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './gpsplotter.component.html',
  styleUrls: ['./gpsplotter.component.css']
})
export class GpsplotterComponent implements AfterViewInit, OnChanges, OnDestroy {
  @Input() country: string = '';
  @Input() visible: boolean = false;
  @Input() hexCoordinates: string[] = [];

  @ViewChild('mapContainer', { static: false })
  private mapContainer?: ElementRef<HTMLDivElement>;

  private resizeObserver?: ResizeObserver;
  private overlayGroup: L.LayerGroup | null = null;
  private mapReady = false;

  public map: L.Map | null = null;
  public routeData: RoutePayload | null = null;

  ngAfterViewInit(): void {
    console.log('GpsplotterComponent ngAfterViewInit');
    this.routeData = this.rebuildRoutePayloadFromHexCoordinates(this.hexCoordinates);
    console.log('Gpsplotter routeData after view init:', this.routeData);

    if (this.visible) {
      this.initializeWhenReady();
    }
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['hexCoordinates']) {
      this.routeData = this.rebuildRoutePayloadFromHexCoordinates(this.hexCoordinates);
      console.log('Gpsplotter hexCoordinates changed:', this.hexCoordinates?.length ?? 0);
      console.log('Gpsplotter routeData rebuilt:', this.routeData);

      if (this.mapReady) {
        this.refreshLayers();
        this.fitMapToRoute();
      }
    }

    if (changes['country'] && this.mapReady) {
      this.applyCountryToMap();
    }

    if (changes['visible'] && this.visible) {
      console.log('Gpsplotter visible=true, initializing map');
      this.initializeWhenReady();
    }
  }

  ngOnDestroy(): void {
    this.resizeObserver?.disconnect();
    this.resizeObserver = undefined;

    if (this.map) {
      this.map.off();
      this.map.remove();
      this.map = null;
    }

    this.overlayGroup = null;
    this.mapReady = false;
  }

  private initializeWhenReady(): void {
    setTimeout(() => {
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          this.initializeMap();
          this.setupResizeObserver();
          this.refreshMapSize();
          this.refreshLayers();
          this.fitMapToRoute();
        });
      });
    }, 0);
  }

  private initializeMap(): void {
    if (this.mapReady) {
      console.log('Gpsplotter map already ready');
      this.refreshMapSize();
      return;
    }

    const container = this.mapContainer?.nativeElement;
    if (!container) {
      console.error('Gpsplotter mapContainer not found');
      return;
    }

    console.log('Gpsplotter container size:', {
      clientWidth: container.clientWidth,
      clientHeight: container.clientHeight,
      offsetWidth: container.offsetWidth,
      offsetHeight: container.offsetHeight
    });

    const config = this.getCountryMapConfig(this.country);

    try {
      this.map = L.map(container, {
        center: config.center,
        zoom: config.zoom,
        zoomControl: true
      });

      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors'
      }).addTo(this.map);

      this.overlayGroup = L.layerGroup().addTo(this.map);
      this.mapReady = true;

      console.log('Gpsplotter map initialized successfully');
    } catch (error) {
      console.error('Gpsplotter failed to initialize map:', error);
    }
  }

  private setupResizeObserver(): void {
    const container = this.mapContainer?.nativeElement;
    if (!container) {
      return;
    }

    this.resizeObserver?.disconnect();

    this.resizeObserver = new ResizeObserver(() => {
      this.refreshMapSize();
    });

    this.resizeObserver.observe(container);
  }

  private refreshMapSize(): void {
    if (!this.map) {
      return;
    }

    this.map.invalidateSize();

    window.setTimeout(() => this.map?.invalidateSize(), 100);
    window.setTimeout(() => this.map?.invalidateSize(), 250);
    window.setTimeout(() => this.map?.invalidateSize(), 500);
    window.setTimeout(() => this.map?.invalidateSize(), 1000);
  }

  private decodeGpsHexToRoutePoint(hex: string): RoutePoint | null {
    if (typeof hex !== 'string') {
      return null;
    }

    const normalized = hex.trim().toUpperCase();

    if (!/^[0-9A-F]{16}$/.test(normalized)) {
      return null;
    }

    try {
      const bytes = new Uint8Array(
        normalized.match(/.{1,2}/g)!.map((value) => parseInt(value, 16))
      );

      const view = new DataView(bytes.buffer);

      const latScaled = view.getInt32(0, false);
      const lngScaled = view.getInt32(4, false);

      const lat = latScaled / 1_000_000;
      const lng = lngScaled / 1_000_000;

      if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
        return null;
      }

      if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
        return null;
      }

      return {
        lat,
        lng,
        label: `${lat.toFixed(6)}, ${lng.toFixed(6)}`
      };
    } catch {
      return null;
    }
  }

  private rebuildRoutePayloadFromHexCoordinates(hexCoordinates: string[]): RoutePayload | null {
    const decodedPoints = (hexCoordinates ?? [])
      .map((hex) => this.decodeGpsHexToRoutePoint(hex))
      .filter((point): point is RoutePoint => point !== null);

    console.log('Gpsplotter decoded points:', decodedPoints.length);

    if (decodedPoints.length === 0) {
      return null;
    }

    if (decodedPoints.length === 1) {
      return {
        start: decodedPoints[0],
        waypoints: {},
        destination: null
      };
    }

    const start = decodedPoints[0];
    const destination = decodedPoints[decodedPoints.length - 1];
    const middlePoints = decodedPoints.slice(1, -1);

    const waypoints: Record<string, RoutePoint> = {};

    middlePoints.forEach((point, index) => {
      waypoints[String(index + 1)] = point;
    });

    return {
      start,
      waypoints,
      destination
    };
  }

  private getCountryMapConfig(country: string): { center: L.LatLngExpression; zoom: number } {
    const normalized = (country || '').trim().toLowerCase();

    switch (normalized) {
      case 'albania':
        return { center: L.latLng(41.25, 19.97), zoom: 8 };
      case 'portugal':
        return { center: L.latLng(39.63, -8.09), zoom: 8 };
      case 'spain':
        return { center: L.latLng(40.35, -3.62), zoom: 6 };
      case 'france':
        return { center: L.latLng(46.9, 2.1), zoom: 4 };
      case 'brazil':
        return { center: L.latLng(-14.07, -49.71), zoom: 5 };
      case 'united states':
        return { center: L.latLng(39.83, -98.58), zoom: 4 };
      default:
        return { center: L.latLng(39.5, -8.0), zoom: 6 };
    }
  }

  private applyCountryToMap(): void {
    if (!this.map) {
      return;
    }

    const config = this.getCountryMapConfig(this.country);
    this.map.setView(config.center, config.zoom);
    this.refreshMapSize();
  }

  private getMarkerText(
    type: 'Start' | 'Waypoint' | 'Destination',
    point: RoutePoint,
    index?: number
  ): string {
    const value = point.label ? point.label : `${point.lat}, ${point.lng}`;

    if (type === 'Waypoint') {
      const numberText = index !== undefined ? String(index + 1) : '?';
      return `Waypoint ${numberText}: ${value}`;
    }

    return `${type}: ${value}`;
  }

  private createColoredIcon(color: string): L.DivIcon {
    return L.divIcon({
      className: 'custom-map-marker leaflet-marker-icon',
      html: `
        <div
          style="
            width: 18px;
            height: 18px;
            border-radius: 50%;
            background-color: ${color};
            border: 2px solid #ffffff;
            box-shadow: 0 1px 4px rgba(0, 0, 0, 0.35);
            box-sizing: border-box;
          ">
        </div>
      `,
      iconSize: [18, 18],
      iconAnchor: [9, 9]
    });
  }

  private createMarker(point: RoutePoint, color: string, tooltip: string): L.Marker {
    const marker = L.marker([point.lat, point.lng], {
      icon: this.createColoredIcon(color),
      interactive: false,
      bubblingMouseEvents: false,
      riseOnHover: true
    });

    marker.bindTooltip(tooltip, {
      direction: 'top'
    });

    return marker;
  }

  private refreshLayers(): void {
    if (!this.overlayGroup) {
      console.log('Gpsplotter refreshLayers skipped: overlayGroup not ready');
      return;
    }

    this.overlayGroup.clearLayers();

    if (!this.routeData) {
      console.log('Gpsplotter refreshLayers skipped: routeData is null');
      return;
    }

    const path: [number, number][] = [];

    if (this.routeData.start) {
      const startMarker = this.createMarker(
        this.routeData.start,
        '#2e7d32',
        this.getMarkerText('Start', this.routeData.start)
      );

      this.overlayGroup.addLayer(startMarker);
      path.push([this.routeData.start.lat, this.routeData.start.lng]);
    }

    const orderedWaypointKeys = Object.keys(this.routeData.waypoints ?? {}).sort(
      (a, b) => Number(a) - Number(b)
    );

    orderedWaypointKeys.forEach((key, index) => {
      const point = this.routeData?.waypoints[key];
      if (!point) {
        return;
      }

      const waypointMarker = this.createMarker(
        point,
        '#1976d2',
        this.getMarkerText('Waypoint', point, index)
      );

      this.overlayGroup?.addLayer(waypointMarker);
      path.push([point.lat, point.lng]);
    });

    if (this.routeData.destination) {
      const destinationMarker = this.createMarker(
        this.routeData.destination,
        '#f9a825',
        this.getMarkerText('Destination', this.routeData.destination)
      );

      this.overlayGroup.addLayer(destinationMarker);
      path.push([this.routeData.destination.lat, this.routeData.destination.lng]);
    }

    if (path.length >= 2) {
      this.overlayGroup.addLayer(L.polyline(path));
    }

    console.log('Gpsplotter layers rendered. Path size:', path.length);
  }

  private fitMapToRoute(): void {
    if (!this.map || !this.routeData) {
      return;
    }

    const points: L.LatLngExpression[] = [];

    if (this.routeData.start) {
      points.push([this.routeData.start.lat, this.routeData.start.lng]);
    }

    const orderedWaypointKeys = Object.keys(this.routeData.waypoints ?? {}).sort(
      (a, b) => Number(a) - Number(b)
    );

    for (const key of orderedWaypointKeys) {
      const point = this.routeData.waypoints[key];
      if (point) {
        points.push([point.lat, point.lng]);
      }
    }

    if (this.routeData.destination) {
      points.push([this.routeData.destination.lat, this.routeData.destination.lng]);
    }

    if (points.length === 0) {
      return;
    }

    if (points.length === 1) {
      this.map.setView(points[0], 13);
      this.refreshMapSize();
      return;
    }

    this.map.fitBounds(L.latLngBounds(points), { padding: [30, 30] });
    this.refreshMapSize();

    console.log('Gpsplotter fit bounds with points:', points.length);
  }
}