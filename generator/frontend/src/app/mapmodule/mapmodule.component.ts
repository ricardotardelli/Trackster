import {
  AfterViewInit,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  NgZone,
  OnChanges,
  OnDestroy,
  OnInit,
  Output,
  SimpleChanges,
  ViewChild
} from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient, HttpParams } from '@angular/common/http';
import * as L from 'leaflet';

type PointSelectionMode = 'start' | 'destination' | 'waypoints';

interface GeoPoint {
  lat: number;
  lng: number;
  label?: string;
}

@Component({
  selector: 'app-mapmodule',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './mapmodule.component.html',
  styleUrls: ['./mapmodule.component.css']
})
export class MapmoduleComponent implements OnInit, OnChanges, AfterViewInit, OnDestroy {
  @Input() country: string = '';
  @Input() visible: boolean = true;
  @Output() saveRoute = new EventEmitter<string>();

  @ViewChild('mapContainer', { static: false })
  private mapContainer?: ElementRef<HTMLDivElement>;

  private resizeObserver?: ResizeObserver;
  private mapReady = false;
  private contextMenuHandler?: (event: MouseEvent) => void;
  private overlayGroup: L.LayerGroup | null = null;

  private readonly debugEnabled = true;
  private markerSequence = 0;

  constructor(
    private readonly ngZone: NgZone,
    private readonly http: HttpClient
  ) {}

  public locationSearch = '';
  public startFrom = '';
  public destination = '';
  public waypoints: GeoPoint[] = [];
  private readonly defaultCountry: string = 'Portugal';

  public startPoint: GeoPoint | null = null;
  public destinationPoint: GeoPoint | null = null;

  public pointSelectionMode: PointSelectionMode = 'start';

  public options: L.MapOptions = {
    layers: [
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; OpenStreetMap contributors'
      })
    ],
    zoom: 6,
    center: L.latLng(39.5, -8.0)
  };

  public map: L.Map | null = null;
  public layers: L.Layer[] = [];

  ngOnInit(): void {
    this.log('ngOnInit');
    this.applyCountryToMap();
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.log('ngOnChanges', changes);

    if (changes['country']) {
      this.applyCountryToMap();
    }

    if (changes['visible'] && this.visible) {
      this.initializeOrRefreshMap();
    }
  }

  ngAfterViewInit(): void {
    this.log('ngAfterViewInit');
    this.setupResizeObserver();

    setTimeout(() => {
      this.initializeOrRefreshMap();
    }, 0);
  }

  ngOnDestroy(): void {
    this.log('ngOnDestroy');

    this.resizeObserver?.disconnect();
    this.resizeObserver = undefined;

    const container = this.mapContainer?.nativeElement;
    if (container && this.contextMenuHandler) {
      container.removeEventListener('contextmenu', this.contextMenuHandler);
    }

    if (this.map) {
      this.map.off();
      this.map.remove();
      this.map = null;
    }

    this.overlayGroup = null;
    this.mapReady = false;
  }

  public save(): void {
    this.log('save');
    this.saveRoute.emit(this.getOutput());
  }

  public refreshMapSize(): void {
    if (!this.map) {
      return;
    }

    this.log('refreshMapSize');

    this.map.invalidateSize();
    window.setTimeout(() => this.map?.invalidateSize(), 100);
    window.setTimeout(() => this.map?.invalidateSize(), 250);
    window.setTimeout(() => this.map?.invalidateSize(), 500);
  }

  private initializeOrRefreshMap(): void {
    this.log('initializeOrRefreshMap', { mapReady: this.mapReady });

    setTimeout(() => {
      if (!this.mapReady) {
        this.initializeMap();
      } else {
        this.refreshMapSize();
      }
    }, 0);
  }

  private setupResizeObserver(): void {
    const container = this.mapContainer?.nativeElement;
    if (!container) {
      return;
    }

    this.resizeObserver?.disconnect();

    this.resizeObserver = new ResizeObserver(() => {
      this.log('ResizeObserver fired');
      this.refreshMapSize();
    });

    this.resizeObserver.observe(container);
  }

  private getCountryMapConfig(country: string): { center: L.LatLngExpression; zoom: number } {
    const normalized = (country || '').trim().toLowerCase();

    switch (normalized) {
      case 'portugal':
        return { center: L.latLng(39.5, -8.0), zoom: 6 };
      case 'spain':
        return { center: L.latLng(40.2, -3.7), zoom: 6 };
      case 'france':
        return { center: L.latLng(46.2, 2.2), zoom: 6 };
      case 'brazil':
        return { center: L.latLng(-14.2, -51.9), zoom: 4 };
      case 'usa':
      case 'united states':
        return { center: L.latLng(39.8, -98.6), zoom: 4 };
      default:
        return { center: L.latLng(39.5, -8.0), zoom: 6 };
    }
  }

  private applyCountryToMap(): void {
    const effectiveCountry =
      this.country && this.country.trim() !== ''
        ? this.country
        : this.defaultCountry;

    const config = this.getCountryMapConfig(effectiveCountry);

    this.options = {
      ...this.options,
      center: config.center,
      zoom: config.zoom
    };

    this.log('applyCountryToMap', { effectiveCountry, config });

    if (this.map) {
      this.map.setView(config.center, config.zoom);
      this.refreshMapSize();
    }
  }

  private initializeMap(): void {
    if (this.mapReady) {
      this.log('initializeMap skipped because mapReady=true');
      this.refreshMapSize();
      return;
    }

    const container = this.mapContainer?.nativeElement;
    if (!container) {
      this.log('initializeMap aborted: no container');
      return;
    }

    this.log('initializeMap creating map');

    this.map = L.map(container, {
      ...this.options,
      zoomControl: true
    });

    this.overlayGroup = L.layerGroup().addTo(this.map);
    this.mapReady = true;

    this.map.on('click', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent('MAP click', event);
      this.ngZone.run(() => {
        this.onMapClick(event);
      });
    });

    this.map.on('mousedown', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent('MAP mousedown', event);
    });

    this.map.on('mouseup', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent('MAP mouseup', event);
    });

    this.map.on('mouseover', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent('MAP mouseover', event);
    });

    this.map.on('contextmenu', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent('MAP contextmenu', event);
      L.DomEvent.stop(event.originalEvent);

      this.ngZone.run(() => {
        this.onMapRightClick(event);
      });
    });

    this.contextMenuHandler = (event: MouseEvent) => {
      event.preventDefault();
    };

    container.addEventListener('contextmenu', this.contextMenuHandler);

    this.refreshLayers();
    this.refreshMapSize();

    window.setTimeout(() => {
      if (this.map) {
        this.map.setView(this.options.center as L.LatLngExpression, this.options.zoom as number);
        this.refreshMapSize();
      }
    }, 0);
  }

  public getLocationPlaceholder(): string {
    if (this.pointSelectionMode === 'start') {
      return 'Select or type start point name';
    }

    if (this.pointSelectionMode === 'destination') {
      return 'Select or type destination name';
    }

    return 'Search and add waypoint';
  }

  public searchSelectedAddress(): void {
    this.searchAddress(this.locationSearch, this.pointSelectionMode);
  }

  public searchAddress(query: string, target: PointSelectionMode): void {
    const trimmedQuery = query ? query.trim() : '';

    if (!trimmedQuery) {
      return;
    }

    const params = new HttpParams()
      .set('q', trimmedQuery)
      .set('format', 'jsonv2')
      .set('limit', '1');

    this.http
      .get<any[]>('https://nominatim.openstreetmap.org/search', { params })
      .subscribe({
        next: (results: any[]) => {
          this.log('searchAddress result', results);

          if (!results || results.length === 0) {
            return;
          }

          const firstResult = results[0];
          const resolvedName = firstResult.display_name || trimmedQuery;

          const point: GeoPoint = {
            lat: Number(firstResult.lat),
            lng: Number(firstResult.lon),
            label: resolvedName
          };

          if (target === 'start') {
            this.startPoint = point;
            this.startFrom = resolvedName;
            this.locationSearch = resolvedName;
          } else if (target === 'destination') {
            this.destinationPoint = point;
            this.destination = resolvedName;
            this.locationSearch = resolvedName;
          } else {
            this.waypoints.push(point);
            this.locationSearch = '';
          }

          this.refreshLayers();

          if (this.map) {
            this.map.setView([point.lat, point.lng], 13);
            this.refreshMapSize();
          }
        },
        error: (error: unknown) => {
          console.error('Address search failed:', error);
        }
      });
  }

  public onMapClick(event: L.LeafletMouseEvent): void {
    this.logLeafletEvent('onMapClick ENTER', event);

    const lat = Number(event.latlng.lat.toFixed(6));
    const lng = Number(event.latlng.lng.toFixed(6));

    const point: GeoPoint = {
      lat,
      lng,
      label: `${lat}, ${lng}`
    };

    if (this.pointSelectionMode === 'start') {
      this.startPoint = point;
      this.startFrom = point.label || '';
      this.locationSearch = this.startFrom;
    } else if (this.pointSelectionMode === 'destination') {
      this.destinationPoint = point;
      this.destination = point.label || '';
      this.locationSearch = this.destination;
    } else {
      this.waypoints.push(point);
      this.locationSearch = '';
    }

    this.log('onMapClick AFTER mutation', {
      mode: this.pointSelectionMode,
      startPoint: this.startPoint,
      destinationPoint: this.destinationPoint,
      waypoints: this.waypoints
    });

    this.refreshLayers();
  }

  public onMapRightClick(event: L.LeafletMouseEvent): void {
    this.logLeafletEvent('onMapRightClick ENTER', event);

    const clickedLat = Number(event.latlng.lat.toFixed(6));
    const clickedLng = Number(event.latlng.lng.toFixed(6));

    if (this.tryRemoveMarkerNear(clickedLat, clickedLng)) {
      return;
    }

    if (this.pointSelectionMode === 'start' && this.startPoint) {
      this.startPoint = null;
      this.startFrom = '';
      this.locationSearch = '';
      this.refreshLayers();
      return;
    }

    if (this.pointSelectionMode === 'destination' && this.destinationPoint) {
      this.destinationPoint = null;
      this.destination = '';
      this.locationSearch = '';
      this.refreshLayers();
    }
  }

  private tryRemoveMarkerNear(lat: number, lng: number): boolean {
    const tolerance = 0.0008;

    if (
      this.startPoint &&
      Math.abs(this.startPoint.lat - lat) <= tolerance &&
      Math.abs(this.startPoint.lng - lng) <= tolerance
    ) {
      this.log('tryRemoveMarkerNear removed START');
      this.startPoint = null;
      this.startFrom = '';
      if (this.pointSelectionMode === 'start') {
        this.locationSearch = '';
      }
      this.refreshLayers();
      return true;
    }

    if (
      this.destinationPoint &&
      Math.abs(this.destinationPoint.lat - lat) <= tolerance &&
      Math.abs(this.destinationPoint.lng - lng) <= tolerance
    ) {
      this.log('tryRemoveMarkerNear removed DESTINATION');
      this.destinationPoint = null;
      this.destination = '';
      if (this.pointSelectionMode === 'destination') {
        this.locationSearch = '';
      }
      this.refreshLayers();
      return true;
    }

    const waypointIndex = this.waypoints.findIndex((waypoint: GeoPoint) => {
      return (
        Math.abs(waypoint.lat - lat) <= tolerance &&
        Math.abs(waypoint.lng - lng) <= tolerance
      );
    });

    if (waypointIndex >= 0) {
      this.log('tryRemoveMarkerNear removed WAYPOINT', { waypointIndex });
      this.waypoints.splice(waypointIndex, 1);
      this.refreshLayers();
      return true;
    }

    return false;
  }

  public setSelectionMode(mode: PointSelectionMode): void {
    this.log('setSelectionMode', { from: this.pointSelectionMode, to: mode });

    this.pointSelectionMode = mode;

    if (mode === 'start') {
      this.locationSearch = this.startFrom;
    } else if (mode === 'destination') {
      this.locationSearch = this.destination;
    } else {
      this.locationSearch = '';
    }
  }

  public clearAll(): void {
    this.log('clearAll');

    this.locationSearch = '';
    this.startFrom = '';
    this.destination = '';
    this.startPoint = null;
    this.destinationPoint = null;
    this.waypoints = [];
    this.layers = [];

    this.refreshLayers();
  }

  public getOutput(): string {
    const waypointsObject: Record<string, GeoPoint> = {};

    this.waypoints.forEach((wp: GeoPoint, index: number) => {
      waypointsObject[String(index + 1)] = {
        lat: wp.lat,
        lng: wp.lng,
        label: wp.label
      };
    });

    return JSON.stringify(
      {
        start: this.startPoint
          ? {
              lat: this.startPoint.lat,
              lng: this.startPoint.lng,
              label: this.startPoint.label
            }
          : null,
        waypoints: waypointsObject,
        destination: this.destinationPoint
          ? {
              lat: this.destinationPoint.lat,
              lng: this.destinationPoint.lng,
              label: this.destinationPoint.label
            }
          : null
      },
      null,
      2
    );
  }

  private getMarkerText(
    type: 'Start' | 'Waypoint' | 'Destination',
    point: GeoPoint,
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
          class="custom-map-marker-inner"
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

  private createMarker(
    point: GeoPoint,
    color: string,
    tooltip: string,
    debugName: string
  ): L.Marker {
    const markerId = ++this.markerSequence;

    const marker = L.marker([point.lat, point.lng], {
      icon: this.createColoredIcon(color),
      bubblingMouseEvents: false,
      interactive: true,
      riseOnHover: true
    });

    marker.bindTooltip(tooltip, {
      direction: 'top'
    });

    marker.on('add', () => {
      this.log(`MARKER ${debugName}#${markerId} add`, { point });
    });

    marker.on('remove', () => {
      this.log(`MARKER ${debugName}#${markerId} remove`, { point });
    });

    marker.on('mouseover', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent(`MARKER ${debugName}#${markerId} mouseover`, event);
    });

    marker.on('mouseout', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent(`MARKER ${debugName}#${markerId} mouseout`, event);
    });

    marker.on('mousedown', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent(`MARKER ${debugName}#${markerId} mousedown`, event);
    });

    marker.on('mouseup', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent(`MARKER ${debugName}#${markerId} mouseup`, event);
    });

    marker.on('click', (event: L.LeafletMouseEvent) => {
      this.logLeafletEvent(`MARKER ${debugName}#${markerId} click`, event);
    });

    marker.on('tooltipopen', () => {
      this.log(`MARKER ${debugName}#${markerId} tooltipopen`);
    });

    marker.on('tooltipclose', () => {
      this.log(`MARKER ${debugName}#${markerId} tooltipclose`);
    });

    return marker;
  }

  private refreshLayers(): void {
    this.log('refreshLayers START', {
      startPoint: this.startPoint,
      destinationPoint: this.destinationPoint,
      waypoints: [...this.waypoints]
    });

    this.layers = [];

    if (!this.overlayGroup) {
      this.log('refreshLayers aborted: no overlayGroup');
      return;
    }

    this.overlayGroup.clearLayers();

    if (this.startPoint) {
      const startMarker = this.createMarker(
        this.startPoint,
        '#2e7d32',
        this.getMarkerText('Start', this.startPoint),
        'START'
      );

      this.overlayGroup.addLayer(startMarker);
      this.layers.push(startMarker);
    }

    this.waypoints.forEach((wp: GeoPoint, index: number) => {
      const waypointMarker = this.createMarker(
        wp,
        '#1976d2',
        this.getMarkerText('Waypoint', wp, index),
        `WAYPOINT[${index}]`
      );

      waypointMarker.on('click', () => {
        this.log(`WAYPOINT[${index}] remove requested`, {
          before: [...this.waypoints]
        });

        this.ngZone.run(() => {
          this.waypoints.splice(index, 1);

          this.log(`WAYPOINT[${index}] removed`, {
            after: [...this.waypoints]
          });

          this.refreshLayers();
        });
      });

      this.overlayGroup?.addLayer(waypointMarker);
      this.layers.push(waypointMarker);
    });

    if (this.destinationPoint) {
      const destinationMarker = this.createMarker(
        this.destinationPoint,
        '#f9a825',
        this.getMarkerText('Destination', this.destinationPoint),
        'DESTINATION'
      );

      this.overlayGroup.addLayer(destinationMarker);
      this.layers.push(destinationMarker);
    }

    const path: [number, number][] = [];

    if (this.startPoint) {
      path.push([this.startPoint.lat, this.startPoint.lng]);
    }

    this.waypoints.forEach((wp: GeoPoint) => {
      path.push([wp.lat, wp.lng]);
    });

    if (this.destinationPoint) {
      path.push([this.destinationPoint.lat, this.destinationPoint.lng]);
    }

    if (path.length >= 2) {
      const polyline = L.polyline(path);
      this.overlayGroup.addLayer(polyline);
      this.layers.push(polyline);
    }

    this.log('refreshLayers END', {
      renderedLayers: this.layers.length
    });
  }

  private log(message: string, data?: unknown): void {
    if (!this.debugEnabled) {
      return;
    }

    if (data === undefined) {
      console.log(`[Mapmodule DEBUG] ${message}`);
      return;
    }

    console.log(`[Mapmodule DEBUG] ${message}`, data);
  }

  private logLeafletEvent(label: string, event: L.LeafletMouseEvent): void {
    if (!this.debugEnabled) {
      return;
    }

    const target = event.originalEvent?.target as HTMLElement | null;

    console.log(`[Mapmodule DEBUG] ${label}`, {
      latlng: event.latlng ? {
        lat: event.latlng.lat,
        lng: event.latlng.lng
      } : null,
      originalEventType: event.originalEvent?.type ?? null,
      targetTag: target?.tagName ?? null,
      targetClass: target?.className ?? null,
      targetHtml: target?.outerHTML?.slice(0, 200) ?? null
    });
  }
}