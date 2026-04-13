import { DialogshellComponent } from '../dialogshell/dialogshell.component';
import { Inject, Optional } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef, MatDialogModule } from '@angular/material/dialog';
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

interface MapDialogData {
  country: string;
  routeData: RoutePayload | null;
}

@Component({
  selector: 'app-mapmodule',
  standalone: true,
  imports: [
    CommonModule, 
    FormsModule,
    MatDialogModule,
    DialogshellComponent
  ],
  templateUrl: './mapmodule.component.html',
  styleUrls: ['./mapmodule.component.css']
})
export class MapmoduleComponent implements OnInit, OnChanges, AfterViewInit, OnDestroy {
  @Input() country: string = '';
  @Input() visible: boolean = true;
  @Input() routeData: RoutePayload | null = null;

  @Output() saveRoute = new EventEmitter<string>();

  @ViewChild('mapContainer', { static: false })
  private mapContainer?: ElementRef<HTMLDivElement>;

  private resizeObserver?: ResizeObserver;
  private mapReady = false;
  private contextMenuHandler?: (event: MouseEvent) => void;
  private overlayGroup: L.LayerGroup | null = null;

  private readonly defaultCountry: string = 'Portugal';

  constructor(
    private readonly ngZone: NgZone,
    private readonly http: HttpClient,
    @Optional() private readonly dialogRef?: MatDialogRef<MapmoduleComponent>,
    @Optional() @Inject(MAT_DIALOG_DATA) private readonly dialogData?: MapDialogData
  ) {}

  public locationSearch = '';
  public startFrom = '';
  public destination = '';
  public waypoints: GeoPoint[] = [];

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
    if (this.dialogData) {
      if (this.dialogData.country) {
        this.country = this.dialogData.country;
      }

      if (this.dialogData.routeData) {
        this.routeData = this.dialogData.routeData;
      }
    }

    this.applyCountryToMap();
    this.applyRouteDataFromInput();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['country']) {
      this.applyCountryToMap();
    }

    if (changes['routeData']) {
      this.applyRouteDataFromInput();

      setTimeout(() => {
        this.fitMapToRoute();
      }, 0);
    }

    if (changes['visible'] && this.visible) {
      this.initializeOrRefreshMap();

      setTimeout(() => {
        this.applyRouteDataFromInput();
        this.fitMapToRoute();
      }, 0);
    }
  }

  ngAfterViewInit(): void {
    this.setupResizeObserver();

    setTimeout(() => {
      this.initializeOrRefreshMap();
      this.applyRouteDataFromInput();
      this.fitMapToRoute();
    }, 0);
  }

  ngOnDestroy(): void {
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

  save(): void {
    const payload = {
      start: this.startPoint,
      waypoints: this.waypoints,
      destination: this.destinationPoint
    };

    const routeJson = JSON.stringify(payload);

    if (this.dialogRef) {
      this.dialogRef.close(routeJson);
      return;
    }

    this.saveRoute.emit(routeJson);
  }

  get isDialogMode(): boolean {
    return !!this.dialogRef;
  }

  closeDialog(): void {
    if (this.dialogRef) {
      this.dialogRef.close();
    }
  }

  public refreshMapSize(): void {
    if (!this.map) {
      return;
    }

    this.map.invalidateSize();
    window.setTimeout(() => this.map?.invalidateSize(), 100);
    window.setTimeout(() => this.map?.invalidateSize(), 250);
    window.setTimeout(() => this.map?.invalidateSize(), 500);
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

    this.refreshLayers();
  }

  public onMapRightClick(event: L.LeafletMouseEvent): void {
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

  public setSelectionMode(mode: PointSelectionMode): void {
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

  private initializeOrRefreshMap(): void {
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
      this.refreshMapSize();
    });

    this.resizeObserver.observe(container);
  }

  private getCountryMapConfig(country: string): { center: L.LatLngExpression; zoom: number } {
    const normalized = (country || '').trim().toLowerCase();

    switch (normalized) {
      case 'afghanistan':
        return { center: L.latLng(33.83, 65.31), zoom: 6 };

      case 'albania':
        return { center: L.latLng(41.25, 19.97), zoom: 8 };

      case 'algeria':
        return { center: L.latLng(27.92, 0.53), zoom: 5 };

      case 'andorra':
        return { center: L.latLng(42.55, 1.58), zoom: 10 };

      case 'angola':
        return { center: L.latLng(-11.88, 18.81), zoom: 6 };

      case 'antigua and barbuda':
        return { center: L.latLng(17.11, -61.85), zoom: 9 };

      case 'argentina':
        return { center: L.latLng(-37.24, -64.08), zoom: 5 };

      case 'armenia':
        return { center: L.latLng(39.95, 45.06), zoom: 8 };

      case 'australia':
        return { center: L.latLng(-24.84, 133.06), zoom: 5 };

      case 'austria':
        return { center: L.latLng(47.92, 14.95), zoom: 7 };

      case 'azerbaijan':
        return { center: L.latLng(40.04, 47.64), zoom: 7 };

      case 'bahamas':
        return { center: L.latLng(26.69, -78.4), zoom: 8 };

      case 'bahrain':
        return { center: L.latLng(26.07, 50.55), zoom: 9 };

      case 'bangladesh':
        return { center: L.latLng(23.56, 89.88), zoom: 7 };

      case 'barbados':
        return { center: L.latLng(13.19, -59.54), zoom: 10 };

      case 'belarus':
        return { center: L.latLng(53.71, 27.79), zoom: 7 };

      case 'belgium':
        return { center: L.latLng(50.58, 4.74), zoom: 8 };

      case 'belize':
        return { center: L.latLng(17.31, -88.7), zoom: 8 };

      case 'benin':
        return { center: L.latLng(9.24, 2.29), zoom: 7 };

      case 'bhutan':
        return { center: L.latLng(27.61, 90.5), zoom: 8 };

      case 'bolivia':
        return { center: L.latLng(-16.4, -63.64), zoom: 6 };

      case 'bosnia and herzegovina':
        return { center: L.latLng(43.85, 18.12), zoom: 8 };

      case 'botswana':
        return { center: L.latLng(-22.46, 24.31), zoom: 7 };

      case 'brazil':
        return { center: L.latLng(-14.07, -49.71), zoom: 5 };

      case 'brunei':
        return { center: L.latLng(4.71, 114.89), zoom: 9 };

      case 'bulgaria':
        return { center: L.latLng(42.74, 25.14), zoom: 7 };

      case 'burkina faso':
        return { center: L.latLng(12.24, -1.28), zoom: 7 };

      case 'burundi':
        return { center: L.latLng(-3.46, 29.96), zoom: 8 };

      case 'cabo verde':
        return { center: L.latLng(15.12, -23.61), zoom: 7 };

      case 'cambodia':
        return { center: L.latLng(12.87, 105.0), zoom: 7 };

      case 'cameroon':
        return { center: L.latLng(7.23, 13.49), zoom: 6 };

      case 'canada':
        return { center: L.latLng(56.7, -110.24), zoom: 4 };

      case 'central african republic':
        return { center: L.latLng(6.76, 20.48), zoom: 6 };

      case 'chad':
        return { center: L.latLng(15.28, 18.31), zoom: 6 };

      case 'chile':
        return { center: L.latLng(-54.02, -69.84), zoom: 5 };

      case 'china':
        return { center: L.latLng(36.8, 98.77), zoom: 4 };

      case 'colombia':
        return { center: L.latLng(3.97, -72.49), zoom: 6 };

      case 'comoros':
        return { center: L.latLng(-11.88, 43.87), zoom: 8 };

      case 'congo':
        return { center: L.latLng(-0.65, 15.94), zoom: 7 };

      case 'costa rica':
        return { center: L.latLng(9.71, -83.68), zoom: 8 };

      case 'croatia':
        return { center: L.latLng(44.54, 15.58), zoom: 7 };

      case 'cuba':
        return { center: L.latLng(21.39, -77.7), zoom: 6 };

      case 'cyprus':
        return { center: L.latLng(34.84, 33.03), zoom: 9 };

      case 'czechia':
        return { center: L.latLng(49.76, 15.54), zoom: 7 };

      case 'denmark':
        return { center: L.latLng(56.32, 9.46), zoom: 8 };

      case 'djibouti':
        return { center: L.latLng(11.86, 42.41), zoom: 9 };

      case 'dominica':
        return { center: L.latLng(15.41, -61.37), zoom: 10 };

      case 'dominican republic':
        return { center: L.latLng(18.7, -70.13), zoom: 8 };

      case 'ecuador':
        return { center: L.latLng(-1.76, -78.28), zoom: 7 };

      case 'egypt':
        return { center: L.latLng(26.9, 29.37), zoom: 6 };

      case 'el salvador':
        return { center: L.latLng(13.82, -88.92), zoom: 8 };

      case 'equatorial guinea':
        return { center: L.latLng(1.71, 10.38), zoom: 9 };

      case 'eritrea':
        return { center: L.latLng(15.2, 38.3), zoom: 7 };

      case 'estonia':
        return { center: L.latLng(58.5, 25.56), zoom: 8 };

      case 'eswatini':
        return { center: L.latLng(-26.57, 31.36), zoom: 9 };

      case 'ethiopia':
        return { center: L.latLng(9.36, 38.73), zoom: 6 };

      case 'fiji':
        return { center: L.latLng(-17.94, 177.98), zoom: 3 };

      case 'finland':
        return { center: L.latLng(65.03, 27.37), zoom: 6 };

      case 'france':
        return { center: L.latLng(46.9, 2.1), zoom: 4 };

      case 'gabon':
        return { center: L.latLng(-0.95, 11.59), zoom: 7 };

      case 'gambia':
        return { center: L.latLng(13.4, -16.04), zoom: 8 };

      case 'georgia':
        return { center: L.latLng(42.3, 43.6), zoom: 7 };

      case 'germany':
        return { center: L.latLng(51.13, 10.4), zoom: 6 };

      case 'ghana':
        return { center: L.latLng(7.95, -1.22), zoom: 7 };

      case 'greece':
        return { center: L.latLng(39.07, 22.96), zoom: 6 };

      case 'grenada':
        return { center: L.latLng(12.12, -61.68), zoom: 10 };

      case 'guatemala':
        return { center: L.latLng(15.78, -90.23), zoom: 7 };

      case 'guinea':
        return { center: L.latLng(10.43, -10.94), zoom: 7 };

      case 'guinea-bissau':
        return { center: L.latLng(12.05, -14.95), zoom: 8 };

      case 'guyana':
        return { center: L.latLng(4.79, -58.98), zoom: 7 };

      case 'haiti':
        return { center: L.latLng(18.92, -72.68), zoom: 8 };

      case 'honduras':
        return { center: L.latLng(14.82, -86.62), zoom: 7 };

      case 'hungary':
        return { center: L.latLng(47.16, 19.4), zoom: 7 };

      case 'iceland':
        return { center: L.latLng(64.91, -18.57), zoom: 6 };

      case 'india':
        return { center: L.latLng(22.72, 79.59), zoom: 5 };

      case 'indonesia':
        return { center: L.latLng(-2.22, 117.24), zoom: 4 };

      case 'iran':
        return { center: L.latLng(32.58, 54.29), zoom: 5 };

      case 'iraq':
        return { center: L.latLng(33.02, 43.79), zoom: 6 };

      case 'ireland':
        return { center: L.latLng(53.18, -8.14), zoom: 7 };

      case 'israel':
        return { center: L.latLng(31.42, 35.07), zoom: 8 };

      case 'italy':
        return { center: L.latLng(42.75, 12.34), zoom: 6 };

      case 'jamaica':
        return { center: L.latLng(18.16, -77.32), zoom: 9 };

      case 'japan':
        return { center: L.latLng(36.08, 138.25), zoom: 6 };

      case 'jordan':
        return { center: L.latLng(31.24, 36.79), zoom: 7 };

      case 'kazakhstan':
        return { center: L.latLng(48.19, 67.28), zoom: 4 };

      case 'kenya':
        return { center: L.latLng(0.34, 37.8), zoom: 6 };

      case 'kiribati':
        return { center: L.latLng(1.87, -157.36), zoom: 5 };

      case 'kuwait':
        return { center: L.latLng(29.33, 47.59), zoom: 8 };

      case 'kyrgyzstan':
        return { center: L.latLng(41.51, 74.77), zoom: 7 };

      case 'laos':
        return { center: L.latLng(18.44, 103.75), zoom: 7 };

      case 'latvia':
        return { center: L.latLng(56.81, 24.83), zoom: 8 };

      case 'lebanon':
        return { center: L.latLng(34.18, 35.87), zoom: 9 };

      case 'lesotho':
        return { center: L.latLng(-29.58, 28.23), zoom: 8 };

      case 'liberia':
        return { center: L.latLng(6.47, -9.32), zoom: 7 };

      case 'libya':
        return { center: L.latLng(27.0, 18.01), zoom: 5 };

      case 'liechtenstein':
        return { center: L.latLng(47.17, 9.56), zoom: 10 };

      case 'lithuania':
        return { center: L.latLng(55.29, 23.89), zoom: 8 };

      case 'luxembourg':
        return { center: L.latLng(49.77, 6.09), zoom: 9 };

      case 'madagascar':
        return { center: L.latLng(-19.37, 46.7), zoom: 6 };

      case 'malawi':
        return { center: L.latLng(-13.58, 34.12), zoom: 7 };

      case 'malaysia':
        return { center: L.latLng(4.88, 114.93), zoom: 5 };

      case 'maldives':
        return { center: L.latLng(3.2, 73.22), zoom: 7 };

      case 'mali':
        return { center: L.latLng(17.57, -1.99), zoom: 6 };

      case 'malta':
        return { center: L.latLng(35.94, 14.38), zoom: 10 };

      case 'marshall islands':
        return { center: L.latLng(7.13, 171.18), zoom: 6 };

      case 'mauritania':
        return { center: L.latLng(20.26, -10.35), zoom: 6 };

      case 'mauritius':
        return { center: L.latLng(-20.28, 57.55), zoom: 9 };

      case 'mexico':
        return { center: L.latLng(23.94, -102.58), zoom: 5 };

      case 'micronesia':
        return { center: L.latLng(7.43, 150.55), zoom: 6 };

      case 'moldova':
        return { center: L.latLng(47.2, 28.47), zoom: 8 };

      case 'monaco':
        return { center: L.latLng(43.74, 7.42), zoom: 12 };

      case 'mongolia':
        return { center: L.latLng(46.69, 104.3), zoom: 5 };

      case 'montenegro':
        return { center: L.latLng(42.78, 19.24), zoom: 9 };

      case 'morocco':
        return { center: L.latLng(29.84, -8.84), zoom: 6 };

      case 'mozambique':
        return { center: L.latLng(-18.98, 35.53), zoom: 6 };

      case 'myanmar':
        return { center: L.latLng(20.33, 96.48), zoom: 6 };

      case 'namibia':
        return { center: L.latLng(-22.22, 18.33), zoom: 6 };

      case 'nauru':
        return { center: L.latLng(-0.52, 166.93), zoom: 11 };

      case 'nepal':
        return { center: L.latLng(28.27, 83.94), zoom: 7 };

      case 'netherlands':
        return { center: L.latLng(52.3, 5.51), zoom: 8 };

      case 'new zealand':
        return { center: L.latLng(-42.53, 171.4), zoom: 6 };

      case 'nicaragua':
        return { center: L.latLng(12.89, -84.9), zoom: 7 };

      case 'niger':
        return { center: L.latLng(17.61, 9.4), zoom: 6 };

      case 'nigeria':
        return { center: L.latLng(7.9, 8.09), zoom: 6 };

      case 'north korea':
        return { center: L.latLng(40.15, 127.18), zoom: 7 };

      case 'north macedonia':
        return { center: L.latLng(41.83, 21.75), zoom: 8 };

      case 'norway':
        return { center: L.latLng(63.4, 10.54), zoom: 5 };

      case 'oman':
        return { center: L.latLng(20.63, 56.27), zoom: 7 };

      case 'pakistan':
        return { center: L.latLng(28.96, 66.44), zoom: 5 };

      case 'palau':
        return { center: L.latLng(7.5, 134.58), zoom: 8 };

      case 'palestine':
        return { center: L.latLng(31.95, 35.2), zoom: 9 };

      case 'panama':
        return { center: L.latLng(8.52, -80.12), zoom: 8 };

      case 'papua new guinea':
        return { center: L.latLng(-6.14, 145.08), zoom: 6 };

      case 'paraguay':
        return { center: L.latLng(-22.01, -60.47), zoom: 6 };

      case 'peru':
        return { center: L.latLng(-9.15, -74.38), zoom: 5 };

      case 'philippines':
        return { center: L.latLng(11.78, 122.88), zoom: 5 };

      case 'poland':
        return { center: L.latLng(52.22, 19.43), zoom: 7 };

      case 'portugal':
        return { center: L.latLng(39.63, -8.09), zoom: 8 };

      case 'qatar':
        return { center: L.latLng(25.24, 51.18), zoom: 8 };

      case 'romania':
        return { center: L.latLng(45.85, 24.97), zoom: 7 };

      case 'russia':
        return { center: L.latLng(61.98, 96.69), zoom: 3 };

      case 'rwanda':
        return { center: L.latLng(-1.88, 29.91), zoom: 9 };

      case 'saint kitts and nevis':
        return { center: L.latLng(17.33, -62.75), zoom: 10 };

      case 'saint lucia':
        return { center: L.latLng(13.91, -60.98), zoom: 10 };

      case 'saint vincent and the grenadines':
        return { center: L.latLng(13.25, -61.2), zoom: 10 };

      case 'samoa':
        return { center: L.latLng(-13.76, -172.1), zoom: 8 };

      case 'san marino':
        return { center: L.latLng(43.94, 12.46), zoom: 11 };

      case 'sao tome and principe':
        return { center: L.latLng(0.21, 6.73), zoom: 8 };

      case 'saudi arabia':
        return { center: L.latLng(23.54, 45.08), zoom: 5 };

      case 'senegal':
        return { center: L.latLng(14.65, -14.48), zoom: 7 };

      case 'serbia':
        return { center: L.latLng(44.2, 20.79), zoom: 7 };

      case 'seychelles':
        return { center: L.latLng(-4.68, 55.49), zoom: 8 };

      case 'sierra leone':
        return { center: L.latLng(8.64, -11.84), zoom: 8 };

      case 'singapore':
        return { center: L.latLng(1.35, 103.82), zoom: 10 };

      case 'slovakia':
        return { center: L.latLng(48.71, 19.7), zoom: 8 };

      case 'slovenia':
        return { center: L.latLng(46.12, 14.82), zoom: 8 };

      case 'solomon islands':
        return { center: L.latLng(-9.65, 160.16), zoom: 6 };

      case 'somalia':
        return { center: L.latLng(4.75, 46.7), zoom: 5 };

      case 'south africa':
        return { center: L.latLng(-29.26, 24.03), zoom: 5 };

      case 'south korea':
        return { center: L.latLng(36.39, 128.0), zoom: 7 };

      case 'south sudan':
        return { center: L.latLng(7.86, 29.69), zoom: 6 };

      case 'spain':
        return { center: L.latLng(40.35, -3.62), zoom: 6 };

      case 'sri lanka':
        return { center: L.latLng(7.61, 80.7), zoom: 8 };

      case 'sudan':
        return { center: L.latLng(14.03, 29.94), zoom: 5 };

      case 'suriname':
        return { center: L.latLng(4.13, -55.91), zoom: 7 };

      case 'sweden':
        return { center: L.latLng(62.95, 16.6), zoom: 5 };

      case 'switzerland':
        return { center: L.latLng(46.8, 8.23), zoom: 8 };

      case 'syria':
        return { center: L.latLng(35.02, 38.51), zoom: 7 };

      case 'tajikistan':
        return { center: L.latLng(38.53, 70.37), zoom: 7 };

      case 'tanzania':
        return { center: L.latLng(-6.28, 34.81), zoom: 6 };

      case 'thailand':
        return { center: L.latLng(15.12, 101.0), zoom: 7 };

      case 'timor-leste':
        return { center: L.latLng(-8.85, 125.95), zoom: 9 };

      case 'togo':
        return { center: L.latLng(8.62, 0.96), zoom: 8 };

      case 'tonga':
        return { center: L.latLng(-21.18, -175.2), zoom: 8 };

      case 'trinidad and tobago':
        return { center: L.latLng(10.43, -61.27), zoom: 9 };

      case 'tunisia':
        return { center: L.latLng(34.12, 9.55), zoom: 7 };

      case 'turkey':
        return { center: L.latLng(39.11, 35.16), zoom: 5 };

      case 'turkmenistan':
        return { center: L.latLng(39.4, 59.37), zoom: 6 };

      case 'tuvalu':
        return { center: L.latLng(-7.11, 177.65), zoom: 9 };

      case 'uganda':
        return { center: L.latLng(1.4, 32.37), zoom: 7 };

      case 'ukraine':
        return { center: L.latLng(49.05, 31.38), zoom: 6 };

      case 'united arab emirates':
        return { center: L.latLng(24.29, 54.61), zoom: 8 };

      case 'united kingdom':
        return { center: L.latLng(54.7, -3.28), zoom: 6 };

      case 'united states':
        return { center: L.latLng(39.83, -98.58), zoom: 4 };

      case 'uruguay':
        return { center: L.latLng(-32.91, -55.81), zoom: 7 };

      case 'uzbekistan':
        return { center: L.latLng(40.49, 63.17), zoom: 6 };

      case 'vanuatu':
        return { center: L.latLng(-16.2, 167.84), zoom: 7 };

      case 'vatican city':
        return { center: L.latLng(41.9, 12.45), zoom: 13 };

      case 'venezuela':
        return { center: L.latLng(7.06, -66.24), zoom: 6 };

      case 'vietnam':
        return { center: L.latLng(16.7, 106.3), zoom: 6 };

      case 'yemen':
        return { center: L.latLng(15.91, 48.7), zoom: 6 };

      case 'zambia':
        return { center: L.latLng(-13.46, 27.77), zoom: 6 };

      case 'zimbabwe':
        return { center: L.latLng(-18.98, 29.85), zoom: 7 };

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

    if (this.map) {
      this.map.setView(config.center, config.zoom);
      this.refreshMapSize();
    }
  }

  private initializeMap(): void {
    if (this.mapReady) {
      this.refreshMapSize();
      return;
    }

    const container = this.mapContainer?.nativeElement;
    if (!container) {
      return;
    }

    this.map = L.map(container, {
      ...this.options,
      zoomControl: true
    });

    this.overlayGroup = L.layerGroup().addTo(this.map);
    this.mapReady = true;

    this.map.on('click', (event: L.LeafletMouseEvent) => {
      this.ngZone.run(() => {
        this.onMapClick(event);
      });
    });

    this.map.on('contextmenu', (event: L.LeafletMouseEvent) => {
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

  private applyRouteDataFromInput(): void {
    const route = this.routeData;

    if (!route) {
      this.startPoint = null;
      this.destinationPoint = null;
      this.startFrom = '';
      this.destination = '';
      this.waypoints = [];
      this.locationSearch = '';
      this.refreshLayers();
      return;
    }

    this.startPoint = this.isValidRoutePoint(route.start)
      ? {
          lat: route.start.lat,
          lng: route.start.lng,
          label: route.start.label
        }
      : null;

    this.destinationPoint = this.isValidRoutePoint(route.destination)
      ? {
          lat: route.destination.lat,
          lng: route.destination.lng,
          label: route.destination.label
        }
      : null;

    this.startFrom = this.startPoint?.label ?? '';
    this.destination = this.destinationPoint?.label ?? '';

    const orderedWaypointKeys = Object.keys(route.waypoints ?? {}).sort(
      (a, b) => Number(a) - Number(b)
    );

    this.waypoints = orderedWaypointKeys
      .map((key) => route.waypoints[key])
      .filter((point): point is RoutePoint => this.isValidRoutePoint(point))
      .map((point) => ({
        lat: point.lat,
        lng: point.lng,
        label: point.label
      }));

    if (this.pointSelectionMode === 'start') {
      this.locationSearch = this.startFrom;
    } else if (this.pointSelectionMode === 'destination') {
      this.locationSearch = this.destination;
    } else {
      this.locationSearch = '';
    }

    this.refreshLayers();
  }

  private isValidRoutePoint(value: unknown): value is RoutePoint {
    if (!value || typeof value !== 'object') {
      return false;
    }

    const point = value as Record<string, unknown>;

    return (
      typeof point['lat'] === 'number' &&
      Number.isFinite(point['lat']) &&
      typeof point['lng'] === 'number' &&
      Number.isFinite(point['lng'])
    );
  }

  private fitMapToRoute(): void {
    if (!this.map) {
      return;
    }

    const points: L.LatLngExpression[] = [];

    if (this.startPoint) {
      points.push([this.startPoint.lat, this.startPoint.lng]);
    }

    for (const waypoint of this.waypoints) {
      points.push([waypoint.lat, waypoint.lng]);
    }

    if (this.destinationPoint) {
      points.push([this.destinationPoint.lat, this.destinationPoint.lng]);
    }

    if (points.length === 0) {
      return;
    }

    if (points.length === 1) {
      this.map.setView(points[0], 13);
      this.refreshMapSize();
      return;
    }

    const bounds = L.latLngBounds(points);
    this.map.fitBounds(bounds, { padding: [30, 30] });
    this.refreshMapSize();
  }

  private tryRemoveMarkerNear(lat: number, lng: number): boolean {
    const tolerance = 0.0008;

    if (
      this.startPoint &&
      Math.abs(this.startPoint.lat - lat) <= tolerance &&
      Math.abs(this.startPoint.lng - lng) <= tolerance
    ) {
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
      this.waypoints.splice(waypointIndex, 1);
      this.refreshLayers();
      return true;
    }

    return false;
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
    tooltip: string
  ): L.Marker {
    const marker = L.marker([point.lat, point.lng], {
      icon: this.createColoredIcon(color),
      bubblingMouseEvents: false,
      interactive: true,
      riseOnHover: true
    });

    marker.bindTooltip(tooltip, {
      direction: 'top'
    });

    return marker;
  }

  private refreshLayers(): void {
    this.layers = [];

    if (!this.overlayGroup) {
      return;
    }

    this.overlayGroup.clearLayers();

    if (this.startPoint) {
      const startMarker = this.createMarker(
        this.startPoint,
        '#2e7d32',
        this.getMarkerText('Start', this.startPoint)
      );

      this.overlayGroup.addLayer(startMarker);
      this.layers.push(startMarker);
    }

    this.waypoints.forEach((wp: GeoPoint, index: number) => {
      const waypointMarker = this.createMarker(
        wp,
        '#1976d2',
        this.getMarkerText('Waypoint', wp, index)
      );

      waypointMarker.on('click', () => {
        this.ngZone.run(() => {
          this.waypoints.splice(index, 1);
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
        this.getMarkerText('Destination', this.destinationPoint)
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
  }
}