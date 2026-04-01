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
    this.applyCountryToMap();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['country']) {
      this.applyCountryToMap();
    }

    if (changes['visible'] && this.visible) {
      this.initializeOrRefreshMap();
    }
  }

  ngAfterViewInit(): void {
    this.setupResizeObserver();

    setTimeout(() => {
      this.initializeOrRefreshMap();
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

  public save(): void {
    this.saveRoute.emit(this.getOutput());
  }

  public refreshMapSize(): void {
    if (!this.map) return;

    this.map.invalidateSize();
    window.setTimeout(() => this.map?.invalidateSize(), 100);
    window.setTimeout(() => this.map?.invalidateSize(), 250);
    window.setTimeout(() => this.map?.invalidateSize(), 500);
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
    if (!container) return;

    this.resizeObserver?.disconnect();

    this.resizeObserver = new ResizeObserver(() => {
      this.refreshMapSize();
    });

    this.resizeObserver.observe(container);
  }

  private getCountryMapConfig(country: string): { center: L.LatLngExpression; zoom: number } {
    const normalized = (country || '').trim().toLowerCase();

    switch (normalized) {
      case 'portugal':
        return { center: L.latLng(39.63, -8.09), zoom: 8 };
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
    if (!container) return;

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

    this.tryRemoveMarkerNear(clickedLat, clickedLng);
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
      this.locationSearch = '';
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
      this.locationSearch = '';
      this.refreshLayers();
      return true;
    }

    const index = this.waypoints.findIndex(
      w =>
        Math.abs(w.lat - lat) <= tolerance &&
        Math.abs(w.lng - lng) <= tolerance
    );

    if (index >= 0) {
      this.waypoints.splice(index, 1);
      this.refreshLayers();
      return true;
    }

    return false;
  }

  public getOutput(): string {
    const waypointsObject: Record<string, GeoPoint> = {};

    this.waypoints.forEach((wp, index) => {
      waypointsObject[String(index + 1)] = wp;
    });

    return JSON.stringify(
      {
        start: this.startPoint,
        waypoints: waypointsObject,
        destination: this.destinationPoint
      },
      null,
      2
    );
  }

  private refreshLayers(): void {
    if (!this.overlayGroup) return;

    this.overlayGroup.clearLayers();

    if (this.startPoint) {
      this.overlayGroup.addLayer(L.marker([this.startPoint.lat, this.startPoint.lng]));
    }

    this.waypoints.forEach(wp => {
      this.overlayGroup?.addLayer(L.marker([wp.lat, wp.lng]));
    });

    if (this.destinationPoint) {
      this.overlayGroup.addLayer(L.marker([this.destinationPoint.lat, this.destinationPoint.lng]));
    }
  }
}