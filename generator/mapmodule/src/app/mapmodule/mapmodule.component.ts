import { Component, OnInit, NgZone } from '@angular/core';
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
  templateUrl: './mapmodule.component.html',
  styleUrls: ['./mapmodule.component.css']
})
export class MapmoduleComponent implements OnInit {
  constructor(
    private ngZone: NgZone,
    private http: HttpClient
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

  ngOnInit(): void {}

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
          }
        },
        error: (error: unknown) => {
          console.error('Address search failed:', error);
        }
      });
  }

  public onMapReady(map: L.Map): void {
    this.map = map;
  }

  public onMapClick(event: L.LeafletMouseEvent): void {
    const lat = Number(event.latlng.lat.toFixed(6));
    const lng = Number(event.latlng.lng.toFixed(6));

    const point: GeoPoint = {
      lat: lat,
      lng: lng,
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
  }

  public swapPoints(): void {
    const oldStartFrom = this.startFrom;
    const oldStartPoint = this.startPoint;

    this.startFrom = this.destination;
    this.startPoint = this.destinationPoint;

    this.destination = oldStartFrom;
    this.destinationPoint = oldStartPoint;

    if (this.pointSelectionMode === 'start') {
      this.locationSearch = this.startFrom;
    } else if (this.pointSelectionMode === 'destination') {
      this.locationSearch = this.destination;
    } else {
      this.locationSearch = '';
    }

    this.refreshLayers();
  }

  public getOutput(): string {
    return JSON.stringify(
      {
        start: this.startPoint,
        waypoints: this.waypoints,
        destination: this.destinationPoint
      },
      null,
      2
    );
  }

  private removeWaypoint(index: number): void {
    this.waypoints.splice(index, 1);
    this.refreshLayers();
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
      className: '',
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

  private refreshLayers(): void {
    const newLayers: L.Layer[] = [];

    if (this.startPoint) {
      const startMarker = L.marker(
        [this.startPoint.lat, this.startPoint.lng],
        {
          icon: this.createColoredIcon('#2e7d32')
        }
      );

      startMarker.bindTooltip(
        this.getMarkerText('Start', this.startPoint),
        { direction: 'top' }
      );

      newLayers.push(startMarker);
    }

    this.waypoints.forEach((wp: GeoPoint, index: number) => {
      const wpMarker = L.marker(
        [wp.lat, wp.lng],
        {
          icon: this.createColoredIcon('#1976d2')
        }
      );

      wpMarker.bindTooltip(
        this.getMarkerText('Waypoint', wp, index),
        { direction: 'top' }
      );

      wpMarker.on('click', () => {
        this.ngZone.run(() => {
          this.removeWaypoint(index);
        });
      });

      newLayers.push(wpMarker);
    });

    if (this.destinationPoint) {
      const destinationMarker = L.marker(
        [this.destinationPoint.lat, this.destinationPoint.lng],
        {
          icon: this.createColoredIcon('#f9a825')
        }
      );

      destinationMarker.bindTooltip(
        this.getMarkerText('Destination', this.destinationPoint),
        { direction: 'top' }
      );

      newLayers.push(destinationMarker);
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
      newLayers.push(L.polyline(path));
    }

    this.layers = newLayers;
  }
}