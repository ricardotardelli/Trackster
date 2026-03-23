import { Component, OnInit, NgZone } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import * as L from 'leaflet';

type PointSelectionMode = 'start' | 'destination' | 'waypoints';

interface GeoPoint {
  lat: number;
  lng: number;
}

@Component({
  selector: 'app-mapmodule',
  templateUrl: './mapmodule.component.html',
  styleUrls: ['./mapmodule.component.css']
})
export class MapmoduleComponent implements OnInit {
  ngOnInit(): void { }
  constructor(
    private ngZone: NgZone,
    private http: HttpClient
  ) {}

  public startFrom = '';
  public destination = '';
  public waypoints: GeoPoint[] = [];
  private suppressNextMapClick = false;

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

  public testEvent(source: string): void {
    console.log('EVENT FIRED:', source);
  }

  public searchAddress(query: string, target: 'start' | 'destination'): void {
    console.log('searchAddress called:', query, target);

    const trimmedQuery = query?.trim();

    if (!trimmedQuery) {
      console.log('Empty query');
      return;
    }

    const params = new HttpParams()
      .set('q', trimmedQuery)
      .set('format', 'jsonv2')
      .set('limit', '1');

    this.http
      .get<any[]>('https://nominatim.openstreetmap.org/search', { params })
      .subscribe({
        next: (results) => {
          console.log('Search results:', results);

          if (!results || results.length === 0) {
            return;
          }

          const firstResult = results[0];
          const point: GeoPoint = {
            lat: Number(firstResult.lat),
            lng: Number(firstResult.lon)
          };

          if (target === 'start') {
            this.startPoint = point;
            this.startFrom = firstResult.display_name ?? trimmedQuery;
          } else {
            this.destinationPoint = point;
            this.destination = firstResult.display_name ?? trimmedQuery;
          }

          this.refreshLayers();

          if (this.map) {
            this.map.setView([point.lat, point.lng], 13);
          }
        },
        error: (error) => {
          console.error('Address search failed:', error);
        }
      });
  }

  public onMapReady(map: L.Map): void {
    this.map = map;
  }

  private removeWaypoint(index: number): void {
    this.waypoints.splice(index, 1);
    this.refreshLayers();
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
      iconAnchor: [9, 9],
      popupAnchor: [0, -10]
    });
  }

  public onMapClick(event: L.LeafletMouseEvent): void {
    const point: GeoPoint = {
      lat: Number(event.latlng.lat.toFixed(6)),
      lng: Number(event.latlng.lng.toFixed(6))
    };

    if (this.pointSelectionMode === 'start') {
      this.startPoint = point;

      if (!this.startFrom) {
        this.startFrom = `Start (${point.lat}, ${point.lng})`;
      }

    } else if (this.pointSelectionMode === 'destination') {
      this.destinationPoint = point;

      if (!this.destination) {
        this.destination = `Destination (${point.lat}, ${point.lng})`;
      }

    } else if (this.pointSelectionMode === 'waypoints') {
      this.waypoints.push(point);
    }

    this.refreshLayers();
  }

  public setSelectionMode(mode: PointSelectionMode): void {
    this.pointSelectionMode = mode;
  }

  public clearAll(): void {
    this.startFrom = '';
    this.destination = '';
    this.startPoint = null;
    this.destinationPoint = null;
    this.layers = [];
    this.waypoints = [];
  }

  public swapPoints(): void {
    const oldStartFrom = this.startFrom;
    const oldStartPoint = this.startPoint;

    this.startFrom = this.destination;
    this.startPoint = this.destinationPoint;

    this.destination = oldStartFrom;
    this.destinationPoint = oldStartPoint;

    this.refreshLayers();
  }

  public getOutput(): string {
    return JSON.stringify(
      {
        start: this.startPoint,
        destination: this.destinationPoint
      },
      null,
      2
    );
  }

  private refreshLayers(): void {
    const newLayers: L.Layer[] = [];

    // START
    if (this.startPoint) {
      const startMarker = L.marker(
        [this.startPoint.lat, this.startPoint.lng],
        {
          title: 'Start',
          icon: this.createColoredIcon('#2e7d32')
        }
      ).bindPopup('Start');

      newLayers.push(startMarker);
    }

    // WAYPOINTS
    this.waypoints.forEach((wp, index) => {
      const wpMarker = L.marker(
        [wp.lat, wp.lng],
        {
          title: `Waypoint ${index + 1}`,
          icon: this.createColoredIcon('#1976d2')
        }
      );

      wpMarker.bindPopup(`Waypoint ${index + 1}<br><small>Click to remove</small>`);

      wpMarker.on('click', (event: L.LeafletMouseEvent) => {
        L.DomEvent.stop(event.originalEvent);

        this.ngZone.run(() => {
          console.log('Removing waypoint', index);
          this.removeWaypoint(index);
        });
      });

      newLayers.push(wpMarker);
    });

    // DESTINATION
    if (this.destinationPoint) {
      const destinationMarker = L.marker(
        [this.destinationPoint.lat, this.destinationPoint.lng],
        {
          title: 'Destination',
          icon: this.createColoredIcon('#f9a825')
        }
      ).bindPopup('Destination');

      newLayers.push(destinationMarker);
    }

    // LINHA COMPLETA (Start → Waypoints → Destination)
    const path: [number, number][] = [];

    if (this.startPoint) {
      path.push([this.startPoint.lat, this.startPoint.lng]);
    }

    this.waypoints.forEach(wp => {
      path.push([wp.lat, wp.lng]);
    });

    if (this.destinationPoint) {
      path.push([this.destinationPoint.lat, this.destinationPoint.lng]);
    }

    if (path.length >= 2) {
      const line = L.polyline(path);
      newLayers.push(line);
    }

    this.layers = newLayers;
  }
  
}