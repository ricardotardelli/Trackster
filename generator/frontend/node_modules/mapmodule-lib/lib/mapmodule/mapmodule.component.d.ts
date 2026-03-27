import { OnInit, NgZone, OnChanges, SimpleChanges, EventEmitter } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import * as L from 'leaflet';
import * as i0 from "@angular/core";
type PointSelectionMode = 'start' | 'destination' | 'waypoints';
interface GeoPoint {
    lat: number;
    lng: number;
    label?: string;
}
export declare class MapmoduleComponent implements OnInit, OnChanges {
    private ngZone;
    private http;
    country: string;
    saveRoute: EventEmitter<string>;
    constructor(ngZone: NgZone, http: HttpClient);
    locationSearch: string;
    startFrom: string;
    destination: string;
    waypoints: GeoPoint[];
    private defaultCountry;
    startPoint: GeoPoint | null;
    destinationPoint: GeoPoint | null;
    pointSelectionMode: PointSelectionMode;
    save(): void;
    options: L.MapOptions;
    map: L.Map | null;
    layers: L.Layer[];
    ngOnInit(): void;
    ngOnChanges(changes: SimpleChanges): void;
    private logOutput;
    private getCountryMapConfig;
    private applyCountryToMap;
    getLocationPlaceholder(): string;
    searchSelectedAddress(): void;
    searchAddress(query: string, target: PointSelectionMode): void;
    onMapReady(map: L.Map): void;
    onMapClick(event: L.LeafletMouseEvent): void;
    setSelectionMode(mode: PointSelectionMode): void;
    clearAll(): void;
    getOutput(): string;
    private removeWaypoint;
    private getMarkerText;
    private createColoredIcon;
    private refreshLayers;
    static ɵfac: i0.ɵɵFactoryDeclaration<MapmoduleComponent, never>;
    static ɵcmp: i0.ɵɵComponentDeclaration<MapmoduleComponent, "app-mapmodule", never, { "country": { "alias": "country"; "required": false; }; }, { "saveRoute": "saveRoute"; }, never, never, false, never>;
}
export {};
