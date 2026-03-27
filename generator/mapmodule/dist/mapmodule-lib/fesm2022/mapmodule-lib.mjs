import * as i0 from '@angular/core';
import { EventEmitter, Output, Input, Component, NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import * as i2 from '@angular/forms';
import { FormsModule } from '@angular/forms';
import * as i3 from '@asymmetrik/ngx-leaflet';
import { LeafletModule } from '@asymmetrik/ngx-leaflet';
import * as i1 from '@angular/common/http';
import { HttpParams } from '@angular/common/http';
import * as L from 'leaflet';

class MapmoduleComponent {
    constructor(ngZone, http) {
        this.ngZone = ngZone;
        this.http = http;
        this.country = '';
        this.saveRoute = new EventEmitter();
        this.locationSearch = '';
        this.startFrom = '';
        this.destination = '';
        this.waypoints = [];
        this.defaultCountry = 'Portugal';
        this.startPoint = null;
        this.destinationPoint = null;
        this.pointSelectionMode = 'start';
        this.options = {
            layers: [
                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    attribution: '&copy; OpenStreetMap contributors'
                })
            ],
            zoom: 6,
            center: L.latLng(39.5, -8.0)
        };
        this.map = null;
        this.layers = [];
    }
    save() {
        const output = this.getOutput();
        console.log('Map output:', JSON.parse(output));
        this.saveRoute.emit(output);
    }
    ngOnInit() {
        this.applyCountryToMap();
    }
    ngOnChanges(changes) {
        if (changes['country']) {
            this.applyCountryToMap();
        }
    }
    logOutput() {
        console.log('Map output:', JSON.parse(this.getOutput()));
    }
    getCountryMapConfig(country) {
        const normalized = (country || '').trim().toLowerCase();
        switch (normalized) {
            case 'portugal':
                return {
                    center: L.latLng(39.5, -8.0),
                    zoom: 6
                };
            case 'spain':
                return {
                    center: L.latLng(40.2, -3.7),
                    zoom: 6
                };
            case 'france':
                return {
                    center: L.latLng(46.2, 2.2),
                    zoom: 6
                };
            case 'brazil':
                return {
                    center: L.latLng(-14.2, -51.9),
                    zoom: 4
                };
            case 'usa':
            case 'united states':
                return {
                    center: L.latLng(39.8, -98.6),
                    zoom: 4
                };
            default:
                return {
                    center: L.latLng(39.5, -8.0),
                    zoom: 6
                };
        }
    }
    applyCountryToMap() {
        const effectiveCountry = this.country && this.country.trim() !== ''
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
        }
    }
    getLocationPlaceholder() {
        if (this.pointSelectionMode === 'start') {
            return 'Select or type start point name';
        }
        if (this.pointSelectionMode === 'destination') {
            return 'Select or type destination name';
        }
        return 'Search and add waypoint';
    }
    searchSelectedAddress() {
        this.searchAddress(this.locationSearch, this.pointSelectionMode);
    }
    searchAddress(query, target) {
        const trimmedQuery = query ? query.trim() : '';
        if (!trimmedQuery) {
            return;
        }
        const params = new HttpParams()
            .set('q', trimmedQuery)
            .set('format', 'jsonv2')
            .set('limit', '1');
        this.http
            .get('https://nominatim.openstreetmap.org/search', { params })
            .subscribe({
            next: (results) => {
                if (!results || results.length === 0) {
                    return;
                }
                const firstResult = results[0];
                const resolvedName = firstResult.display_name || trimmedQuery;
                const point = {
                    lat: Number(firstResult.lat),
                    lng: Number(firstResult.lon),
                    label: resolvedName
                };
                if (target === 'start') {
                    this.startPoint = point;
                    this.startFrom = resolvedName;
                    this.locationSearch = resolvedName;
                }
                else if (target === 'destination') {
                    this.destinationPoint = point;
                    this.destination = resolvedName;
                    this.locationSearch = resolvedName;
                }
                else {
                    this.waypoints.push(point);
                    this.locationSearch = '';
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
    onMapReady(map) {
        this.map = map;
    }
    onMapClick(event) {
        const lat = Number(event.latlng.lat.toFixed(6));
        const lng = Number(event.latlng.lng.toFixed(6));
        const point = {
            lat: lat,
            lng: lng,
            label: `${lat}, ${lng}`
        };
        if (this.pointSelectionMode === 'start') {
            this.startPoint = point;
            this.startFrom = point.label || '';
            this.locationSearch = this.startFrom;
        }
        else if (this.pointSelectionMode === 'destination') {
            this.destinationPoint = point;
            this.destination = point.label || '';
            this.locationSearch = this.destination;
        }
        else {
            this.waypoints.push(point);
            this.locationSearch = '';
        }
        this.refreshLayers();
    }
    setSelectionMode(mode) {
        this.pointSelectionMode = mode;
        if (mode === 'start') {
            this.locationSearch = this.startFrom;
        }
        else if (mode === 'destination') {
            this.locationSearch = this.destination;
        }
        else {
            this.locationSearch = '';
        }
    }
    clearAll() {
        this.locationSearch = '';
        this.startFrom = '';
        this.destination = '';
        this.startPoint = null;
        this.destinationPoint = null;
        this.waypoints = [];
        this.layers = [];
    }
    getOutput() {
        const waypointsObject = {};
        this.waypoints.forEach((wp, index) => {
            waypointsObject[String(index + 1)] = {
                lat: wp.lat,
                lng: wp.lng,
                label: wp.label
            };
        });
        return JSON.stringify({
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
        }, null, 2);
    }
    removeWaypoint(index) {
        this.waypoints.splice(index, 1);
        this.refreshLayers();
    }
    getMarkerText(type, point, index) {
        const value = point.label ? point.label : `${point.lat}, ${point.lng}`;
        if (type === 'Waypoint') {
            const numberText = index !== undefined ? String(index + 1) : '?';
            return `Waypoint ${numberText}: ${value}`;
        }
        return `${type}: ${value}`;
    }
    createColoredIcon(color) {
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
    refreshLayers() {
        const newLayers = [];
        if (this.startPoint) {
            const startMarker = L.marker([this.startPoint.lat, this.startPoint.lng], {
                icon: this.createColoredIcon('#2e7d32')
            });
            startMarker.bindTooltip(this.getMarkerText('Start', this.startPoint), { direction: 'top' });
            newLayers.push(startMarker);
        }
        this.waypoints.forEach((wp, index) => {
            const wpMarker = L.marker([wp.lat, wp.lng], {
                icon: this.createColoredIcon('#1976d2')
            });
            wpMarker.bindTooltip(this.getMarkerText('Waypoint', wp, index), { direction: 'top' });
            wpMarker.on('click', () => {
                this.ngZone.run(() => {
                    this.removeWaypoint(index);
                });
            });
            newLayers.push(wpMarker);
        });
        if (this.destinationPoint) {
            const destinationMarker = L.marker([this.destinationPoint.lat, this.destinationPoint.lng], {
                icon: this.createColoredIcon('#f9a825')
            });
            destinationMarker.bindTooltip(this.getMarkerText('Destination', this.destinationPoint), { direction: 'top' });
            newLayers.push(destinationMarker);
        }
        const path = [];
        if (this.startPoint) {
            path.push([this.startPoint.lat, this.startPoint.lng]);
        }
        this.waypoints.forEach((wp) => {
            path.push([wp.lat, wp.lng]);
        });
        if (this.destinationPoint) {
            path.push([this.destinationPoint.lat, this.destinationPoint.lng]);
        }
        if (path.length >= 2) {
            newLayers.push(L.polyline(path));
        }
        this.layers = newLayers;
        this.logOutput();
    }
    static { this.ɵfac = i0.ɵɵngDeclareFactory({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleComponent, deps: [{ token: i0.NgZone }, { token: i1.HttpClient }], target: i0.ɵɵFactoryTarget.Component }); }
    static { this.ɵcmp = i0.ɵɵngDeclareComponent({ minVersion: "14.0.0", version: "17.3.12", type: MapmoduleComponent, selector: "app-mapmodule", inputs: { country: "country" }, outputs: { saveRoute: "saveRoute" }, usesOnChanges: true, ngImport: i0, template: "<div class=\"route-picker-container\">\r\n  <div class=\"controls-block\">\r\n    <div class=\"toolbar\">\r\n      <div class=\"field-group search-group\">\r\n        <label for=\"locationSearch\">Location</label>\r\n\r\n        <div class=\"search-row\">\r\n          <input\r\n            id=\"locationSearch\"\r\n            name=\"locationSearch\"\r\n            type=\"text\"\r\n            [(ngModel)]=\"locationSearch\"\r\n            [placeholder]=\"getLocationPlaceholder()\"\r\n            (keyup.enter)=\"searchSelectedAddress()\"\r\n            (blur)=\"searchSelectedAddress()\"\r\n          />\r\n\r\n          <button\r\n            type=\"button\"\r\n            class=\"primary\"\r\n            (click)=\"searchSelectedAddress()\">\r\n            Search\r\n          </button>\r\n        </div>\r\n      </div>\r\n    </div>\r\n\r\n    <div class=\"actions\">\r\n      <button\r\n        type=\"button\"\r\n        (click)=\"setSelectionMode('start')\"\r\n        [class.active]=\"pointSelectionMode === 'start'\">\r\n        Start Point\r\n      </button>\r\n\r\n      <button\r\n        type=\"button\"\r\n        (click)=\"setSelectionMode('waypoints')\"\r\n        [class.active]=\"pointSelectionMode === 'waypoints'\">\r\n        Waypoints\r\n      </button>\r\n\r\n      <button\r\n        type=\"button\"\r\n        (click)=\"setSelectionMode('destination')\"\r\n        [class.active]=\"pointSelectionMode === 'destination'\">\r\n        Destination\r\n      </button>\r\n\r\n      <button type=\"button\" (click)=\"clearAll()\" class=\"primary\">\r\n        Clear\r\n      </button>\r\n\r\n      <button type=\"button\" (click)=\"save()\" class=\"primary\">\r\n        Save\r\n      </button>\r\n    </div>\r\n  </div>\r\n\r\n  <div\r\n    class=\"map\"\r\n    leaflet\r\n    [leafletOptions]=\"options\"\r\n    [leafletLayers]=\"layers\"\r\n    (leafletMapReady)=\"onMapReady($event)\"\r\n    (leafletClick)=\"onMapClick($event)\">\r\n  </div>\r\n</div>", styles: [":host ::ng-deep .leaflet-control-attribution{display:none!important}.route-picker-container{width:33vw;min-width:560px;min-height:0;display:grid;grid-template-rows:auto minmax(220px,1fr) auto;gap:16px;padding:16px;box-sizing:border-box}.controls-block{display:grid;gap:12px;min-width:0}.toolbar{width:100%;min-width:0}.field-group{display:grid;gap:6px;min-width:0}.search-group{width:100%}.search-row{display:flex;gap:10px;align-items:center;width:fit-content}.search-row input{width:395px;min-width:0;height:30px;padding:0 12px;font-size:14px;box-sizing:border-box;border:1px solid #cfe0ff;border-radius:12px}.actions{display:flex;flex-wrap:wrap;gap:10px;align-items:center}.actions button,.search-row button{height:30px;padding:0 16px;border-radius:12px;border:1px solid #cfe0ff;background:#fff;cursor:pointer;white-space:nowrap;box-sizing:border-box;flex:0 0 auto}.actions button.active,.actions button.primary,.search-row button.primary{background:#1976d2;color:#fff;border-color:#1976d2}.actions button:disabled,.search-row button:disabled{opacity:.6;cursor:not-allowed}.map{width:100%;height:100%;min-height:380px;border:1px solid #ccc;border-radius:12px;overflow:hidden;box-sizing:border-box}.coordinates-panel{display:grid;grid-template-columns:1fr;gap:12px}.coord-block,.output-panel{border:1px solid #ddd;padding:12px;background:#fafafa;box-sizing:border-box}.output-panel{min-height:80px;max-height:120px;overflow:auto}.coord-block h3,.output-panel h3{margin:0 0 8px}pre{margin:0;white-space:pre-wrap;word-break:break-word}\n"], dependencies: [{ kind: "directive", type: i2.DefaultValueAccessor, selector: "input:not([type=checkbox])[formControlName],textarea[formControlName],input:not([type=checkbox])[formControl],textarea[formControl],input:not([type=checkbox])[ngModel],textarea[ngModel],[ngDefaultControl]" }, { kind: "directive", type: i2.NgControlStatus, selector: "[formControlName],[ngModel],[formControl]" }, { kind: "directive", type: i2.NgModel, selector: "[ngModel]:not([formControlName]):not([formControl])", inputs: ["name", "disabled", "ngModel", "ngModelOptions"], outputs: ["ngModelChange"], exportAs: ["ngModel"] }, { kind: "directive", type: i3.LeafletDirective, selector: "[leaflet]", inputs: ["leafletFitBoundsOptions", "leafletPanOptions", "leafletZoomOptions", "leafletZoomPanOptions", "leafletOptions", "leafletZoom", "leafletCenter", "leafletFitBounds", "leafletMaxBounds", "leafletMinZoom", "leafletMaxZoom"], outputs: ["leafletMapReady", "leafletZoomChange", "leafletCenterChange", "leafletClick", "leafletDoubleClick", "leafletMouseDown", "leafletMouseUp", "leafletMouseMove", "leafletMouseOver", "leafletMouseOut", "leafletMapMove", "leafletMapMoveStart", "leafletMapMoveEnd", "leafletMapZoom", "leafletMapZoomStart", "leafletMapZoomEnd"] }, { kind: "directive", type: i3.LeafletLayersDirective, selector: "[leafletLayers]", inputs: ["leafletLayers"] }] }); }
}
i0.ɵɵngDeclareClassMetadata({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleComponent, decorators: [{
            type: Component,
            args: [{ selector: 'app-mapmodule', template: "<div class=\"route-picker-container\">\r\n  <div class=\"controls-block\">\r\n    <div class=\"toolbar\">\r\n      <div class=\"field-group search-group\">\r\n        <label for=\"locationSearch\">Location</label>\r\n\r\n        <div class=\"search-row\">\r\n          <input\r\n            id=\"locationSearch\"\r\n            name=\"locationSearch\"\r\n            type=\"text\"\r\n            [(ngModel)]=\"locationSearch\"\r\n            [placeholder]=\"getLocationPlaceholder()\"\r\n            (keyup.enter)=\"searchSelectedAddress()\"\r\n            (blur)=\"searchSelectedAddress()\"\r\n          />\r\n\r\n          <button\r\n            type=\"button\"\r\n            class=\"primary\"\r\n            (click)=\"searchSelectedAddress()\">\r\n            Search\r\n          </button>\r\n        </div>\r\n      </div>\r\n    </div>\r\n\r\n    <div class=\"actions\">\r\n      <button\r\n        type=\"button\"\r\n        (click)=\"setSelectionMode('start')\"\r\n        [class.active]=\"pointSelectionMode === 'start'\">\r\n        Start Point\r\n      </button>\r\n\r\n      <button\r\n        type=\"button\"\r\n        (click)=\"setSelectionMode('waypoints')\"\r\n        [class.active]=\"pointSelectionMode === 'waypoints'\">\r\n        Waypoints\r\n      </button>\r\n\r\n      <button\r\n        type=\"button\"\r\n        (click)=\"setSelectionMode('destination')\"\r\n        [class.active]=\"pointSelectionMode === 'destination'\">\r\n        Destination\r\n      </button>\r\n\r\n      <button type=\"button\" (click)=\"clearAll()\" class=\"primary\">\r\n        Clear\r\n      </button>\r\n\r\n      <button type=\"button\" (click)=\"save()\" class=\"primary\">\r\n        Save\r\n      </button>\r\n    </div>\r\n  </div>\r\n\r\n  <div\r\n    class=\"map\"\r\n    leaflet\r\n    [leafletOptions]=\"options\"\r\n    [leafletLayers]=\"layers\"\r\n    (leafletMapReady)=\"onMapReady($event)\"\r\n    (leafletClick)=\"onMapClick($event)\">\r\n  </div>\r\n</div>", styles: [":host ::ng-deep .leaflet-control-attribution{display:none!important}.route-picker-container{width:33vw;min-width:560px;min-height:0;display:grid;grid-template-rows:auto minmax(220px,1fr) auto;gap:16px;padding:16px;box-sizing:border-box}.controls-block{display:grid;gap:12px;min-width:0}.toolbar{width:100%;min-width:0}.field-group{display:grid;gap:6px;min-width:0}.search-group{width:100%}.search-row{display:flex;gap:10px;align-items:center;width:fit-content}.search-row input{width:395px;min-width:0;height:30px;padding:0 12px;font-size:14px;box-sizing:border-box;border:1px solid #cfe0ff;border-radius:12px}.actions{display:flex;flex-wrap:wrap;gap:10px;align-items:center}.actions button,.search-row button{height:30px;padding:0 16px;border-radius:12px;border:1px solid #cfe0ff;background:#fff;cursor:pointer;white-space:nowrap;box-sizing:border-box;flex:0 0 auto}.actions button.active,.actions button.primary,.search-row button.primary{background:#1976d2;color:#fff;border-color:#1976d2}.actions button:disabled,.search-row button:disabled{opacity:.6;cursor:not-allowed}.map{width:100%;height:100%;min-height:380px;border:1px solid #ccc;border-radius:12px;overflow:hidden;box-sizing:border-box}.coordinates-panel{display:grid;grid-template-columns:1fr;gap:12px}.coord-block,.output-panel{border:1px solid #ddd;padding:12px;background:#fafafa;box-sizing:border-box}.output-panel{min-height:80px;max-height:120px;overflow:auto}.coord-block h3,.output-panel h3{margin:0 0 8px}pre{margin:0;white-space:pre-wrap;word-break:break-word}\n"] }]
        }], ctorParameters: () => [{ type: i0.NgZone }, { type: i1.HttpClient }], propDecorators: { country: [{
                type: Input
            }], saveRoute: [{
                type: Output
            }] } });

class MapmoduleModule {
    static { this.ɵfac = i0.ɵɵngDeclareFactory({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleModule, deps: [], target: i0.ɵɵFactoryTarget.NgModule }); }
    static { this.ɵmod = i0.ɵɵngDeclareNgModule({ minVersion: "14.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleModule, declarations: [MapmoduleComponent], imports: [CommonModule,
            FormsModule,
            LeafletModule], exports: [MapmoduleComponent] }); }
    static { this.ɵinj = i0.ɵɵngDeclareInjector({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleModule, imports: [CommonModule,
            FormsModule,
            LeafletModule] }); }
}
i0.ɵɵngDeclareClassMetadata({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleModule, decorators: [{
            type: NgModule,
            args: [{
                    declarations: [MapmoduleComponent],
                    imports: [
                        CommonModule,
                        FormsModule,
                        LeafletModule
                    ],
                    exports: [MapmoduleComponent]
                }]
        }] });

class MapmoduleLibModule {
    static { this.ɵfac = i0.ɵɵngDeclareFactory({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleLibModule, deps: [], target: i0.ɵɵFactoryTarget.NgModule }); }
    static { this.ɵmod = i0.ɵɵngDeclareNgModule({ minVersion: "14.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleLibModule, imports: [MapmoduleModule], exports: [MapmoduleModule] }); }
    static { this.ɵinj = i0.ɵɵngDeclareInjector({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleLibModule, imports: [MapmoduleModule, MapmoduleModule] }); }
}
i0.ɵɵngDeclareClassMetadata({ minVersion: "12.0.0", version: "17.3.12", ngImport: i0, type: MapmoduleLibModule, decorators: [{
            type: NgModule,
            args: [{
                    imports: [MapmoduleModule],
                    exports: [MapmoduleModule]
                }]
        }] });

/**
 * Generated bundle index. Do not edit.
 */

export { MapmoduleComponent, MapmoduleLibModule, MapmoduleModule };
//# sourceMappingURL=mapmodule-lib.mjs.map
