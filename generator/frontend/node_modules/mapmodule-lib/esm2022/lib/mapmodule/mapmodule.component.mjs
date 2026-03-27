import { Component, Input, Output, EventEmitter } from '@angular/core';
import { HttpParams } from '@angular/common/http';
import * as L from 'leaflet';
import * as i0 from "@angular/core";
import * as i1 from "@angular/common/http";
import * as i2 from "@angular/forms";
import * as i3 from "@asymmetrik/ngx-leaflet";
export class MapmoduleComponent {
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibWFwbW9kdWxlLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzIjpbIi4uLy4uLy4uLy4uLy4uL21hcG1vZHVsZS1saWIvc3JjL2xpYi9tYXBtb2R1bGUvbWFwbW9kdWxlLmNvbXBvbmVudC50cyIsIi4uLy4uLy4uLy4uLy4uL21hcG1vZHVsZS1saWIvc3JjL2xpYi9tYXBtb2R1bGUvbWFwbW9kdWxlLmNvbXBvbmVudC5odG1sIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLE9BQU8sRUFBRSxTQUFTLEVBQWtCLEtBQUssRUFBNEIsTUFBTSxFQUFFLFlBQVksRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUNqSCxPQUFPLEVBQWMsVUFBVSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDOUQsT0FBTyxLQUFLLENBQUMsTUFBTSxTQUFTLENBQUM7Ozs7O0FBZTdCLE1BQU0sT0FBTyxrQkFBa0I7SUFJN0IsWUFDVSxNQUFjLEVBQ2QsSUFBZ0I7UUFEaEIsV0FBTSxHQUFOLE1BQU0sQ0FBUTtRQUNkLFNBQUksR0FBSixJQUFJLENBQVk7UUFMakIsWUFBTyxHQUFXLEVBQUUsQ0FBQztRQUNwQixjQUFTLEdBQUcsSUFBSSxZQUFZLEVBQVUsQ0FBQztRQU8xQyxtQkFBYyxHQUFHLEVBQUUsQ0FBQztRQUNwQixjQUFTLEdBQUcsRUFBRSxDQUFDO1FBQ2YsZ0JBQVcsR0FBRyxFQUFFLENBQUM7UUFDakIsY0FBUyxHQUFlLEVBQUUsQ0FBQztRQUMxQixtQkFBYyxHQUFXLFVBQVUsQ0FBQztRQUVyQyxlQUFVLEdBQW9CLElBQUksQ0FBQztRQUNuQyxxQkFBZ0IsR0FBb0IsSUFBSSxDQUFDO1FBRXpDLHVCQUFrQixHQUF1QixPQUFPLENBQUM7UUFRakQsWUFBTyxHQUFpQjtZQUM3QixNQUFNLEVBQUU7Z0JBQ04sQ0FBQyxDQUFDLFNBQVMsQ0FBQyxvREFBb0QsRUFBRTtvQkFDaEUsV0FBVyxFQUFFLG1DQUFtQztpQkFDakQsQ0FBQzthQUNIO1lBQ0QsSUFBSSxF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TyxrQkFBa0I7a0JBTDlCLFNBQVM7K0JBQ0UsZUFBZTtvR0FLaEIsT0FBTztzQkFBZixLQUFLO2dCQUNJLFNBQVM7c0JBQWxCLE1BQU0iLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDb21wb25lbnQsIE9uSW5pdCwgTmdab25lLCBJbnB1dCwgT25DaGFuZ2VzLCBTaW1wbGVDaGFuZ2VzLCBPdXRwdXQsIEV2ZW50RW1pdHRlciB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xyXG5pbXBvcnQgeyBIdHRwQ2xpZW50LCBIdHRwUGFyYW1zIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uL2h0dHAnO1xyXG5pbXBvcnQgKiBhcyBMIGZyb20gJ2xlYWZsZXQnO1xyXG5cclxudHlwZSBQb2ludFNlbGVjdGlvbk1vZGUgPSAnc3RhcnQnIHwgJ2Rlc3RpbmF0aW9uJyB8ICd3YXlwb2ludHMnO1xyXG5cclxuaW50ZXJmYWNlIEdlb1BvaW50IHtcclxuICBsYXQ6IG51bWJlcjtcclxuICBsbmc6IG51bWJlcjtcclxuICBsYWJlbD86IHN0cmluZztcclxufVxyXG5cclxuQENvbXBvbmVudCh7XHJcbiAgc2VsZWN0b3I6ICdhcHAtbWFwbW9kdWxlJyxcclxuICB0ZW1wbGF0ZVVybDogJy4vbWFwbW9kdWxlLmNvbXBvbmVudC5odG1sJyxcclxuICBzdHlsZVVybHM6IFsnLi9tYXBtb2R1bGUuY29tcG9uZW50LmNzcyddXHJcbn0pXHJcbmV4cG9ydCBjbGFzcyBNYXBtb2R1bGVDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcyB7XHJcbiAgQElucHV0KCkgY291bnRyeTogc3RyaW5nID0gJyc7XHJcbiAgQE91dHB1dCgpIHNhdmVSb3V0ZSA9IG5ldyBFdmVudEVtaXR0ZXI8c3RyaW5nPigpO1xyXG5cclxuICBjb25zdHJ1Y3RvcihcclxuICAgIHByaXZhdGUgbmdab25lOiBOZ1pvbmUsXHJcbiAgICBwcml2YXRlIGh0dHA6IEh0dHBDbGllbnRcclxuICApIHt9XHJcblxyXG4gIHB1YmxpYyBsb2NhdGlvblNlYXJjaCA9ICcnO1xyXG4gIHB1YmxpYyBzdGFydEZyb20gPSAnJztcclxuICBwdWJsaWMgZGVzdGluYXRpb24gPSAnJztcclxuICBwdWJsaWMgd2F5cG9pbnRzOiBHZW9Qb2ludFtdID0gW107XHJcbiAgcHJpdmF0ZSBkZWZhdWx0Q291bnRyeTogc3RyaW5nID0gJ1BvcnR1Z2FsJztcclxuXHJcbiAgcHVibGljIHN0YXJ0UG9pbnQ6IEdlb1BvaW50IHwgbnVsbCA9IG51bGw7XHJcbiAgcHVibGljIGRlc3RpbmF0aW9uUG9pbnQ6IEdlb1BvaW50IHwgbnVsbCA9IG51bGw7XHJcblxyXG4gIHB1YmxpYyBwb2ludFNlbGVjdGlvbk1vZGU6IFBvaW50U2VsZWN0aW9uTW9kZSA9ICdzdGFydCc7XHJcblxyXG4gIHB1YmxpYyBzYXZlKCk6IHZvaWQge1xyXG4gICAgY29uc3Qgb3V0cHV0ID0gdGhpcy5nZXRPdXRwdXQoKTtcclxuICAgIGNvbnNvbGUubG9nKCdNYXAgb3V0cHV0OicsIEpTT04ucGFyc2Uob3V0cHV0KSk7XHJcbiAgICB0aGlzLnNhdmVSb3V0ZS5lbWl0KG91dHB1dCk7XHJcbiAgfVxyXG4gIFxyXG4gIHB1YmxpYyBvcHRpb25zOiBMLk1hcE9wdGlvbnMgPSB7XHJcbiAgICBsYXllcnM6IFtcclxuICAgICAgTC50aWxlTGF5ZXIoJ2h0dHBzOi8ve3N9LnRpbGUub3BlbnN0cmVldG1hcC5vcmcve3p9L3t4fS97eX0ucG5nJywge1xyXG4gICAgICAgIGF0dHJpYnV0aW9uOiAnJmNvcHk7IE9wZW5TdHJlZXRNYXAgY29udHJpYnV0b3JzJ1xyXG4gICAgICB9KVxyXG4gICAgXSxcclxuICAgIHpvb206IDYsXHJcbiAgICBjZW50ZXI6IEwubGF0TG5nKDM5LjUsIC04LjApXHJcbiAgfTtcclxuXHJcbiAgcHVibGljIG1hcDogTC5NYXAgfCBudWxsID0gbnVsbDtcclxuICBwdWJsaWMgbGF5ZXJzOiBMLkxheWVyW10gPSBbXTtcclxuXHJcbiAgbmdPbkluaXQoKTogdm9pZCB7XHJcbiAgICB0aGlzLmFwcGx5Q291bnRyeVRvTWFwKCk7XHJcbiAgfVxyXG5cclxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XHJcbiAgICBpZiAoY2hhbmdlc1snY291bnRyeSddKSB7XHJcbiAgICAgIHRoaXMuYXBwbHlDb3VudHJ5VG9NYXAoKTtcclxuICAgIH1cclxuICB9XHJcblxyXG4gIHByaXZhdGUgbG9nT3V0cHV0KCk6IHZvaWQge1xyXG4gICAgY29uc29sZS5sb2coJ01hcCBvdXRwdXQ6JywgSlNPTi5wYXJzZSh0aGlzLmdldE91dHB1dCgpKSk7XHJcbiAgfVxyXG5cclxuICBwcml2YXRlIGdldENvdW50cnlNYXBDb25maWcoY291bnRyeTogc3RyaW5nKTogeyBjZW50ZXI6IEwuTGF0TG5nRXhwcmVzc2lvbjsgem9vbTogbnVtYmVyIH0ge1xyXG4gICAgY29uc3Qgbm9ybWFsaXplZCA9IChjb3VudHJ5IHx8ICcnKS50cmltKCkudG9Mb3dlckNhc2UoKTtcclxuXHJcbiAgICBzd2l0Y2ggKG5vcm1hbGl6ZWQpIHtcclxuICAgICAgY2FzZSAncG9ydHVnYWwnOlxyXG4gICAgICAgIHJldHVybiB7XHJcbiAgICAgICAgICBjZW50ZXI6IEwubGF0TG5nKDM5LjUsIC04LjApLFxyXG4gICAgICAgICAgem9vbTogNlxyXG4gICAgICAgIH07XHJcblxyXG4gICAgICBjYXNlICdzcGFpbic6XHJcbiAgICAgICAgcmV0dXJuIHtcclxuICAgICAgICAgIGNlbnRlcjogTC5sYXRMbmcoNDAuMiwgLTMuNyksXHJcbiAgICAgICAgICB6b29tOiA2XHJcbiAgICAgICAgfTtcclxuXHJcbiAgICAgIGNhc2UgJ2ZyYW5jZSc6XHJcbiAgICAgICAgcmV0dXJuIHtcclxuICAgICAgICAgIGNlbnRlcjogTC5sYXRMbmcoNDYuMiwgMi4yKSxcclxuICAgICAgICAgIHpvb206IDZcclxuICAgICAgICB9O1xyXG5cclxuICAgICAgY2FzZSAnYnJhemlsJzpcclxuICAgICAgICByZXR1cm4ge1xyXG4gICAgICAgICAgY2VudGVyOiBMLmxhdExuZygtMTQuMiwgLTUxLjkpLFxyXG4gICAgICAgICAgem9vbTogNFxyXG4gICAgICAgIH07XHJcblxyXG4gICAgICBjYXNlICd1c2EnOlxyXG4gICAgICBjYXNlICd1bml0ZWQgc3RhdGVzJzpcclxuICAgICAgICByZXR1cm4ge1xyXG4gICAgICAgICAgY2VudGVyOiBMLmxhdExuZygzOS44LCAtOTguNiksXHJcbiAgICAgICAgICB6b29tOiA0XHJcbiAgICAgICAgfTtcclxuXHJcbiAgICAgIGRlZmF1bHQ6XHJcbiAgICAgICAgcmV0dXJuIHtcclxuICAgICAgICAgIGNlbnRlcjogTC5sYXRMbmcoMzkuNSwgLTguMCksXHJcbiAgICAgICAgICB6b29tOiA2XHJcbiAgICAgICAgfTtcclxuICAgIH1cclxuICB9XHJcblxyXG4gIHByaXZhdGUgYXBwbHlDb3VudHJ5VG9NYXAoKTogdm9pZCB7XHJcbiAgICBjb25zdCBlZmZlY3RpdmVDb3VudHJ5ID1cclxuICAgICAgdGhpcy5jb3VudHJ5ICYmIHRoaXMuY291bnRyeS50cmltKCkgIT09ICcnXHJcbiAgICAgICAgPyB0aGlzLmNvdW50cnlcclxuICAgICAgICA6IHRoaXMuZGVmYXVsdENvdW50cnk7XHJcblxyXG4gICAgY29uc3QgY29uZmlnID0gdGhpcy5nZXRDb3VudHJ5TWFwQ29uZmlnKGVmZmVjdGl2ZUNvdW50cnkpO1xyXG5cclxuICAgIHRoaXMub3B0aW9ucyA9IHtcclxuICAgICAgLi4udGhpcy5vcHRpb25zLFxyXG4gICAgICBjZW50ZXI6IGNvbmZpZy5jZW50ZXIsXHJcbiAgICAgIHpvb206IGNvbmZpZy56b29tXHJcbiAgICB9O1xyXG5cclxuICAgIGlmICh0aGlzLm1hcCkge1xyXG4gICAgICB0aGlzLm1hcC5zZXRWaWV3KGNvbmZpZy5jZW50ZXIsIGNvbmZpZy56b29tKTtcclxuICAgIH1cclxuICB9XHJcblxyXG4gIHB1YmxpYyBnZXRMb2NhdGlvblBsYWNlaG9sZGVyKCk6IHN0cmluZyB7XHJcbiAgICBpZiAodGhpcy5wb2ludFNlbGVjdGlvbk1vZGUgPT09ICdzdGFydCcpIHtcclxuICAgICAgcmV0dXJuICdTZWxlY3Qgb3IgdHlwZSBzdGFydCBwb2ludCBuYW1lJztcclxuICAgIH1cclxuXHJcbiAgICBpZiAodGhpcy5wb2ludFNlbGVjdGlvbk1vZGUgPT09ICdkZXN0aW5hdGlvbicpIHtcclxuICAgICAgcmV0dXJuICdTZWxlY3Qgb3IgdHlwZSBkZXN0aW5hdGlvbiBuYW1lJztcclxuICAgIH1cclxuXHJcbiAgICByZXR1cm4gJ1NlYXJjaCBhbmQgYWRkIHdheXBvaW50JztcclxuICB9XHJcblxyXG4gIHB1YmxpYyBzZWFyY2hTZWxlY3RlZEFkZHJlc3MoKTogdm9pZCB7XHJcbiAgICB0aGlzLnNlYXJjaEFkZHJlc3ModGhpcy5sb2NhdGlvblNlYXJjaCwgdGhpcy5wb2ludFNlbGVjdGlvbk1vZGUpO1xyXG4gIH1cclxuXHJcbiAgcHVibGljIHNlYXJjaEFkZHJlc3MocXVlcnk6IHN0cmluZywgdGFyZ2V0OiBQb2ludFNlbGVjdGlvbk1vZGUpOiB2b2lkIHtcclxuICAgIGNvbnN0IHRyaW1tZWRRdWVyeSA9IHF1ZXJ5ID8gcXVlcnkudHJpbSgpIDogJyc7XHJcblxyXG4gICAgaWYgKCF0cmltbWVkUXVlcnkpIHtcclxuICAgICAgcmV0dXJuO1xyXG4gICAgfVxyXG5cclxuICAgIGNvbnN0IHBhcmFtcyA9IG5ldyBIdHRwUGFyYW1zKClcclxuICAgICAgLnNldCgncScsIHRyaW1tZWRRdWVyeSlcclxuICAgICAgLnNldCgnZm9ybWF0JywgJ2pzb252MicpXHJcbiAgICAgIC5zZXQoJ2xpbWl0JywgJzEnKTtcclxuXHJcbiAgICB0aGlzLmh0dHBcclxuICAgICAgLmdldDxhbnlbXT4oJ2h0dHBzOi8vbm9taW5hdGltLm9wZW5zdHJlZXRtYXAub3JnL3NlYXJjaCcsIHsgcGFyYW1zIH0pXHJcbiAgICAgIC5zdWJzY3JpYmUoe1xyXG4gICAgICAgIG5leHQ6IChyZXN1bHRzOiBhbnlbXSkgPT4ge1xyXG4gICAgICAgICAgaWYgKCFyZXN1bHRzIHx8IHJlc3VsdHMubGVuZ3RoID09PSAwKSB7XHJcbiAgICAgICAgICAgIHJldHVybjtcclxuICAgICAgICAgIH1cclxuXHJcbiAgICAgICAgICBjb25zdCBmaXJzdFJlc3VsdCA9IHJlc3VsdHNbMF07XHJcbiAgICAgICAgICBjb25zdCByZXNvbHZlZE5hbWUgPSBmaXJzdFJlc3VsdC5kaXNwbGF5X25hbWUgfHwgdHJpbW1lZFF1ZXJ5O1xyXG5cclxuICAgICAgICAgIGNvbnN0IHBvaW50OiBHZW9Qb2ludCA9IHtcclxuICAgICAgICAgICAgbGF0OiBOdW1iZXIoZmlyc3RSZXN1bHQubGF0KSxcclxuICAgICAgICAgICAgbG5nOiBOdW1iZXIoZmlyc3RSZXN1bHQubG9uKSxcclxuICAgICAgICAgICAgbGFiZWw6IHJlc29sdmVkTmFtZVxyXG4gICAgICAgICAgfTtcclxuXHJcbiAgICAgICAgICBpZiAodGFyZ2V0ID09PSAnc3RhcnQnKSB7XHJcbiAgICAgICAgICAgIHRoaXMuc3RhcnRQb2ludCA9IHBvaW50O1xyXG4gICAgICAgICAgICB0aGlzLnN0YXJ0RnJvbSA9IHJlc29sdmVkTmFtZTtcclxuICAgICAgICAgICAgdGhpcy5sb2NhdGlvblNlYXJjaCA9IHJlc29sdmVkTmFtZTtcclxuICAgICAgICAgIH0gZWxzZSBpZiAodGFyZ2V0ID09PSAnZGVzdGluYXRpb24nKSB7XHJcbiAgICAgICAgICAgIHRoaXMuZGVzdGluYXRpb25Qb2ludCA9IHBvaW50O1xyXG4gICAgICAgICAgICB0aGlzLmRlc3RpbmF0aW9uID0gcmVzb2x2ZWROYW1lO1xyXG4gICAgICAgICAgICB0aGlzLmxvY2F0aW9uU2VhcmNoID0gcmVzb2x2ZWROYW1lO1xyXG4gICAgICAgICAgfSBlbHNlIHtcclxuICAgICAgICAgICAgdGhpcy53YXlwb2ludHMucHVzaChwb2ludCk7XHJcbiAgICAgICAgICAgIHRoaXMubG9jYXRpb25TZWFyY2ggPSAnJztcclxuICAgICAgICAgIH1cclxuXHJcbiAgICAgICAgICB0aGlzLnJlZnJlc2hMYXllcnMoKTtcclxuXHJcbiAgICAgICAgICBpZiAodGhpcy5tYXApIHtcclxuICAgICAgICAgICAgdGhpcy5tYXAuc2V0VmlldyhbcG9pbnQubGF0LCBwb2ludC5sbmddLCAxMyk7XHJcbiAgICAgICAgICB9XHJcbiAgICAgICAgfSxcclxuICAgICAgICBlcnJvcjogKGVycm9yOiB1bmtub3duKSA9PiB7XHJcbiAgICAgICAgICBjb25zb2xlLmVycm9yKCdBZGRyZXNzIHNlYXJjaCBmYWlsZWQ6JywgZXJyb3IpO1xyXG4gICAgICAgIH1cclxuICAgICAgfSk7XHJcbiAgfVxyXG5cclxuICBwdWJsaWMgb25NYXBSZWFkeShtYXA6IEwuTWFwKTogdm9pZCB7XHJcbiAgICB0aGlzLm1hcCA9IG1hcDtcclxuICB9XHJcblxyXG4gIHB1YmxpYyBvbk1hcENsaWNrKGV2ZW50OiBMLkxlYWZsZXRNb3VzZUV2ZW50KTogdm9pZCB7XHJcbiAgICBjb25zdCBsYXQgPSBOdW1iZXIoZXZlbnQubGF0bG5nLmxhdC50b0ZpeGVkKDYpKTtcclxuICAgIGNvbnN0IGxuZyA9IE51bWJlcihldmVudC5sYXRsbmcubG5nLnRvRml4ZWQoNikpO1xyXG5cclxuICAgIGNvbnN0IHBvaW50OiBHZW9Qb2ludCA9IHtcclxuICAgICAgbGF0OiBsYXQsXHJcbiAgICAgIGxuZzogbG5nLFxyXG4gICAgICBsYWJlbDogYCR7bGF0fSwgJHtsbmd9YFxyXG4gICAgfTtcclxuXHJcbiAgICBpZiAodGhpcy5wb2ludFNlbGVjdGlvbk1vZGUgPT09ICdzdGFydCcpIHtcclxuICAgICAgdGhpcy5zdGFydFBvaW50ID0gcG9pbnQ7XHJcbiAgICAgIHRoaXMuc3RhcnRGcm9tID0gcG9pbnQubGFiZWwgfHwgJyc7XHJcbiAgICAgIHRoaXMubG9jYXRpb25TZWFyY2ggPSB0aGlzLnN0YXJ0RnJvbTtcclxuICAgIH0gZWxzZSBpZiAodGhpcy5wb2ludFNlbGVjdGlvbk1vZGUgPT09ICdkZXN0aW5hdGlvbicpIHtcclxuICAgICAgdGhpcy5kZXN0aW5hdGlvblBvaW50ID0gcG9pbnQ7XHJcbiAgICAgIHRoaXMuZGVzdGluYXRpb24gPSBwb2ludC5sYWJlbCB8fCAnJztcclxuICAgICAgdGhpcy5sb2NhdGlvblNlYXJjaCA9IHRoaXMuZGVzdGluYXRpb247XHJcbiAgICB9IGVsc2Uge1xyXG4gICAgICB0aGlzLndheXBvaW50cy5wdXNoKHBvaW50KTtcclxuICAgICAgdGhpcy5sb2NhdGlvblNlYXJjaCA9ICcnO1xyXG4gICAgfVxyXG5cclxuICAgIHRoaXMucmVmcmVzaExheWVycygpO1xyXG4gIH1cclxuXHJcbiAgcHVibGljIHNldFNlbGVjdGlvbk1vZGUobW9kZTogUG9pbnRTZWxlY3Rpb25Nb2RlKTogdm9pZCB7XHJcbiAgICB0aGlzLnBvaW50U2VsZWN0aW9uTW9kZSA9IG1vZGU7XHJcblxyXG4gICAgaWYgKG1vZGUgPT09ICdzdGFydCcpIHtcclxuICAgICAgdGhpcy5sb2NhdGlvblNlYXJjaCA9IHRoaXMuc3RhcnRGcm9tO1xyXG4gICAgfSBlbHNlIGlmIChtb2RlID09PSAnZGVzdGluYXRpb24nKSB7XHJcbiAgICAgIHRoaXMubG9jYXRpb25TZWFyY2ggPSB0aGlzLmRlc3RpbmF0aW9uO1xyXG4gICAgfSBlbHNlIHtcclxuICAgICAgdGhpcy5sb2NhdGlvblNlYXJjaCA9ICcnO1xyXG4gICAgfVxyXG4gIH1cclxuXHJcbiAgcHVibGljIGNsZWFyQWxsKCk6IHZvaWQge1xyXG4gICAgdGhpcy5sb2NhdGlvblNlYXJjaCA9ICcnO1xyXG4gICAgdGhpcy5zdGFydEZyb20gPSAnJztcclxuICAgIHRoaXMuZGVzdGluYXRpb24gPSAnJztcclxuICAgIHRoaXMuc3RhcnRQb2ludCA9IG51bGw7XHJcbiAgICB0aGlzLmRlc3RpbmF0aW9uUG9pbnQgPSBudWxsO1xyXG4gICAgdGhpcy53YXlwb2ludHMgPSBbXTtcclxuICAgIHRoaXMubGF5ZXJzID0gW107XHJcbiAgfVxyXG5cclxuICBwdWJsaWMgZ2V0T3V0cHV0KCk6IHN0cmluZyB7XHJcbiAgICBjb25zdCB3YXlwb2ludHNPYmplY3Q6IFJlY29yZDxzdHJpbmcsIEdlb1BvaW50PiA9IHt9O1xyXG5cclxuICAgIHRoaXMud2F5cG9pbnRzLmZvckVhY2goKHdwOiBHZW9Qb2ludCwgaW5kZXg6IG51bWJlcikgPT4ge1xyXG4gICAgICB3YXlwb2ludHNPYmplY3RbU3RyaW5nKGluZGV4ICsgMSldID0ge1xyXG4gICAgICAgIGxhdDogd3AubGF0LFxyXG4gICAgICAgIGxuZzogd3AubG5nLFxyXG4gICAgICAgIGxhYmVsOiB3cC5sYWJlbFxyXG4gICAgICB9O1xyXG4gICAgfSk7XHJcblxyXG4gICAgcmV0dXJuIEpTT04uc3RyaW5naWZ5KFxyXG4gICAgICB7XHJcbiAgICAgICAgc3RhcnQ6IHRoaXMuc3RhcnRQb2ludFxyXG4gICAgICAgICAgPyB7XHJcbiAgICAgICAgICAgICAgbGF0OiB0aGlzLnN0YXJ0UG9pbnQubGF0LFxyXG4gICAgICAgICAgICAgIGxuZzogdGhpcy5zdGFydFBvaW50LmxuZyxcclxuICAgICAgICAgICAgICBsYWJlbDogdGhpcy5zdGFydFBvaW50LmxhYmVsXHJcbiAgICAgICAgICAgIH1cclxuICAgICAgICAgIDogbnVsbCxcclxuICAgICAgICB3YXlwb2ludHM6IHdheXBvaW50c09iamVjdCxcclxuICAgICAgICBkZXN0aW5hdGlvbjogdGhpcy5kZXN0aW5hdGlvblBvaW50XHJcbiAgICAgICAgICA/IHtcclxuICAgICAgICAgICAgICBsYXQ6IHRoaXMuZGVzdGluYXRpb25Qb2ludC5sYXQsXHJcbiAgICAgICAgICAgICAgbG5nOiB0aGlzLmRlc3RpbmF0aW9uUG9pbnQubG5nLFxyXG4gICAgICAgICAgICAgIGxhYmVsOiB0aGlzLmRlc3RpbmF0aW9uUG9pbnQubGFiZWxcclxuICAgICAgICAgICAgfVxyXG4gICAgICAgICAgOiBudWxsXHJcbiAgICAgIH0sXHJcbiAgICAgIG51bGwsXHJcbiAgICAgIDJcclxuICAgICk7XHJcbiAgfVxyXG5cclxuICBwcml2YXRlIHJlbW92ZVdheXBvaW50KGluZGV4OiBudW1iZXIpOiB2b2lkIHtcclxuICAgIHRoaXMud2F5cG9pbnRzLnNwbGljZShpbmRleCwgMSk7XHJcbiAgICB0aGlzLnJlZnJlc2hMYXllcnMoKTtcclxuICB9XHJcblxyXG4gIHByaXZhdGUgZ2V0TWFya2VyVGV4dChcclxuICAgIHR5cGU6ICdTdGFydCcgfCAnV2F5cG9pbnQnIHwgJ0Rlc3RpbmF0aW9uJyxcclxuICAgIHBvaW50OiBHZW9Qb2ludCxcclxuICAgIGluZGV4PzogbnVtYmVyXHJcbiAgKTogc3RyaW5nIHtcclxuICAgIGNvbnN0IHZhbHVlID0gcG9pbnQubGFiZWwgPyBwb2ludC5sYWJlbCA6IGAke3BvaW50LmxhdH0sICR7cG9pbnQubG5nfWA7XHJcblxyXG4gICAgaWYgKHR5cGUgPT09ICdXYXlwb2ludCcpIHtcclxuICAgICAgY29uc3QgbnVtYmVyVGV4dCA9IGluZGV4ICE9PSB1bmRlZmluZWQgPyBTdHJpbmcoaW5kZXggKyAxKSA6ICc/JztcclxuICAgICAgcmV0dXJuIGBXYXlwb2ludCAke251bWJlclRleHR9OiAke3ZhbHVlfWA7XHJcbiAgICB9XHJcblxyXG4gICAgcmV0dXJuIGAke3R5cGV9OiAke3ZhbHVlfWA7XHJcbiAgfVxyXG5cclxuICBwcml2YXRlIGNyZWF0ZUNvbG9yZWRJY29uKGNvbG9yOiBzdHJpbmcpOiBMLkRpdkljb24ge1xyXG4gICAgcmV0dXJuIEwuZGl2SWNvbih7XHJcbiAgICAgIGNsYXNzTmFtZTogJycsXHJcbiAgICAgIGh0bWw6IGBcclxuICAgICAgICA8ZGl2XHJcbiAgICAgICAgICBzdHlsZT1cIlxyXG4gICAgICAgICAgICB3aWR0aDogMThweDtcclxuICAgICAgICAgICAgaGVpZ2h0OiAxOHB4O1xyXG4gICAgICAgICAgICBib3JkZXItcmFkaXVzOiA1MCU7XHJcbiAgICAgICAgICAgIGJhY2tncm91bmQtY29sb3I6ICR7Y29sb3J9O1xyXG4gICAgICAgICAgICBib3JkZXI6IDJweCBzb2xpZCAjZmZmZmZmO1xyXG4gICAgICAgICAgICBib3gtc2hhZG93OiAwIDFweCA0cHggcmdiYSgwLCAwLCAwLCAwLjM1KTtcclxuICAgICAgICAgICAgYm94LXNpemluZzogYm9yZGVyLWJveDtcclxuICAgICAgICAgIFwiPlxyXG4gICAgICAgIDwvZGl2PlxyXG4gICAgICBgLFxyXG4gICAgICBpY29uU2l6ZTogWzE4LCAxOF0sXHJcbiAgICAgIGljb25BbmNob3I6IFs5LCA5XVxyXG4gICAgfSk7XHJcbiAgfVxyXG5cclxuICBwcml2YXRlIHJlZnJlc2hMYXllcnMoKTogdm9pZCB7XHJcbiAgICBjb25zdCBuZXdMYXllcnM6IEwuTGF5ZXJbXSA9IFtdO1xyXG5cclxuICAgIGlmICh0aGlzLnN0YXJ0UG9pbnQpIHtcclxuICAgICAgY29uc3Qgc3RhcnRNYXJrZXIgPSBMLm1hcmtlcihcclxuICAgICAgICBbdGhpcy5zdGFydFBvaW50LmxhdCwgdGhpcy5zdGFydFBvaW50LmxuZ10sXHJcbiAgICAgICAge1xyXG4gICAgICAgICAgaWNvbjogdGhpcy5jcmVhdGVDb2xvcmVkSWNvbignIzJlN2QzMicpXHJcbiAgICAgICAgfVxyXG4gICAgICApO1xyXG5cclxuICAgICAgc3RhcnRNYXJrZXIuYmluZFRvb2x0aXAoXHJcbiAgICAgICAgdGhpcy5nZXRNYXJrZXJUZXh0KCdTdGFydCcsIHRoaXMuc3RhcnRQb2ludCksXHJcbiAgICAgICAgeyBkaXJlY3Rpb246ICd0b3AnIH1cclxuICAgICAgKTtcclxuXHJcbiAgICAgIG5ld0xheWVycy5wdXNoKHN0YXJ0TWFya2VyKTtcclxuICAgIH1cclxuXHJcbiAgICB0aGlzLndheXBvaW50cy5mb3JFYWNoKCh3cDogR2VvUG9pbnQsIGluZGV4OiBudW1iZXIpID0+IHtcclxuICAgICAgY29uc3Qgd3BNYXJrZXIgPSBMLm1hcmtlcihcclxuICAgICAgICBbd3AubGF0LCB3cC5sbmddLFxyXG4gICAgICAgIHtcclxuICAgICAgICAgIGljb246IHRoaXMuY3JlYXRlQ29sb3JlZEljb24oJyMxOTc2ZDInKVxyXG4gICAgICAgIH1cclxuICAgICAgKTtcclxuXHJcbiAgICAgIHdwTWFya2VyLmJpbmRUb29sdGlwKFxyXG4gICAgICAgIHRoaXMuZ2V0TWFya2VyVGV4dCgnV2F5cG9pbnQnLCB3cCwgaW5kZXgpLFxyXG4gICAgICAgIHsgZGlyZWN0aW9uOiAndG9wJyB9XHJcbiAgICAgICk7XHJcblxyXG4gICAgICB3cE1hcmtlci5vbignY2xpY2snLCAoKSA9PiB7XHJcbiAgICAgICAgdGhpcy5uZ1pvbmUucnVuKCgpID0+IHtcclxuICAgICAgICAgIHRoaXMucmVtb3ZlV2F5cG9pbnQoaW5kZXgpO1xyXG4gICAgICAgIH0pO1xyXG4gICAgICB9KTtcclxuXHJcbiAgICAgIG5ld0xheWVycy5wdXNoKHdwTWFya2VyKTtcclxuICAgIH0pO1xyXG5cclxuICAgIGlmICh0aGlzLmRlc3RpbmF0aW9uUG9pbnQpIHtcclxuICAgICAgY29uc3QgZGVzdGluYXRpb25NYXJrZXIgPSBMLm1hcmtlcihcclxuICAgICAgICBbdGhpcy5kZXN0aW5hdGlvblBvaW50LmxhdCwgdGhpcy5kZXN0aW5hdGlvblBvaW50LmxuZ10sXHJcbiAgICAgICAge1xyXG4gICAgICAgICAgaWNvbjogdGhpcy5jcmVhdGVDb2xvcmVkSWNvbignI2Y5YTgyNScpXHJcbiAgICAgICAgfVxyXG4gICAgICApO1xyXG5cclxuICAgICAgZGVzdGluYXRpb25NYXJrZXIuYmluZFRvb2x0aXAoXHJcbiAgICAgICAgdGhpcy5nZXRNYXJrZXJUZXh0KCdEZXN0aW5hdGlvbicsIHRoaXMuZGVzdGluYXRpb25Qb2ludCksXHJcbiAgICAgICAgeyBkaXJlY3Rpb246ICd0b3AnIH1cclxuICAgICAgKTtcclxuXHJcbiAgICAgIG5ld0xheWVycy5wdXNoKGRlc3RpbmF0aW9uTWFya2VyKTtcclxuICAgIH1cclxuXHJcbiAgICBjb25zdCBwYXRoOiBbbnVtYmVyLCBudW1iZXJdW10gPSBbXTtcclxuXHJcbiAgICBpZiAodGhpcy5zdGFydFBvaW50KSB7XHJcbiAgICAgIHBhdGgucHVzaChbdGhpcy5zdGFydFBvaW50LmxhdCwgdGhpcy5zdGFydFBvaW50LmxuZ10pO1xyXG4gICAgfVxyXG5cclxuICAgIHRoaXMud2F5cG9pbnRzLmZvckVhY2goKHdwOiBHZW9Qb2ludCkgPT4ge1xyXG4gICAgICBwYXRoLnB1c2goW3dwLmxhdCwgd3AubG5nXSk7XHJcbiAgICB9KTtcclxuXHJcbiAgICBpZiAodGhpcy5kZXN0aW5hdGlvblBvaW50KSB7XHJcbiAgICAgIHBhdGgucHVzaChbdGhpcy5kZXN0aW5hdGlvblBvaW50LmxhdCwgdGhpcy5kZXN0aW5hdGlvblBvaW50LmxuZ10pO1xyXG4gICAgfVxyXG5cclxuICAgIGlmIChwYXRoLmxlbmd0aCA+PSAyKSB7XHJcbiAgICAgIG5ld0xheWVycy5wdXNoKEwucG9seWxpbmUocGF0aCkpO1xyXG4gICAgfVxyXG5cclxuICAgIHRoaXMubGF5ZXJzID0gbmV3TGF5ZXJzO1xyXG4gICAgdGhpcy5sb2dPdXRwdXQoKTtcclxuICB9XHJcbn0iLCI8ZGl2IGNsYXNzPVwicm91dGUtcGlja2VyLWNvbnRhaW5lclwiPlxyXG4gIDxkaXYgY2xhc3M9XCJjb250cm9scy1ibG9ja1wiPlxyXG4gICAgPGRpdiBjbGFzcz1cInRvb2xiYXJcIj5cclxuICAgICAgPGRpdiBjbGFzcz1cImZpZWxkLWdyb3VwIHNlYXJjaC1ncm91cFwiPlxyXG4gICAgICAgIDxsYWJlbCBmb3I9XCJsb2NhdGlvblNlYXJjaFwiPkxvY2F0aW9uPC9sYWJlbD5cclxuXHJcbiAgICAgICAgPGRpdiBjbGFzcz1cInNlYXJjaC1yb3dcIj5cclxuICAgICAgICAgIDxpbnB1dFxyXG4gICAgICAgICAgICBpZD1cImxvY2F0aW9uU2VhcmNoXCJcclxuICAgICAgICAgICAgbmFtZT1cImxvY2F0aW9uU2VhcmNoXCJcclxuICAgICAgICAgICAgdHlwZT1cInRleHRcIlxyXG4gICAgICAgICAgICBbKG5nTW9kZWwpXT1cImxvY2F0aW9uU2VhcmNoXCJcclxuICAgICAgICAgICAgW3BsYWNlaG9sZGVyXT1cImdldExvY2F0aW9uUGxhY2Vob2xkZXIoKVwiXHJcbiAgICAgICAgICAgIChrZXl1cC5lbnRlcik9XCJzZWFyY2hTZWxlY3RlZEFkZHJlc3MoKVwiXHJcbiAgICAgICAgICAgIChibHVyKT1cInNlYXJjaFNlbGVjdGVkQWRkcmVzcygpXCJcclxuICAgICAgICAgIC8+XHJcblxyXG4gICAgICAgICAgPGJ1dHRvblxyXG4gICAgICAgICAgICB0eXBlPVwiYnV0dG9uXCJcclxuICAgICAgICAgICAgY2xhc3M9XCJwcmltYXJ5XCJcclxuICAgICAgICAgICAgKGNsaWNrKT1cInNlYXJjaFNlbGVjdGVkQWRkcmVzcygpXCI+XHJcbiAgICAgICAgICAgIFNlYXJjaFxyXG4gICAgICAgICAgPC9idXR0b24+XHJcbiAgICAgICAgPC9kaXY+XHJcbiAgICAgIDwvZGl2PlxyXG4gICAgPC9kaXY+XHJcblxyXG4gICAgPGRpdiBjbGFzcz1cImFjdGlvbnNcIj5cclxuICAgICAgPGJ1dHRvblxyXG4gICAgICAgIHR5cGU9XCJidXR0b25cIlxyXG4gICAgICAgIChjbGljayk9XCJzZXRTZWxlY3Rpb25Nb2RlKCdzdGFydCcpXCJcclxuICAgICAgICBbY2xhc3MuYWN0aXZlXT1cInBvaW50U2VsZWN0aW9uTW9kZSA9PT0gJ3N0YXJ0J1wiPlxyXG4gICAgICAgIFN0YXJ0IFBvaW50XHJcbiAgICAgIDwvYnV0dG9uPlxyXG5cclxuICAgICAgPGJ1dHRvblxyXG4gICAgICAgIHR5cGU9XCJidXR0b25cIlxyXG4gICAgICAgIChjbGljayk9XCJzZXRTZWxlY3Rpb25Nb2RlKCd3YXlwb2ludHMnKVwiXHJcbiAgICAgICAgW2NsYXNzLmFjdGl2ZV09XCJwb2ludFNlbGVjdGlvbk1vZGUgPT09ICd3YXlwb2ludHMnXCI+XHJcbiAgICAgICAgV2F5cG9pbnRzXHJcbiAgICAgIDwvYnV0dG9uPlxyXG5cclxuICAgICAgPGJ1dHRvblxyXG4gICAgICAgIHR5cGU9XCJidXR0b25cIlxyXG4gICAgICAgIChjbGljayk9XCJzZXRTZWxlY3Rpb25Nb2RlKCdkZXN0aW5hdGlvbicpXCJcclxuICAgICAgICBbY2xhc3MuYWN0aXZlXT1cInBvaW50U2VsZWN0aW9uTW9kZSA9PT0gJ2Rlc3RpbmF0aW9uJ1wiPlxyXG4gICAgICAgIERlc3RpbmF0aW9uXHJcbiAgICAgIDwvYnV0dG9uPlxyXG5cclxuICAgICAgPGJ1dHRvbiB0eXBlPVwiYnV0dG9uXCIgKGNsaWNrKT1cImNsZWFyQWxsKClcIiBjbGFzcz1cInByaW1hcnlcIj5cclxuICAgICAgICBDbGVhclxyXG4gICAgICA8L2J1dHRvbj5cclxuXHJcbiAgICAgIDxidXR0b24gdHlwZT1cImJ1dHRvblwiIChjbGljayk9XCJzYXZlKClcIiBjbGFzcz1cInByaW1hcnlcIj5cclxuICAgICAgICBTYXZlXHJcbiAgICAgIDwvYnV0dG9uPlxyXG4gICAgPC9kaXY+XHJcbiAgPC9kaXY+XHJcblxyXG4gIDxkaXZcclxuICAgIGNsYXNzPVwibWFwXCJcclxuICAgIGxlYWZsZXRcclxuICAgIFtsZWFmbGV0T3B0aW9uc109XCJvcHRpb25zXCJcclxuICAgIFtsZWFmbGV0TGF5ZXJzXT1cImxheWVyc1wiXHJcbiAgICAobGVhZmxldE1hcFJlYWR5KT1cIm9uTWFwUmVhZHkoJGV2ZW50KVwiXHJcbiAgICAobGVhZmxldENsaWNrKT1cIm9uTWFwQ2xpY2soJGV2ZW50KVwiPlxyXG4gIDwvZGl2PlxyXG48L2Rpdj4iXX0=