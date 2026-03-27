
import { importProvidersFrom } from '@angular/core';
import { bootstrapApplication } from '@angular/platform-browser';
import { AppComponent } from './app/app.component';
import { MapmoduleLibModule } from 'mapmodule-lib';

bootstrapApplication(AppComponent, {
  providers: [
    importProvidersFrom(MapmoduleLibModule)
  ]
}).catch((err) => console.error(err));
