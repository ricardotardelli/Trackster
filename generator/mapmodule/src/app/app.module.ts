import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { AppComponent } from './app.component';
import { MapmoduleModule } from './mapmodule/mapmodule.module';
import { HttpClientModule } from '@angular/common/http';

@NgModule({
  declarations: [AppComponent],
  imports: [
    BrowserModule,
    HttpClientModule,
    MapmoduleModule
  ],
  bootstrap: [AppComponent]
})
export class AppModule {}