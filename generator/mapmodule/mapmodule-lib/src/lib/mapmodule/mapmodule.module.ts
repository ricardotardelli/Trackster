import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { LeafletModule } from '@asymmetrik/ngx-leaflet';
import { MapmoduleComponent } from './mapmodule.component';

@NgModule({
  declarations: [MapmoduleComponent],
  imports: [
    CommonModule,
    FormsModule,
    LeafletModule
  ],
  exports: [MapmoduleComponent]
})
export class MapmoduleModule {}