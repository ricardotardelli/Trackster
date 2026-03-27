import { NgModule } from '@angular/core';
import { MapmoduleModule } from './mapmodule/mapmodule.module';

@NgModule({
  imports: [MapmoduleModule],
  exports: [MapmoduleModule]
})
export class MapmoduleLibModule {}