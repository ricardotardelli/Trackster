import { Routes } from '@angular/router';
import { HomePageComponent } from './pages/home-page.component';
import { PlatformPageComponent } from './pages/platform-page.component';
import { UseCasesPageComponent } from './pages/use-cases-page.component';
import { AboutPageComponent } from './pages/about-page.component';
import { ContactPageComponent } from './pages/contact-page.component';
import { PrivacyPageComponent } from './pages/privacy-page.component';

export const appRoutes: Routes = [
  { path: '', component: HomePageComponent, title: 'Trackster | High-fidelity CAN Simulation' },
  { path: 'platform', component: PlatformPageComponent, title: 'Trackster | Platform' },
  { path: 'use-cases', component: UseCasesPageComponent, title: 'Trackster | Use Cases' },
  { path: 'about', component: AboutPageComponent, title: 'Trackster | About' },
  { path: 'contact', component: ContactPageComponent, title: 'Trackster | Contact' },
  { path: 'privacy', component: PrivacyPageComponent, title: 'Trackster | Privacy' },
  { path: '**', redirectTo: '' }
];
