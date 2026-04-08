import { Component, Input } from '@angular/core';
import { RouterLink } from '@angular/router';

@Component({
  selector: 'app-hero-cta',
  standalone: true,
  imports: [RouterLink],
  template: `
    <section class="hero section">
      <div class="container hero-grid">
        <div class="hero-copy">
          <p class="hero-kicker">{{ kicker }}</p>
          <h1>{{ title }}</h1>
          <p class="hero-text">{{ text }}</p>

          <div class="hero-actions">
            <a class="cta-primary" [href]="primaryUrl">{{ primaryLabel }}</a>
            <a class="cta-secondary" [routerLink]="secondaryUrl">{{ secondaryLabel }}</a>
          </div>
        </div>

        <div class="hero-panel surface-card">
          <div class="hero-panel-top">
            <span class="panel-pill">Trackster Studio</span>
            <span class="panel-status">Secure access</span>
          </div>

          <div class="hero-stat-grid">
            <div class="hero-stat">
              <strong>DBC-aware</strong>
              <span>Use structured signal definitions as the basis for generation workflows.</span>
            </div>
            <div class="hero-stat">
              <strong>Route-driven</strong>
              <span>Combine CAN activity with geographic movement and route-based scenarios.</span>
            </div>
            <div class="hero-stat">
              <strong>Scalable output</strong>
              <span>Generate repeatable datasets for labs, backends, and integration pipelines.</span>
            </div>
            <div class="hero-stat">
              <strong>Controlled delivery</strong>
              <span>Give teams a secure, browser-based workspace to define and run simulations.</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  `,
  styles: [`
    .hero {
      padding-top: 48px;
      padding-bottom: 42px;
    }

    .hero-grid {
      display: grid;
      grid-template-columns: 1.05fr 0.95fr;
      gap: 28px;
      align-items: center;
    }

    .hero-kicker {
      margin: 0 0 14px;
      color: #1d4ed8;
      font-size: 0.95rem;
      font-weight: 800;
      letter-spacing: 0.05em;
      text-transform: uppercase;
    }

    h1 {
      margin: 0 0 16px;
      color: #0b1f44;
      font-size: clamp(2.5rem, 5vw, 4.25rem);
      line-height: 0.98;
      letter-spacing: -0.04em;
    }

    .hero-text {
      margin: 0;
      max-width: 640px;
      font-size: 1.08rem;
      line-height: 1.8;
      color: #334155;
    }

    .hero-actions {
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin-top: 28px;
    }

    .hero-panel {
      padding: 24px;
    }

    .hero-panel-top {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      margin-bottom: 18px;
    }

    .panel-pill,
    .panel-status {
      display: inline-flex;
      align-items: center;
      min-height: 34px;
      padding: 0 12px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 0.92rem;
      background: linear-gradient(135deg, #ffffff, #f8fbff);
      border: 1px solid #bfdbfe;
      color: #0b1f44;
    }

    .hero-stat-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }

    .hero-stat {
      padding: 18px;
      border-radius: 16px;
      border: 1px solid rgba(191, 219, 254, 0.9);
      background: linear-gradient(135deg, rgba(255, 255, 255, 0.94), rgba(248, 251, 255, 0.94));
    }

    .hero-stat strong {
      display: block;
      margin-bottom: 8px;
      color: #0b1f44;
      font-size: 1rem;
    }

    .hero-stat span {
      color: #475569;
      line-height: 1.65;
      font-size: 0.95rem;
    }

    @media (max-width: 980px) {
      .hero-grid {
        grid-template-columns: 1fr;
      }
    }

    @media (max-width: 640px) {
      .hero-stat-grid {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class HeroCtaComponent {
  @Input({ required: true }) public kicker!: string;
  @Input({ required: true }) public title!: string;
  @Input({ required: true }) public text!: string;
  @Input({ required: true }) public primaryLabel!: string;
  @Input({ required: true }) public primaryUrl!: string;
  @Input({ required: true }) public secondaryLabel!: string;
  @Input({ required: true }) public secondaryUrl!: string;
}
