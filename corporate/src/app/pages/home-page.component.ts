import { Component } from '@angular/core';
import { RouterLink } from '@angular/router';
import { HeroCtaComponent } from '../components/hero-cta.component';

@Component({
  standalone: true,
  imports: [RouterLink, HeroCtaComponent],
  template: `
    <app-hero-cta
      kicker="Structured simulation for modern vehicle testing"
      title="High-fidelity CAN generation with route-aware control"
      text="Trackster gives teams a clear, browser-based way to define simulation inputs, generate structured CAN activity, and create repeatable outputs for validation, backend testing, and synthetic fleet workflows."
      primaryLabel="Open Studio"
      primaryUrl="https://studio.trackster.pt"
      secondaryLabel="Explore Platform"
      secondaryUrl="/platform"
    />

    <section class="section">
      <div class="container">
        <div class="section-header">
          <p class="section-kicker">Why Trackster</p>
          <h2 class="section-title">Built for teams that need control, repeatability, and realistic test data</h2>
          <p class="section-text">
            Trackster helps engineering and validation teams move beyond fragile one-off scripts and manually assembled datasets.
            It creates a structured environment for CAN simulation workflows that need to be repeatable, configurable, and ready for scale.
          </p>
        </div>

        <div class="card-grid three">
          <article class="surface-card feature-card">
            <h3>Configuration without guesswork</h3>
            <p>Define DBC files, selected CAN frames, route inputs, generation parameters, block sizing, and execution settings from a single controlled workspace.</p>
          </article>

          <article class="surface-card feature-card">
            <h3>Route-aware scenario generation</h3>
            <p>Combine movement logic with CAN activity to produce richer, more realistic scenarios for simulation, ingestion, and platform validation.</p>
          </article>

          <article class="surface-card feature-card">
            <h3>Cloud-ready workflows</h3>
            <p>Generate structured outputs through scalable cloud processing while keeping access centralized and browser-based for internal teams.</p>
          </article>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="container">
        <div class="section-header">
          <p class="section-kicker">Core capabilities</p>
          <h2 class="section-title">Designed around practical simulation work, not marketing abstractions</h2>
        </div>

        <div class="card-grid two">
          <article class="surface-card capability-card">
            <h3>Simulation control</h3>
            <ul>
              <li>Vehicle count and duration management</li>
              <li>Block-based generation settings</li>
              <li>Latency and route interpolation inputs</li>
              <li>Speed and distance unit controls</li>
            </ul>
          </article>

          <article class="surface-card capability-card">
            <h3>Data definition and output</h3>
            <ul>
              <li>DBC-aware generation workflows</li>
              <li>Selectable CAN frame sets</li>
              <li>Route and GPS-based scenario setup</li>
              <li>Structured output for downstream validation</li>
            </ul>
          </article>
        </div>
      </div>
    </section>

    <section class="section cta-band">
      <div class="container">
        <div class="surface-card cta-card">
          <div>
            <p class="section-kicker">Start the workflow</p>
            <h2>Open Trackster Studio and work from a secure browser-based environment.</h2>
            <p>
              Use the production workspace for controlled simulation runs, repeatable parameter configuration, and structured output generation.
            </p>
          </div>

          <div class="cta-card-actions">
            <a class="cta-primary" href="https://studio.trackster.pt">Open Studio</a>
            <a class="cta-secondary" routerLink="/contact">Request Demo</a>
          </div>
        </div>
      </div>
    </section>
  `,
  styles: [`
    .feature-card,
    .capability-card {
      padding: 24px;
    }

    .feature-card h3,
    .capability-card h3,
    .cta-card h2 {
      margin: 0 0 12px;
      color: #0b1f44;
      font-size: 1.28rem;
    }

    .feature-card p,
    .cta-card p {
      margin: 0;
      line-height: 1.75;
      color: #475569;
    }

    .capability-card ul {
      margin: 0;
      padding-left: 18px;
      line-height: 1.85;
      color: #475569;
    }

    .cta-band {
      padding-top: 18px;
      padding-bottom: 88px;
    }

    .cta-card {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 22px;
      align-items: center;
      padding: 28px;
    }

    .cta-card-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      justify-content: flex-end;
    }

    @media (max-width: 900px) {
      .cta-card {
        grid-template-columns: 1fr;
      }

      .cta-card-actions {
        justify-content: flex-start;
      }
    }
  `]
})
export class HomePageComponent {}
